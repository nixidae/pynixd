"""The same workload against `nix-daemon` and against pynixd, byte for byte.

This is the stream mode of `nix/functional-tests/`, without the functional
tests. It runs one small workload twice, with the recorder of
`nix_daemon_protocol.wirelog` between the client and the daemon, and it states
that the two recordings agree.

    client -> outer.sock -> recorder -> inner.sock -> the daemon

The contract of pynixd is that a client cannot tell pynixd from `nix-daemon`,
and that is a statement about the bytes. A script of the functional suite says
"pass" or "fail" for reasons that are not the wire, and it needs Linux and a
builder. This needs neither, so it runs in the dev shell on any host.

It found three defects:

1. `nix store add-file` and then `nix store gc` deleted the file against
   `nix-daemon`, and deleted nothing against pynixd. An idle pooled connection
   kept a worker of the daemon alive, and that worker held the temporary root
   of the path. `tests/unit/test_gc_retires_idle_connections.py` holds the
   rule that corrects it.
2. `QueryPathInfo` answered `sha256:<digest>` where `nix-daemon` answers the
   digest alone. The fast path of pynixd read the `narHash` column of the
   database, which carries the name of the algorithm, and the wire does not.
   No client complained, because `Hash::parseAny` reads both forms.
3. The second build of a content-addressed derivation answered
   `willBuild: [cad.drv]` to `QueryMissing`, and `nix-daemon` answers an
   empty set. The client then took a different code path, so every operation
   after it differed too. `pynixd/goals/realisations.py` holds the rule.

There are two workloads, and each one is a run of the test.

**`builds`** builds four derivations, with `/bin/sh` as the builder: one plain
one twice, one with two outputs, one that fails, and one that is
content-addressed twice. A garbage collection follows each group. So
`BuildPathsWithResults`, the temporary roots that a build makes, a failure,
and the answer that a second build of the same derivation gives are all in
the comparison.

**`queries`** asks about one closure every way that `nix-store -q` asks, and
it exports the closure to a file and imports it again. `nix-store` is the old
command, and it reaches operations that `nix store` does not.

**`modes`** checks and repairs, signs a path, and copies a closure to a second
store and back. It found the fourth defect: `BuildMode.CHECK` and
`BuildMode.REPAIR` both raised `RuntimeError` in the goal system, so
`nix build --rebuild`, `--repair`, `nix-store --realise --check`,
`--repair-path` and `--verify --repair` all failed through pynixd.

**`impure`** builds one impure derivation three times. It found the fifth and
the sixth: pynixd read the realisation of the last build and answered with it,
where Nix builds an impure derivation every time; and
`hash_derivation_modulo` read the word `impure` in the digest of the output as
a content hash, so it called the derivation fixed-output and gave every
realisation an id that no Nix agrees with.

Issue nixidae/nixidae#214.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import pytest

from nix_daemon_protocol.wirelog import compare, decode
from nix_daemon_protocol.wirelog.diff import report

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from anyio.abc import Process

    Runner = Callable[[list[str]], Awaitable[str]]
    """Run one command against the recorder, and answer its last line."""

    Workload = Callable[[Runner, Path, Path], Awaitable[None]]
    """One list of commands, given the runner, the store root and the work dir."""

NIX = shutil.which("nix")
PYNIXD = shutil.which("pynixd")

pytestmark = pytest.mark.skipif(
    NIX is None or PYNIXD is None,
    reason="this test needs both `nix` and `pynixd` on the PATH",
)

# Nix refuses a store when a parent of it is a symbolic link, and `/tmp` is
# one on Darwin. A Unix socket path also has to fit in `sun_path`, which is
# 104 bytes, so the root is short and not a `tmp_path` of pytest.
TEMP_ROOT = Path("/private/tmp") if Path("/private/tmp").is_dir() else Path("/tmp")
BASE = TEMP_ROOT / "pynixd-wire-parity"
SOCKET_WAIT = 30.0
# How long the group gets to end on SIGTERM before SIGKILL.
GRACE = 30.0

# Each derivation builds anywhere. `/bin/sh` is the builder, so the store needs
# no `bash` in it and the workload needs no channel.
DERIVATION = """
derivation {
  name = "probe";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo hi > $out" ];
}
"""

# Two outputs, so `BuildPathsWithResults` answers more than one path and the
# temporary roots of the build cover both.
MULTI = """
derivation {
  name = "multi";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  outputs = [ "out" "dev" ];
  args = [ "-c" "echo a > $out; echo b > $dev" ];
}
"""

# A builder that prints bytes no UTF-8 decoder accepts. The log stream
# carries them as opaque bytes: `nix-daemon` forwards them without looking,
# and pynixd must do the same. Four backslashes in this file are two in the
# expression (Python), which are one on the shell (Nix `"..."`), which the
# shell reads as octal. Before the lossless codec, pynixd failed the whole
# build with `internal scheduler error` on these bytes and registered no
# outputs. Issue nixidae/nixidae#662.
BINARY_LOG = """
derivation {
  name = "binary-log";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "printf '\\\\037\\\\213\\\\010not-utf8\\\\n' >&2; echo done > $out" ];
}
"""

# A build that fails. The two daemons must report the failure the same way.
FAILS = """
derivation {
  name = "fails";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "exit 3" ];
}
"""

# A content-addressed derivation, which names no output path. The second build
# of it is the one that matters: the store then holds a realisation, and
# `QueryMissing` has to read that realisation to know the output is there.
CONTENT_ADDRESSED = """
derivation {
  name = "cad";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo ca > $out" ];
  __contentAddressed = true;
  outputHashMode = "recursive";
  outputHashAlgo = "sha256";
}
"""
CA_FLAGS = ["--extra-experimental-features", "ca-derivations"]

# The store path of a derivation, with no output dependency on it. The client
# asks for that one path, and `nix-daemon` makes it valid and answers it.
OPAQUE_DRV = f"builtins.unsafeDiscardOutputDependency ({MULTI}).drvPath"

IMPURE_FLAGS = ["--extra-experimental-features", "impure-derivations ca-derivations", "--impure"]


def impure_expr(counter: Path) -> str:
    """A derivation that Nix must build every time, with a counter to prove it.

    `__impure` makes it impure, so the output path comes from what the build
    wrote and every build writes something else.

    **Two outputs, as `impure-derivations.nix` of the functional suite has.**
    That test builds `impure.all` and then `impure`, so the second command
    asks for one output of a derivation that the first one built whole.
    """
    return f"""
derivation {{
  name = "impure";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  outputs = [ "out" "stuff" ];
  args = [ "-c" "read n < {counter} || n=0; echo $((n + 1)) > {counter}; echo $n > $out; echo $n > $stuff" ];
  __impure = true;
  outputHashMode = "recursive";
  outputHashAlgo = "sha256";
}}
"""


# A chain of three derivations. `QueryMissing` must name all three, because
# `mustBuildDrv` at `misc.cc:139` enqueues each input of what it builds, and
# the client prints "these 3 derivations will be built".
CHAIN = """
let
  step = name: input: derivation {
    inherit name;
    system = builtins.currentSystem;
    builder = "/bin/sh";
    # `read` and `echo` are builtins of the shell. The store of this test
    # holds no `coreutils`, so `cat` is not there to run.
    args = [ "-c" (if input == null then "echo 0 > $out" else "read n < ${input}; echo $((n + 1)) > $out") ];
  };
  bottom = step "chain-bottom" null;
  middle = step "chain-middle" bottom;
in step "chain-top" middle
"""

# The same chain, with a failure at the bottom. The client must learn which
# derivation failed, and not only that one dependency did.
FAILING_CHAIN = """
let
  bottom = derivation {
    name = "chain-bad-bottom";
    system = builtins.currentSystem;
    builder = "/bin/sh";
    args = [ "-c" "exit 3" ];
  };
in derivation {
  name = "chain-bad-top";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "read n < ${bottom}; echo $n > $out" ];
}
"""


# An output that names another store path, so the closure has an edge in it
# and `--references`, `--referrers` and `--requisites` all have an answer.
REFERRER = """
derivation {
  name = "referrer";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo ${builtins.toFile "dep" "a dependency"} > $out" ];
}
"""


def _config(work: Path, root: Path) -> Path:
    """The configuration that `nix/functional-tests/make-shim.sh` writes."""
    config = work / "pynixd.json"
    config.write_text(
        json.dumps(
            {
                "stores": {
                    "local": {
                        "type": "local-socket",
                        "store_dir": str(root / "store"),
                        "state_dir": str(root / "var/nix"),
                        "socket_path": str(work / "up.sock"),
                        "nix_bin": NIX,
                        "use_db": True,
                        "monitor": False,
                        "probe": False,
                    },
                },
                "unix_path": str(work / "inner.sock"),
                "ssh_port": None,
                "http_port": None,
            },
        ),
    )
    return config


def _backend(role: str, work: Path, root: Path) -> tuple[list[str], dict[str, str]]:
    """The daemon that the recorder starts, and the environment it needs."""
    env = dict(
        os.environ,
        NIX_STORE_DIR=str(root / "store"),
        NIX_STATE_DIR=str(root / "var/nix"),
        # `NIX_STATE_DIR` does not move the build logs, and the daemon writes
        # one for each build. Without this it writes into `/nix/var/log/nix`
        # and answers "Permission denied", which is a difference of the
        # harness and not of pynixd.
        NIX_LOG_DIR=str(root / "var/log/nix"),
        # The builder is `/bin/sh`, which the sandbox does not carry, and this
        # test runs as a user with no build users group.
        NIX_CONFIG=(
            "sandbox = false\nbuild-users-group =\n"
            "experimental-features = nix-command flakes ca-derivations impure-derivations\n"
        ),
    )
    if role == "control":
        return [str(NIX), "daemon"], env
    return [str(PYNIXD), "daemon"], dict(env, PYNIXD_CONFIG=str(_config(work, root)))


async def _wait_for(path: Path) -> None:
    with anyio.fail_after(SOCKET_WAIT):
        while not await anyio.Path(path).exists():
            await anyio.sleep(0.02)


async def _builds(run: Runner, root: Path, work: Path) -> None:
    """Add paths, query them, build four derivations, and collect the garbage."""
    sample = work / "f.txt"
    for words in (
        ["store", "info"],
        ["store", "info", "--json"],
        ["store", "add-file", "--name", "f.txt", str(sample)],
        ["store", "add-path", "--name", "d", str(work / "d")],
        ["store", "ls", "--json", "--recursive", str(root / "store")],
        ["path-info", "--json", str(root / "store")],
        ["store", "verify", "--all"],
        ["store", "optimise"],
        ["store", "dump-path", str(root / "store")],
        ["store", "gc", "--max", "0"],
        ["store", "gc"],
        ["store", "info"],
        # A real build, then the same build again, so the second one reads
        # the output that the first one made. The `gc` at the end then has
        # to free it, and the temporary roots of the build are in the way.
        ["build", "--impure", "--no-link", "--json", "--expr", DERIVATION],
        ["build", "--impure", "--no-link", "--json", "--expr", DERIVATION],
        ["path-info", "--json", "--impure", "--expr", DERIVATION],
        ["store", "gc"],
        ["build", "--impure", "--no-link", "--json", "--expr", MULTI],
        # A chain of three, so `QueryMissing` has to walk the inputs.
        ["build", "--impure", "--no-link", "--json", "--expr", CHAIN],
        # The path of the derivation, and not an output of it.
        # `builtins.unsafeDiscardOutputDependency` drops the output dependency
        # of the string, so the client asks for one opaque store path that
        # happens to end in `.drv`. `build.sh:91` of the functional suite is
        # this command, and it reads the path back out of the JSON.
        ["build", "--impure", "--no-link", "--json", "--expr", OPAQUE_DRV],
        ["build", "--impure", "--no-link", "--json", "--expr", FAILS],
        ["build", "--impure", "--no-link", "--json", "--expr", CONTENT_ADDRESSED, *CA_FLAGS],
        ["build", "--impure", "--no-link", "--json", "--expr", CONTENT_ADDRESSED, *CA_FLAGS],
        ["store", "gc"],
    ):
        await run([str(NIX), *words])


async def _queries(run: Runner, root: Path, work: Path) -> None:
    """Ask about a closure every way that `nix-store -q` asks.

    `nix-store` is the old command, and it reaches operations that the new one
    does not: `QueryReferrers`, `QueryDerivationOutputs`, `ExportPath`,
    `ImportPaths` and the three `--print-*` modes of the collector.
    """
    leaf = await run([str(NIX), "store", "add-file", "--name", "leaf.txt", str(work / "f.txt")])
    referrer = await run([str(NIX), "build", "--impure", "--no-link", "--print-out-paths", "--expr", REFERRER])
    export = work / "exported.nar"
    for words in (
        ["nix-store", "-q", "--references", referrer],
        ["nix-store", "-q", "--referrers", leaf],
        ["nix-store", "-q", "--referrers-closure", leaf],
        ["nix-store", "-q", "--requisites", referrer],
        ["nix-store", "-q", "--size", referrer],
        ["nix-store", "-q", "--hash", referrer],
        ["nix-store", "-q", "--roots", referrer],
        ["nix-store", "-q", "--deriver", referrer],
        ["nix-store", "-q", "--tree", referrer],
        [str(NIX), "path-info", "--json", "--closure-size", "--recursive", referrer],
        [str(NIX), "path-info", "--json", "--sigs", referrer],
        [str(NIX), "store", "diff-closures", leaf, referrer],
        ["nix-store", "--dump-db"],
        ["nix-store", "--gc", "--print-roots"],
        ["nix-store", "--gc", "--print-dead"],
        ["nix-store", "--gc", "--print-live"],
        ["nix-store", "--verify", "--check-contents"],
        [str(NIX), "store", "verify", "--all", "--no-trust"],
        # Export the closure to a file, delete it, and import it again.
        ["sh", "-c", f"nix-store --export $(nix-store -qR {referrer}) > {export}"],
        [str(NIX), "store", "delete", "--ignore-liveness", referrer],
        [str(NIX), "path-info", "--json", referrer],
        ["sh", "-c", f"nix-store --import < {export}"],
        [str(NIX), "path-info", "--json", referrer],
        # A path that no store holds.
        ["nix-store", "-r", f"{root}/store/00000000000000000000000000000000-absent"],
        [str(NIX), "store", "gc"],
    ):
        await run(words)


async def _modes(run: Runner, root: Path, work: Path) -> None:
    """Check, repair, sign, and copy to a second store and back.

    `--rebuild` is `BuildMode.CHECK` and `--repair` is `BuildMode.REPAIR`.
    The goal system of pynixd raised `RuntimeError` for both, so every one of
    these commands failed through pynixd and passed through `nix-daemon`.
    """
    referrer = await run([str(NIX), "build", "--impure", "--no-link", "--print-out-paths", "--expr", REFERRER])
    other = work / "other"
    key = root / "probe.key"
    for words in (
        [str(NIX), "build", "--impure", "--no-link", "--rebuild", "--expr", REFERRER],
        [str(NIX), "build", "--impure", "--no-link", "--repair", "--expr", REFERRER],
        ["nix-store", "--realise", "--check", referrer],
        ["nix-store", "--repair-path", referrer],
        ["nix-store", "--verify", "--check-contents", "--repair"],
        # `nix store sign` is `AddSignatures` on the wire.
        [str(NIX), "store", "sign", "--key-file", str(key), referrer],
        [str(NIX), "path-info", "--json", "--sigs", referrer],
        [str(NIX), "store", "verify", "--all"],
        # A second store, which is `AddToStoreNar` and `NarFromPath`.
        [str(NIX), "copy", "--no-check-sigs", "--to", f"local?root={other}", referrer],
        [str(NIX), "copy", "--no-check-sigs", "--from", f"local?root={other}", referrer],
        [str(NIX), "store", "gc"],
    ):
        await run(words)


async def _failure(run: Runner, root: Path, work: Path) -> None:
    """An input fails, and the client must learn which one.

    `nix-daemon` writes `error: Cannot build '<input>.drv'. Reason: builder
    failed with exit code 3.` for the input, and it writes nothing for the
    derivation that the client asked for, because the client holds that
    failure in the `BuildResult` and prints it itself.

    pynixd wrote both, and it wrote each one as a `pynixd: ` note. Issue nixidae/nixidae#227
    corrected the frame: a goal that another goal waits for writes its failure
    as one error message, and a goal at the top of the request writes none.
    """
    del root, work
    await run([str(NIX), "build", "--impure", "--no-link", "--json", "--expr", FAILING_CHAIN])


async def _binary_log(run: Runner, root: Path, work: Path) -> None:
    """Build a derivation whose log carries non-UTF-8 bytes, twice.

    The builder prints a gzip magic (`1f 8b 08`) of the kind that
    `nixpkgs` emits while gzipping man pages. Both daemons must build it,
    register the output so the second build is a no-op, and carry the bytes
    on the wire. pynixd decoded the log stream as strict UTF-8, so it failed
    the build with `internal scheduler error` and registered nothing, and
    the recordings differed in the response. Issue nixidae/nixidae#662.
    """
    del root, work
    for words in (
        ["build", "--impure", "--no-link", "--json", "--expr", BINARY_LOG],
        ["build", "--impure", "--no-link", "--json", "--expr", BINARY_LOG],
    ):
        await run([str(NIX), *words])


async def _substitute(run: Runner, root: Path, work: Path) -> None:
    """Copy a build to a binary cache, delete it, and get it back.

    `ca:build-cache` and `ca:issue-13247` of the functional suite do this with
    a content-addressed derivation. `--max-jobs 0` states the rule: the second
    build must take every path from the cache, and a build there is a defect.

    A content-addressed output needs its realisation in the cache as well, and
    `nix copy` writes one only when the command names the installable. So this
    copies the expression, and not the output path.
    """
    del work
    cache = f"file://{root}/cache"
    substitute = ["--substituters", cache, "--no-require-sigs", "--max-jobs", "0", "--substitute"]
    pairs: list[tuple[str, list[str]]] = [(DERIVATION, []), (CONTENT_ADDRESSED, CA_FLAGS)]
    for expr, extra in pairs:
        await run([str(NIX), "build", "--impure", "--no-link", "--json", *extra, "--expr", expr])
        await run([str(NIX), "copy", "--to", cache, "--impure", *extra, "--expr", expr])
        await run([str(NIX), "store", "gc"])
        await run([str(NIX), "build", "--impure", "--no-link", "--json", *extra, *substitute, "--expr", expr])
        await run([str(NIX), "store", "gc"])


async def _impure(run: Runner, root: Path, work: Path) -> None:
    """An impure derivation builds every time, and the counter proves it.

    `impure-derivations.sh:36` of the functional suite is this test. The
    builder reads a counter from a file and writes the next number, so a
    second build that gives the first output is a defect. pynixd gave it: it
    read the realisation of the last build and answered with that.

    **This workload runs no garbage collection.** A build of an impure
    derivation leaves a scratch directory in the store beside the output that
    it registers, and the name of that directory holds a random part. The
    store held six `-impure` directories after three builds, and the database
    held three of them. A collection reports the other three as garbage, so
    the two recordings name six random directories and never agree.
    """
    # The counter goes under the store root, which is one path for both runs
    # and which the test wipes between them. A path under the work directory
    # would differ between the two runs, and the two derivations with it.
    counter = root / "counter"
    # A file, and not `--expr`, because `^*` needs an attribute path. The
    # path of the file reaches no derivation, so the two roles agree.
    recipe = work / "impure.nix"
    await anyio.Path(recipe).write_text(f"{{ impure = {impure_expr(counter)}; }}\n")
    for attribute in ("impure^*", "impure", "impure^*", "impure^stuff"):
        await run([str(NIX), "build", "--no-link", "--json", *IMPURE_FLAGS, "--file", str(recipe), attribute])


async def _record(role: str, root: Path, workload: Workload) -> Path:
    """Run the workload once, and answer the directory of the recording."""
    work = BASE / role
    out = BASE / f"rec-{role}"
    for path in (work, out, root / "store", root / "var/nix"):
        await anyio.Path(path).mkdir(parents=True, exist_ok=True)
    sample = work / "f.txt"
    await anyio.Path(sample).write_text("hello wirelog\n")
    await anyio.Path(work / "d").mkdir(exist_ok=True)
    await anyio.Path(work / "d" / "inner").write_text("inner\n")

    command, env = _backend(role, work, root)
    recorder = await anyio.open_process(
        [
            sys.executable,
            "-m",
            "nix_daemon_protocol.wirelog",
            "record",
            "--listen",
            str(work / "outer.sock"),
            "--connect",
            str(work / "inner.sock"),
            "--out",
            str(out),
            "--",
            *command,
        ],
        env=env,
        # A session of its own, so the teardown below can signal the whole
        # tree. The recorder starts a daemon, and a nix daemon forks a worker
        # for each connection. Issue #36.
        start_new_session=True,
    )
    try:
        await _wait_for(work / "outer.sock")
        client = dict(os.environ, NIX_REMOTE=f"unix://{work / 'outer.sock'}", NIX_STORE_DIR=str(root / "store"))

        async def run(words: list[str]) -> str:
            # A command may fail, and a failure is a fine thing to record: the
            # two daemons must fail the same way.
            done = await anyio.run_process(words, env=client, check=False)
            lines = done.stdout.decode(errors="replace").strip().splitlines()
            return lines[-1] if lines else ""

        await workload(run, root, work)
    finally:
        await _end(recorder)
    return out


async def _end(recorder: Process) -> None:
    """Take down the recorder and everything it started.

    `recorder.terminate()` signals one process. The recorder starts a daemon,
    and a nix daemon forks `nix <fd>` for every connection, so those workers
    were signalled by nothing: a run left one behind with `ppid 1`, holding a
    store under `/tmp` open. Issue #36.

    **The recorder is signalled alone, and the group only afterwards.** The
    recorder has a shielded teardown that signals its backend and waits for
    it, and a backend answers that. `pynixd daemon` in particular kills its own
    managed `nix daemon` there, which lives in a session of its own and no
    `killpg` of this group can reach. Signalling the whole group instead of the
    recorder took pynixd down before that teardown ran, and a run then left six
    managed daemons behind where it had left none.

    The sweep afterwards is for what the recorder does not own: the recorder
    has a session of its own, so its pid is the process group, and a `nix <fd>`
    worker still in it outlived its daemon.
    """
    group = recorder.pid
    recorder.terminate()
    with anyio.move_on_after(GRACE) as scope:
        await recorder.wait()

    # Anything still here outlived the recorder, or is the recorder itself
    # refusing SIGTERM.
    #
    # `wait` above has reaped the recorder on the path where it answered, so
    # the group id is a pid the kernel could hand out again. The window is the
    # microseconds to the next line, Linux allocates pids in order up to
    # `pid_max`, and a new owner would have to be a group leader as well.
    # Named rather than closed: not reaping means not waiting, and then the
    # recorder's own teardown never gets the grace period it needs.
    with contextlib.suppress(ProcessLookupError):
        os.killpg(group, signal.SIGKILL)

    if scope.cancelled_caught:
        await recorder.wait()


@pytest.fixture
async def clean_base() -> AsyncIterator[None]:
    shutil.rmtree(BASE, ignore_errors=True)
    yield
    shutil.rmtree(BASE, ignore_errors=True)


@pytest.mark.parametrize(
    "workload",
    [
        _builds,
        _queries,
        _modes,
        _impure,
        # **Issue nixidae/nixidae#226.** pynixd reads the substituters of its own
        # configuration alone, so it answers `willBuild` where `nix-daemon`
        # answers `willSubstitute`, and it then builds. `strict`, so the
        # marker goes away with the correction and does not hide it.
        pytest.param(_substitute, marks=pytest.mark.xfail(strict=True, reason="issue nixidae/nixidae#226")),
        _failure,
        _binary_log,
    ],
    ids=["builds", "queries", "modes", "impure", "substitute", "failure", "binary_log"],
)
@pytest.mark.usefixtures("clean_base")
async def test_the_two_daemons_answer_the_same_bytes(workload: Workload) -> None:
    """Each connection of the pynixd run agrees with the control run.

    The store directory is one path for both runs, because the hash of a store
    path holds that directory. Two roots would give two hashes, and then every
    answer would differ for a reason that is not pynixd.
    """
    root = BASE / "store-root"
    # One signing key for both runs. `nix key generate-secret` makes a new key
    # each time, so two keys would give two signatures for a reason that is
    # not pynixd. Ed25519 is deterministic, so one key gives one signature.
    key = (await anyio.run_process([str(NIX), "key", "generate-secret", "--key-name", "probe"])).stdout.decode()

    recordings: dict[str, Path] = {}
    for role in ("control", "pynixd"):
        shutil.rmtree(root, ignore_errors=True)
        await anyio.Path(root).mkdir(parents=True)
        await anyio.Path(root / "probe.key").write_text(key.strip())
        recordings[role] = await _record(role, root, workload)

    control = sorted(p.relative_to(recordings["control"]) for p in recordings["control"].rglob("conn-*.wire"))
    candidate = sorted(p.relative_to(recordings["pynixd"]) for p in recordings["pynixd"].rglob("conn-*.wire"))
    assert control, "the control run recorded no connection"
    assert control == candidate, f"the two runs served different connections: {control} and {candidate}"

    for name in control:
        one = await decode(recordings["control"] / name)
        two = await decode(recordings["pynixd"] / name)
        assert one.problem is None, one.problem
        assert two.problem is None, two.problem
        differences = compare(one, two)
        assert differences == [], f"{name}\n{report(differences)}"


class TestTeardown:
    """`_end` takes down everything the recorder started.

    **Issue #36 is not reproducible here.** The leak it reports is a `nix <fd>`
    worker, found by the guest census while that census still ran
    `tests/parity`; the guest stopped running this suite for issue #37, and
    this machine leaves nothing either way. So these state what the teardown
    does rather than that the reported leak is gone.
    """

    async def test_a_process_the_recorder_left_behind_is_swept(self, tmp_path: Path) -> None:
        """A nix daemon forks a worker for each connection, and those workers
        outlive it. The recorder's own teardown never signalled them: it
        signals its backend and nothing below."""
        marker = tmp_path / "stray.pid"
        recorder = await anyio.open_process(
            ["sh", "-c", f"sleep 300 & echo $! > {marker}; exec sleep 0.1"],
            start_new_session=True,
        )
        with anyio.fail_after(10):
            while not await anyio.Path(marker).exists():
                await anyio.sleep(0.02)
        stray = int((await anyio.Path(marker).read_text()).strip())
        os.kill(stray, 0)  # It is running, or this test proves nothing.

        await _end(recorder)

        # Polled, not asserted at once. The stray's parent is gone, so it is a
        # zombie until init reaps it, and a zombie still answers `kill(pid, 0)`.
        with anyio.fail_after(10):
            while True:
                try:
                    os.kill(stray, 0)
                except ProcessLookupError:
                    return
                await anyio.sleep(0.02)

    async def test_the_recorder_is_signalled_before_its_group(self, tmp_path: Path) -> None:
        """**Not `killpg(SIGTERM)` first.** The recorder has a shielded
        teardown that signals its backend and waits, and `pynixd daemon` kills
        its own managed `nix daemon` there -- which lives in a session of its
        own, where no `killpg` of this group reaches it. Signalling the group
        instead took pynixd down before that ran, and one run then left six
        managed daemons behind where it had left none.

        So the recorder must see SIGTERM and get its grace period.
        """
        seen = tmp_path / "sigterm"
        recorder = await anyio.open_process(
            ["sh", "-c", f"trap 'echo caught > {seen}; exit 0' TERM; while :; do sleep 0.05; done"],
            start_new_session=True,
        )
        # Let the trap be installed before anything is sent.
        await anyio.sleep(0.5)

        await _end(recorder)

        assert await anyio.Path(seen).exists(), "the recorder never saw SIGTERM"
