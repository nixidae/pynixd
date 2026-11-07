"""pynixd builds without the sandbox where Nix does (`makeDerivationBuilder`, `derivation-builder.cc:1751`).

Only a store whose real directory is its logical one builds without it: a
store at `?root=` always sandboxes (`:1782`). So each store here is a
`NIX_STORE_DIR` store, and the client names the same directory.
"""

from __future__ import annotations

import contextlib
import json
import os
import socket as socket_module
import sqlite3
from typing import TYPE_CHECKING, Any

import anyio
import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_dir import real_store_dir, set_real_store_dir, set_store_dir, store_dir
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.config import NIX_GIT_BIN
from tests._conftest.constants import _NO_PROBE_FEATURE_MATRIX
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping
    from pathlib import Path

# What only a build outside the chroot sees: the host's name, and its real build directory.
PROBE = """read h < /proc/sys/kernel/hostname
case $NIX_BUILD_TOP in /build) w=chroot;; *) w=host;; esac
[ "$PWD" = "$NIX_BUILD_TOP" ] && [ "$TMPDIR" = "$NIX_BUILD_TOP" ] && p=here
echo built on $w
echo $h $w $p > $out"""

FAKE_HASH = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


@pytest.fixture(autouse=True)
def _keep_store_dirs() -> Iterator[None]:
    """A server sets the process's store directories (`instance.py:113`); the session's server needs its own back."""
    saved, saved_real = store_dir(), real_store_dir()
    yield
    set_store_dir(saved)
    set_real_store_dir(saved_real)


def _drv(name: str, body: str, extra: str = "") -> str:
    return (
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; {extra} '
        f"args = [ (builtins.toFile \"{name}.sh\" ''\n{body}\n'') ]; }}"
    )


def _fallthrough() -> float:
    return sum(
        sample.value
        for metric in REGISTRY.collect()
        if metric.name == "pynixd_op_fallthrough"
        for sample in metric.samples
        if sample.name == "pynixd_op_fallthrough_total" and sample.labels.get("op") == "BuildDerivation"
    )


@contextlib.asynccontextmanager
async def _serve(
    tmp_path: Path,
    route: OpRoute,
    sandbox: str,
    *,
    experimental_features: tuple[str, ...] = (),
    nix_bin: Path = NIX_BIN,
    routes: Mapping[str, OpRoute] | None = None,
    **spec_args: Any,
) -> AsyncIterator[tuple[list[str], dict[str, str], Path]]:
    """A fresh `NIX_STORE_DIR` store with `BuildDerivation` on *route*: the build command, its environment, the state.

    *routes* routes other ops; *spec_args* go to `make_test_spec`.
    """
    # One directory for both routes, so that every store path is the same; a route's store replaces the other's.
    base = tmp_path / sandbox
    rmtree_robust(base)
    store, state, logs = base / "store", base / "var/nix", base / "var/log/nix"
    path = tmp_path / f"{route.name}-{sandbox}.sock"
    nix_config = for_test_store(substituters=(), experimental_features=experimental_features)
    nix_config.sandbox = sandbox
    spec = make_test_spec(
        store_id="local",
        store_path=base,
        no_probe=True,
        nix_config=nix_config,
        op_routes={"BuildDerivation": route, **(routes or {})},
        store_dir=store,
        state_dir=state,
        log_dir=logs,
        nix_bin=nix_bin,
        **spec_args,
    )
    env = {"NIX_STORE_DIR": str(store), "NIX_STATE_DIR": str(base / "client"), "NIX_LOG_DIR": str(logs)}
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=path, ssh_port=None, http_port=None):
        yield [str(nix_bin), "--store", f"unix://{path}", "--impure"], env, state


def _rows(state: Path) -> tuple[list[tuple[Any, ...]], list[tuple[str, str]]]:
    db = sqlite3.connect(f"file:{state}/db/db.sqlite?mode=ro", uri=True)
    try:
        valid = db.execute(
            "SELECT path, hash, narSize, deriver, ultimate, sigs, ca FROM ValidPaths ORDER BY path"
        ).fetchall()
        refs = db.execute(
            "SELECT a.path, b.path FROM Refs JOIN ValidPaths a ON a.id = referrer "
            "JOIN ValidPaths b ON b.id = reference ORDER BY a.path, b.path"
        ).fetchall()
    finally:
        db.close()
    return valid, refs


def _errors(err: str) -> list[str]:
    """The build's error: *err* from its first `error:` line."""
    lines = err.splitlines()
    return lines[next(i for i, line in enumerate(lines) if line.startswith("error:")) :]


async def _probe(tmp_path: Path, route: OpRoute, sandbox: str, extra: str) -> dict[str, Any]:
    async with _serve(tmp_path, route, sandbox) as (nix, env, state):
        _rc, out, err, _both = await run_subproc(
            [
                *nix[:1],
                "build",
                *nix[1:],
                "--no-link",
                "--print-out-paths",
                "--print-build-logs",
                "--expr",
                _drv("unsandboxed-probe", PROBE, extra),
            ],
            env=dict(env),
        )
        path = out.strip()
        _rc, info, _err, _both = await run_subproc([*nix[:1], "path-info", *nix[1:], "--json", path], env=dict(env))
        contents = (state.parent.parent / "store" / path.rsplit("/", 1)[-1]).read_text()
    valid, refs = _rows(state)
    info_json = json.loads(info)
    for value in info_json.values() if isinstance(info_json, dict) else info_json:
        value.pop("registrationTime", None)
    return {
        "contents": contents,
        "info": info_json,
        "valid": valid,
        "refs": refs,
        "log": [line for line in err.splitlines() if line.startswith("unsandboxed-")],
    }


async def test_an_unsandboxed_build_runs_on_the_host_as_the_daemons_does(tmp_path: Path) -> None:
    """`__noChroot` under `sandbox = relaxed`, and any derivation under `sandbox = false`, see the host."""
    for sandbox, extra in (("relaxed", "__noChroot = true;"), ("false", "")):
        before = _fallthrough()
        here = await _probe(tmp_path, OpRoute.pynixd, sandbox, extra)
        assert _fallthrough() == before, f"the daemon built the pynixd route's build with sandbox = {sandbox}"
        there = await _probe(tmp_path, OpRoute.daemon, sandbox, extra)

        assert here["contents"] == there["contents"] == f"{socket_module.gethostname()} host here\n"
        assert here["info"] == there["info"]
        assert here["valid"] == there["valid"]
        assert here["refs"] == there["refs"]
        assert here["log"] == there["log"] == ["unsandboxed-probe> built on host"]


# `dev` refers to itself, and `out` to itself and to `dev`: each hash the build sees moves back.
# The builder has no PATH outside the sandbox, so it uses builtins alone.
REWRITTEN = """echo $dev > $dev
@NOISE@
echo $out $dev $n > $out"""


# The default client config names the machine's own daemon as a substituter. Under `NIX_STORE_DIR`, Nix takes
# that store to have this store's directory, sends it these paths, and the daemon refuses each one with "is not
# in the Nix store". nixidae/nixidae#1193.
ALONE = {"substituters": ""}


def _rewritten(name: str, noise: str) -> str:
    return _drv(name, REWRITTEN.replace("@NOISE@", noise), 'outputs = [ "out" "dev" ];')


def _tree(root: Path) -> dict[str, str]:
    if root.is_file():
        return {".": root.read_text()}
    return {str(p.relative_to(root)): p.read_text() for p in sorted(root.rglob("*")) if p.is_file()}


def _normal(text: str, *paths: str) -> list[str]:
    """The lines of *text* from the build on, with each build directory as `<build>`."""
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("note: keeping build directory"):
            lines[i] = "note: keeping build directory <build>"
    for path in paths:
        lines = [line.replace(path, "<store>") for line in lines]
    return lines


async def _rebuilt(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    """A check, a check that differs, a build with one output valid, and a repair, all under `sandbox = false`."""
    seen: dict[str, Any] = {}
    async with _serve(tmp_path, route, "false", experimental_features=("ca-derivations",)) as (nix, env, state):
        store = state.parent.parent / "store"

        async def run(*args: str, expected: int = 0) -> str:
            _rc, out, err, _both = await run_subproc(
                [*nix[:1], *args[:1], *nix[1:], *args[1:]], expected_retcode=expected, env=dict(env), nix_config=ALONE
            )
            if expected != 0:
                seen.setdefault("errors", []).append(_normal(err, str(store)))
            return out

        def look(step: str) -> None:
            seen[step] = {
                "store": sorted(p.name for p in store.iterdir() if not p.name.endswith(".drv")),
                "trees": {
                    p.name: _tree(p)
                    for p in sorted(store.iterdir())
                    if p.name != ".links" and not p.name.endswith((".drv", ".lock"))
                },
                "rows": _rows(state),
            }

        same = _rewritten("unsandboxed-rewrite", "n=same")
        built = (await run("build", "--no-link", "--print-out-paths", "--expr", f"({same}).all")).split()
        look("built")
        await run("build", "--no-link", "--rebuild", "--expr", f"({same}).all")
        look("checked")
        differs = _rewritten("unsandboxed-differs", "read n < /proc/sys/kernel/random/uuid")
        await run("build", "--no-link", "--expr", f"({differs}).all")
        await run("build", "--no-link", "--rebuild", "--keep-failed", "--expr", f"({differs}).all", expected=1)
        look("differed")
        out = next(path for path in built if not path.endswith("-dev"))
        await run_subproc([*nix[:1], "store", "delete", *nix[1:3], out], env=dict(env), nix_config=ALONE)
        await run("build", "--no-link", "--expr", f"({same}).all")
        look("one valid")
        dev = store / next(path for path in built if path.endswith("-dev")).rsplit("/", 1)[-1]
        dev.chmod(0o644)
        dev.write_text("corrupt\n")
        dev.chmod(0o444)
        await run("build", "--no-link", "--repair", "--expr", f"({same}).all")
        look("repaired")
    return seen


def _quiet(seen: Any) -> Any:
    """*seen* without what the noise of `unsandboxed-differs` makes: its `out`'s contents and hash, on each route."""
    if not isinstance(seen, dict) or "trees" not in seen:
        return seen
    noisy = ("-unsandboxed-differs", "-unsandboxed-differs.check")
    valid, refs = seen["rows"]
    return {
        **seen,
        "trees": {name: tree for name, tree in seen["trees"].items() if not name.endswith(noisy)},
        "rows": ([row[:1] + row[3:] if row[0].endswith(noisy) else row for row in valid], refs),
    }


async def test_a_rebuild_without_the_sandbox_rewrites_hashes_as_the_daemon_does(tmp_path: Path) -> None:
    """A valid output builds at `makeFallbackPath(path)`, and its hash moves back after (`derivation-builder.cc:417`).

    That holds for a check, a repair, and a build with one output valid already. Each step compares the store
    directory, every output's files and the database with the daemon's.
    """
    before = _fallthrough()
    here = await _rebuilt(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "the daemon built a rebuild of the pynixd route"
    there = await _rebuilt(tmp_path, OpRoute.daemon)

    dev = next(name for name in here["built"]["trees"] if name.endswith("-dev"))
    # Lillecarl/nix#400: with `ca-derivations`, the daemon finds `dev` corrupt and repairs nothing; pynixd repairs it.
    assert there["repaired"]["trees"][dev] == {".": "corrupt\n"}
    assert here["repaired"]["trees"][dev] == here["built"]["trees"][dev]
    there["repaired"]["trees"][dev] = here["repaired"]["trees"][dev]
    # Lillecarl/nix#401: the failed check leaves the scratch path of `dev` beside `dev`.
    assert sum(name.endswith("-unsandboxed-differs-dev") for name in there["differed"]["store"]) == 2
    for step in ("built", "checked", "differed", "one valid", "repaired"):
        mine, theirs = _quiet(here[step]), _quiet(there[step])
        only = sorted(set(mine["store"]) ^ set(theirs["store"]))
        assert mine["store"] == theirs["store"], (
            step,
            "pynixd's" if only[0] in mine["store"] else "the daemon's",
            only,
        )
        trees = {
            name: (tree, theirs["trees"].get(name))
            for name, tree in mine["trees"].items()
            if tree != theirs["trees"].get(name)
        }
        assert not trees, (step, "pynixd's file, the daemon's", trees)
        assert mine == theirs, step
    assert here["errors"] == there["errors"]
    assert any("may not be deterministic" in line for line in here["errors"][0]), here["errors"]


def _fod(name: str, mode: str, command: str, output_hash: str = FAKE_HASH) -> str:
    return _drv(name, command, f'outputHashMode = "{mode}"; outputHashAlgo = "sha256"; outputHash = "{output_hash}";')


async def _fixed(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    errors: dict[str, list[str]] = {}
    async with _serve(tmp_path, route, "relaxed") as (nix, env, state):

        async def build(expr: str, expected: int) -> str:
            _rc, out, err, _both = await run_subproc(
                [*nix[:1], "build", *nix[1:], "--no-link", "--print-out-paths", "--expr", expr],
                expected_retcode=expected,
                env=dict(env),
                nix_config=ALONE,
            )
            return out if expected == 0 else "\n".join(_errors(err))

        errors["flat"] = (await build(_fod("unsandboxed-fod-flat", "flat", "echo flat > $out"), 1)).splitlines()
        errors["self"] = (await build(_fod("unsandboxed-fod-self", "recursive", "echo $out > $out"), 1)).splitlines()
        got = next(line.split()[-1] for line in errors["flat"] if line.strip().startswith("got:"))
        right = await build(_fod("unsandboxed-fod-flat", "flat", "echo flat > $out", got), 0)
        contents = (state.parent.parent / "store" / right.strip().rsplit("/", 1)[-1]).read_text()
    valid, refs = _rows(state)
    return {"errors": errors, "contents": contents, "valid": valid, "refs": refs}


async def test_an_unsandboxed_fixed_output_lands_where_the_daemon_puts_it(tmp_path: Path) -> None:
    """`relaxed` builds a fixed output outside the chroot (`derivation-builder.cc:1777`), at its final path."""
    before = _fallthrough()
    here = await _fixed(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before
    there = await _fixed(tmp_path, OpRoute.daemon)

    assert here == there
    assert here["contents"] == "flat\n"
    assert all(any("hash mismatch" in line for line in here["errors"][k]) for k in ("flat", "self")), here


def _nix_dir(binary: Path) -> str:
    """The store path of *binary*'s Nix: on the host, outside the test store, which the builder sees whole."""
    return "/".join(os.path.realpath(binary).split("/")[:4])


# `recursive.nix` of Nix's functional suite. The inner Nix finds the store through `NIX_STORE`.
RECURSIVE = """
let
  inner = builtins.toFile "unsandboxed-inner.nix" ''
    derivation { name = "unsandboxed-inner"; system = builtins.currentSystem; builder = "/bin/sh";
      args = [ "-c" "echo $fnord blaat > $out" ]; fnord = builtins.toFile "fnord" "fnord"; }
  '';
in derivation {
  name = "unsandboxed-recursive"; system = builtins.currentSystem; builder = "/bin/sh";
  requiredSystemFeatures = [ "recursive-nix" ];
  PATH = "@NIX@/bin";
  dummy = builtins.toFile "dummy" "bla bla";
  unreachable = builtins.unsafeDiscardStringContext (builtins.toFile "unreachable" "x");
  inherit inner;
  args = [ (builtins.toFile "unsandboxed-recursive.sh" ''
    set -e
    opts="--extra-experimental-features nix-command"
    case $NIX_REMOTE in unix://$NIX_BUILD_TOP/.nix-socket) echo socket in the build directory;; esac
    nix $opts path-info $dummy
    nix $opts build --no-link $dummy
    if nix $opts path-info $unreachable; then exit 10; fi
    if nix $opts build --no-link $unreachable; then exit 11; fi
    echo foobar > foobar
    foobar=$(nix $opts store add-path ./foobar)
    nix $opts path-info $foobar
    n=0
    for p in $(nix $opts path-info --all); do n=$((n+1)); done
    inner1=$(nix $opts build --no-link --print-out-paths --file $inner)
    read fnord blaat < $inner1
    nix $opts key generate-secret --key-name k > key
    signed=$(nix $opts store sign --key-file ./key $dummy 2>&1 || true)
    echo "$foobar $inner1 $n $fnord" > $out
    nix $opts path-info --json $inner1 >> $out
    echo "$signed" >> $out
  '') ];
}
"""


async def _recursive(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    async with _serve(tmp_path, route, "false", experimental_features=("recursive-nix",)) as (nix, env, state):
        store = state.parent.parent / "store"
        _rc, out, err, _both = await run_subproc(
            [
                *nix[:1],
                "build",
                *nix[1:],
                "--no-link",
                "--print-out-paths",
                "--print-build-logs",
                "--expr",
                RECURSIVE.replace("@NIX@", _nix_dir(NIX_BIN)),
            ],
            env=dict(env),
            nix_config=ALONE,
        )
        top = out.strip()
        _rc, info, _err, _both = await run_subproc(
            [*nix[:1], "path-info", *nix[1:], "--json", "--recursive", top], env=dict(env), nix_config=ALONE
        )
        contents = (store / top.rsplit("/", 1)[-1]).read_text()
    valid, refs = _rows(state)
    return {
        "top": top,
        "contents": contents,
        "info": _without_times(json.loads(info)),
        "valid": valid,
        "refs": refs,
        "log": [line for line in err.splitlines() if line.startswith("unsandboxed-")],
    }


def _without_times(info: Any) -> Any:
    for value in info.values() if isinstance(info, dict) else info:
        if isinstance(value, dict):
            value.pop("registrationTime", None)
    return info


async def test_a_recursive_nix_build_without_the_sandbox_builds_what_the_daemon_builds(tmp_path: Path) -> None:
    """`startDaemon` (`derivation-builder.cc:802`) puts the socket in the real build directory, and
    `addDependencyImpl` (`:901`) binds nothing: the builder sees the whole store, the restricted store does not."""
    before = _fallthrough()
    here = await _recursive(tmp_path, OpRoute.pynixd)
    assert _fallthrough() == before, "the daemon built the pynixd route's recursive-nix build"
    there = await _recursive(tmp_path, OpRoute.daemon)

    for key in ("top", "contents", "info", "valid", "refs", "log"):
        assert here[key] == there[key], (key, here[key], there[key])
    assert "unsandboxed-recursive> socket in the build directory" in here["log"], here["log"]
    first, *_info = here["contents"].splitlines()
    foobar, inner1, count, fnord = first.split()
    assert foobar.endswith("-foobar") and inner1.endswith("-unsandboxed-inner"), first
    assert fnord.endswith("-fnord") and int(count) > 2, first
    assert "operation 'addSignatures' is not supported by store" in here["contents"], here["contents"]
    assert {foobar, inner1} <= set(here["info"][here["top"]]["references"]), here["info"]


# `builder-rpc-v0` of Nix's master branch, as `test_local_build_parity.py` builds it in the sandbox.
RPC = """
derivation {
  name = "unsandboxed-rpc"; system = builtins.currentSystem; builder = "/bin/sh";
  __contentAddressed = true; outputHashMode = "recursive"; outputHashAlgo = "sha256";
  outputs = [ "out" "dev" ];
  requiredSystemFeatures = [ "builder-rpc-v0" ];
  PATH = "@NIX@/bin";
  dummy = builtins.toFile "dummy" "bla bla";
  args = [ (builtins.toFile "unsandboxed-rpc.sh" ''
    set -e
    o="--extra-experimental-features nix-command --extra-experimental-features dynamic-derivations"
    refused=$(nix $o path-info $dummy 2>&1 || true)
    echo "dev of $dummy" > dev
    dev=$(nix $o store add --scan --name unsandboxed-rpc-dev ./dev)
    nix $o store submit-output $dev dev
    duplicate=$(nix $o store submit-output $dev dev 2>&1 || true)
    printf '%s\\n' "out:$out" "$dev" "$refused" "$duplicate" > out
    out=$(nix $o store add --scan --name unsandboxed-rpc ./out)
    nix $o store submit-output $out out
  '') ];
}
"""


async def _rpc(tmp_path: Path, route: OpRoute, nix_bin: Path) -> dict[str, Any]:
    features = ("ca-derivations", "dynamic-derivations")
    matrix = {system: set(fs) | {"builder-rpc-v0"} for system, fs in _NO_PROBE_FEATURE_MATRIX.items()}
    async with _serve(
        tmp_path, route, "false", experimental_features=features, nix_bin=nix_bin, feature_matrix=matrix
    ) as (nix, env, state):
        store = state.parent.parent / "store"
        _rc, out, err, _both = await run_subproc(
            [
                *nix[:1],
                "build",
                *nix[1:],
                "--extra-experimental-features",
                " ".join(features),
                "--no-link",
                "--print-out-paths",
                "--print-build-logs",
                "--expr",
                RPC.replace("@NIX@", _nix_dir(nix_bin)),
            ],
            env=dict(env),
            nix_config=ALONE,
        )
        paths = out.split()
        contents = {path: (store / path.rsplit("/", 1)[-1]).read_text() for path in paths}
    db = sqlite3.connect(f"file:{state}/db/db.sqlite?mode=ro", uri=True)
    try:
        trace = db.execute(
            "SELECT drvPath, outputName, outputPath, signatures FROM BuildTraceV3 ORDER BY drvPath, outputName"
        ).fetchall()
    finally:
        db.close()
    valid, refs = _rows(state)
    return {
        "paths": paths,
        "contents": contents,
        "valid": valid,
        "refs": refs,
        "trace": trace,
        "log": [line for line in err.splitlines() if line.startswith("unsandboxed-")],
    }


async def test_a_builder_rpc_build_without_the_sandbox_builds_what_the_daemon_builds(tmp_path: Path) -> None:
    """`RecursiveSubmitted` (`unix-derivation-builder.cc:672`) outside the chroot: the builder submits each output."""
    if NIX_GIT_BIN is None:
        pytest.skip("NIX_GIT_BIN names no Nix of the master branch")
    before = _fallthrough()
    here = await _rpc(tmp_path, OpRoute.pynixd, NIX_GIT_BIN)
    assert _fallthrough() == before, "the daemon built the pynixd route's builder-rpc-v0 build"
    there = await _rpc(tmp_path, OpRoute.daemon, NIX_GIT_BIN)

    for key in ("paths", "contents", "valid", "refs", "trace", "log"):
        assert here[key] == there[key], (key, here[key], there[key])
    dev, out = sorted(here["paths"], key=lambda p: not p.endswith("-dev"))
    lines = here["contents"][out].splitlines()
    assert lines[:2] == ["out:", dev], lines
    assert "not allowed inside derivation" in here["contents"][out], lines
    assert "duplicate output" in here["contents"][out], lines
    assert {row[1] for row in here["trace"]} >= {"out", "dev"}, here["trace"]


# A `builder-rpc-v0` build that writes *noise* in its output; `$u` makes each build differ.
RPC_CHECKED = """
derivation {
  name = "unsandboxed-rpc-checked"; system = builtins.currentSystem; builder = "/bin/sh";
  __contentAddressed = true; outputHashMode = "recursive"; outputHashAlgo = "sha256";
  requiredSystemFeatures = [ "builder-rpc-v0" ];
  PATH = "@NIX@/bin";
  args = [ (builtins.toFile "unsandboxed-rpc-checked.sh" ''
    set -e
    o="--extra-experimental-features nix-command --extra-experimental-features dynamic-derivations"
    read u < /proc/sys/kernel/random/uuid
    echo "@NOISE@" > out
    out=$(nix $o store add --name unsandboxed-rpc-checked ./out)
    nix $o store submit-output $out out
  '') ];
}
"""


async def _rpc_checked(tmp_path: Path, route: OpRoute, nix_bin: Path, noise: str) -> dict[str, Any]:
    features = ("ca-derivations", "dynamic-derivations")
    matrix = {system: set(fs) | {"builder-rpc-v0"} for system, fs in _NO_PROBE_FEATURE_MATRIX.items()}
    expr = RPC_CHECKED.replace("@NIX@", _nix_dir(nix_bin)).replace("@NOISE@", noise)
    answers = []
    async with _serve(
        tmp_path, route, "false", experimental_features=features, nix_bin=nix_bin, feature_matrix=matrix
    ) as (nix, env, state):
        for flag in ((), ("--rebuild",)):
            rc, _out, err, _both = await run_subproc(
                [
                    *nix[:1],
                    "build",
                    *nix[1:],
                    "--extra-experimental-features",
                    " ".join(features),
                    "--no-link",
                    *flag,
                    "--expr",
                    expr,
                ],
                env=dict(env),
                nix_config=ALONE,
                expected_retcode=None,
            )
            db = sqlite3.connect(f"file:{state}/db/db.sqlite?mode=ro", uri=True)
            try:
                trace = db.execute("SELECT drvPath, outputName, outputPath FROM BuildTraceV3").fetchall()
            finally:
                db.close()
            answers.append((rc, err, trace))
    (built, _err, trace), (rc, err, checked) = answers
    assert built == 0, _err
    return {"rc": rc, "errors": _errors(err) if rc else [], "trace": trace, "checked": checked}


async def test_a_check_of_a_builder_rpc_build_compares_what_it_submits(tmp_path: Path) -> None:
    """`checkSubmittedOutputs` (`derivation-builder-impl.cc:762`) has no check mode: a rebuild that differs fails to
    register its realisation. pynixd answers that the derivation may not be deterministic."""
    if NIX_GIT_BIN is None:
        pytest.skip("NIX_GIT_BIN names no Nix of the master branch")
    for noise in ("same", "$u"):
        before = _fallthrough()
        here = await _rpc_checked(tmp_path, OpRoute.pynixd, NIX_GIT_BIN, noise)
        assert _fallthrough() == before, f"the daemon checked the pynixd route's builder-rpc-v0 build ({noise})"
        there = await _rpc_checked(tmp_path, OpRoute.daemon, NIX_GIT_BIN, noise)
        assert here["checked"] == here["trace"] and there["checked"] == there["trace"], (here, there)
        if noise == "same":
            assert here["trace"] == there["trace"], (here, there)
            assert here["rc"] == there["rc"] == 0, (here, there)
            continue
        assert here["rc"] == there["rc"] == 1, (here, there)
        assert "but we already have another one locally." in there["errors"][0], there
        assert here["errors"][0].startswith("error: derivation '"), here
        assert "may not be deterministic: output" in here["errors"][0], here


async def _rpc_repaired(tmp_path: Path, route: OpRoute, nix_bin: Path, adds: OpRoute) -> dict[str, Any]:
    features = ("ca-derivations", "dynamic-derivations")
    matrix = {system: set(fs) | {"builder-rpc-v0"} for system, fs in _NO_PROBE_FEATURE_MATRIX.items()}
    expr = RPC_CHECKED.replace("@NIX@", _nix_dir(nix_bin)).replace("@NOISE@", "same")
    routes = dict.fromkeys(("AddToStore", "AddToStoreNar", "AddMultipleToStore"), adds)
    async with _serve(
        tmp_path,
        route,
        "false",
        experimental_features=features,
        nix_bin=nix_bin,
        feature_matrix=matrix,
        routes=routes,
    ) as (nix, env, state):
        build = [*nix[:1], "build", *nix[1:], "--extra-experimental-features", " ".join(features), "--no-link"]
        _rc, out, _err, _both = await run_subproc(
            [*build, "--print-out-paths", "--expr", expr], env=dict(env), nix_config=ALONE
        )
        real = state.parent.parent / "store" / out.strip().rsplit("/", 1)[-1]
        real.chmod(0o644)
        real.write_text("corrupt\n")
        with anyio.fail_after(60):
            rc, _out, err, _both = await run_subproc(
                [*build, "--repair", "--expr", expr], env=dict(env), nix_config=ALONE, expected_retcode=None
            )
        return {"rc": rc, "err": err, "contents": real.read_text() if real.exists() else None}


async def test_a_repair_of_a_builder_rpc_build_replaces_what_the_builder_submits(tmp_path: Path) -> None:
    """The builder adds its outputs itself. In a repair, pynixd lets go of each output's lock, and each add the
    builder makes repairs its path; the daemon finds the output valid and repairs nothing (Lillecarl/nix#400)."""
    if NIX_GIT_BIN is None:
        pytest.skip("NIX_GIT_BIN names no Nix of the master branch")
    for adds in (OpRoute.pynixd, OpRoute.daemon):
        before = _fallthrough()
        here = await _rpc_repaired(tmp_path, OpRoute.pynixd, NIX_GIT_BIN, adds)
        assert _fallthrough() == before, f"the daemon repaired the pynixd route's builder-rpc-v0 build ({adds.name})"
        assert here["rc"] == 0, here
        assert here["contents"] == "same\n", here
    there = await _rpc_repaired(tmp_path, OpRoute.daemon, NIX_GIT_BIN, OpRoute.pynixd)
    assert there["rc"] == 0, there
    assert there["contents"] == "corrupt\n", there
