"""pynixd builds without the sandbox where Nix does (`makeDerivationBuilder`, `derivation-builder.cc:1751`).

Only a store whose real directory is its logical one builds without it: a
store at `?root=` always sandboxes (`:1782`). So each store here is a
`NIX_STORE_DIR` store, and the client names the same directory.
"""

from __future__ import annotations

import contextlib
import json
import socket as socket_module
import sqlite3
from typing import TYPE_CHECKING, Any

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_dir import real_store_dir, set_real_store_dir, set_store_dir, store_dir
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
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
    tmp_path: Path, route: OpRoute, sandbox: str, *, experimental_features: tuple[str, ...] = ()
) -> AsyncIterator[tuple[list[str], dict[str, str], Path]]:
    """A fresh `NIX_STORE_DIR` store with `BuildDerivation` on *route*: the build command, its environment, the state."""
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
        op_routes={"BuildDerivation": route},
        store_dir=store,
        state_dir=state,
        log_dir=logs,
    )
    env = {"NIX_STORE_DIR": str(store), "NIX_STATE_DIR": str(base / "client"), "NIX_LOG_DIR": str(logs)}
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=path, ssh_port=None, http_port=None):
        yield [str(NIX_BIN), "--store", f"unix://{path}", "--impure"], env, state


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
    """The build's error. The daemon route prints a line before the build starts (nixidae/nixidae#1193)."""
    lines = err.splitlines()
    first = next(i for i, line in enumerate(lines) if line.startswith("building '")) + 1
    return lines[next(i for i in range(first, len(lines)) if lines[i].startswith("error:")) :]


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
