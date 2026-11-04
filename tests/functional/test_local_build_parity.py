"""pynixd's sandbox builds what the daemon builds (`docs/design/builder.md`, proof 5).

The same two derivations build once with `BuildDerivation` on the `pynixd`
route and once on the `daemon` route, each into a fresh test store. The
output's NAR hash, the `ValidPaths` and `Refs` rows and the build log lines
must be equal. `registrationTime` and the row ids differ by nature.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

DEP = (
    'derivation { name = "parity-dep"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo dep > $out" ]; }'
)
# A reference to the dependency, a reference to itself, a carriage return and stderr.
TOP = (
    f"let dep = {DEP}; in "
    'derivation { name = "parity-top"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo one; printf \'50%%\\r100%%\\n\'; echo two >&2; printf tail; echo ${dep} $out > $out" ]; }'
)


def _script(name: str, body: str) -> str:
    return (
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f"args = [ (builtins.toFile \"{name}.sh\" ''\n{body}\n'') ]; }}"
    )


# More lines than `log-lines` keeps, a `@nix` line after each, and a last line that no newline ends.
FAILS = _script(
    "parity-fails",
    """i=0
while [ $i -lt 30 ]; do echo line $i; echo '@nix {"action":"setPhase","phase":"p"}'; i=$((i+1)); done
printf partial
exit 3""",
)

# Each `@nix` action an untrusted builder may send, and lines `handleJSONLogMessage` refuses.
JSON = _script(
    "parity-json",
    """echo before
echo '@nix {"action":"setPhase","phase":"buildPhase"}'
echo '@nix {"action":"msg","level":0,"msg":"builder says hi"}'
echo '@nix {"action":"msg","level":7,"msg":"too quiet"}'
echo '@nix {"action":"start","id":5,"type":105,"level":0,"text":"not a transfer"}'
echo '@nix {"action":"start","id":6,"type":101,"level":0,"text":"downloading","fields":["u"]}'
echo '@nix {"action":"result","id":6,"type":105,"fields":[1,2,0,0]}'
echo '@nix {"action":"stop","id":6}'
echo '@nix {"action":"msg","level":"loud","msg":"x"}'
echo '@nix {"action":"dance"}'
echo '@nix {bad'
echo after
echo done > $out""",
)
BAD_JSON = "bad JSON log message from the derivation builder: "

FAKE_HASH = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


def _fod(name: str, mode: str, command: str, output_hash: str = FAKE_HASH) -> str:
    return (
        f"let dep = {DEP}; in "
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f'args = [ "-c" "{command}" ]; outputHashMode = "{mode}"; outputHashAlgo = "sha256"; '
        f'outputHash = "{output_hash}"; }}'
    )


FOD_FLAT = _fod("parity-fod-flat", "flat", "echo flat > $out")
# A self-reference moves the output to its real path, so `rewriteOutput` changes its content.
FOD_SELF = _fod("parity-fod-self", "recursive", "echo $out > $out")
FOD_DEP = "parity-fod-dep", "recursive", "echo ${dep} > $out"


def _rows(root: Path) -> tuple[list[tuple[Any, ...]], list[tuple[str, str]]]:
    db = sqlite3.connect(f"file:{root}/nix/var/nix/db/db.sqlite?mode=ro", uri=True)
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


def _without_times(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _without_times(v) for k, v in value.items() if k != "registrationTime"}
    if isinstance(value, list):
        return [_without_times(v) for v in value]
    return value


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, route: OpRoute) -> AsyncIterator[tuple[str, Path]]:
    """A fresh store with `BuildDerivation` on *route*: its URL and its root."""
    root = tmp_path / route.name / "store"
    socket = tmp_path / f"{route.name}.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        op_routes={"BuildDerivation": route},
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}", root


async def _build(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    async with _serve(tmp_path, route) as (url, root):
        _rc, out, err, _both = await run_subproc(
            [
                NIX_BIN,
                "build",
                "--store",
                url,
                "--impure",
                "--no-link",
                "--print-out-paths",
                "--print-build-logs",
                "--expr",
                TOP,
            ]
        )
        top = out.strip()
        _rc, info, _err, _both = await run_subproc([NIX_BIN, "path-info", "--store", url, "--json", "--recursive", top])
    valid, refs = _rows(root)
    return {
        "top": top,
        "info": _without_times(json.loads(info)),
        "valid": valid,
        "refs": refs,
        "log": [line for line in err.splitlines() if line.startswith("parity-")],
    }


async def test_pynixd_builds_what_the_daemon_builds(tmp_path: Path) -> None:
    here = await _build(tmp_path, OpRoute.pynixd)
    there = await _build(tmp_path, OpRoute.daemon)

    assert here["top"] == there["top"]
    assert here["info"] == there["info"]
    assert here["valid"] == there["valid"]
    assert here["refs"] == there["refs"]
    assert here["log"] == there["log"]
    assert any(line.endswith("100%") for line in here["log"]), here["log"]


async def _fail(tmp_path: Path, route: OpRoute) -> list[str]:
    async with _serve(tmp_path, route) as (url, _root):
        _rc, _out, err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", FAILS], expected_retcode=1
        )
    return _errors(err)


async def test_a_failed_build_says_what_the_daemon_says(tmp_path: Path) -> None:
    """`fixupBuilderFailureErrorMessage`: the reason, the outputs, the last `log-lines` lines, `nix log`."""
    here = await _fail(tmp_path, OpRoute.pynixd)
    there = await _fail(tmp_path, OpRoute.daemon)

    assert here == there
    assert any("Last 25 log lines:" in line for line in here), here
    assert any(line.rstrip().endswith("> partial") for line in here), here
    assert not any("@nix" in line for line in here), here


async def _json(tmp_path: Path, route: OpRoute) -> list[str]:
    async with _serve(tmp_path, route) as (url, _root):
        _rc, _out, err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-build-logs", "--expr", JSON]
        )
    # NIX-DEVIATION (#27) in `jsonlog.handle`: the parse error's own words differ.
    return [line.split(BAD_JSON, 1)[0] + BAD_JSON if BAD_JSON in line else line for line in err.splitlines()]


async def test_a_builders_json_lines_reach_the_client_as_the_daemon_sends_them(tmp_path: Path) -> None:
    here = await _json(tmp_path, OpRoute.pynixd)
    there = await _json(tmp_path, OpRoute.daemon)

    assert here == there
    assert "builder says hi" in here, here
    assert not any("too quiet" in line or "not a transfer" in line for line in here), here


def _probe(top: str, dep: str) -> str:
    """What the build sees of `/top`, of its store path and of the dependency only its closure brings."""
    checks = "; ".join(f"if [ -e {p} ]; then echo yes; else echo no; fi" for p in ("/top", top, dep))
    return (
        'derivation { name = "parity-probe"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f'args = [ "-c" "({checks}) > $out" ]; }}'
    )


async def _sandbox_closure(tmp_path: Path, route: OpRoute) -> tuple[int, str]:
    async with _serve(tmp_path, route) as (url, root):
        build = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths"]
        _rc, top, _err, _both = await run_subproc([*build, "--expr", TOP])
        top = top.strip()
        _rc, dep, _err, _both = await run_subproc([NIX_BIN, "path-info", "--store", url, "--expr", DEP, "--impure"])
        probe_expr = _probe(top, dep.strip())
        # Optional: a source names a host path, and this store's files are under its root.
        rc, probe, err, _both = await run_subproc(
            [*build, "--option", "extra-sandbox-paths", f"/top={top}?", "--expr", probe_expr], expected_retcode=None
        )
        return rc, (root / probe.strip().lstrip("/")).read_text() if rc == 0 else err


async def test_a_sandbox_path_in_the_store_brings_its_closure(tmp_path: Path) -> None:
    """`DerivationBuildingGoal` binds the closure of each store path `sandbox-paths` names.

    NIX-DEFECT (#23) in `local._binds`: the daemon binds the closure from the
    store path, which a store at `?root=` does not have (Lillecarl/nix#392).
    """
    here = await _sandbox_closure(tmp_path, OpRoute.pynixd)
    there = await _sandbox_closure(tmp_path, OpRoute.daemon)

    # `/top` names a host path that this store does not have; its closure is in the store.
    assert here == (0, "no\nyes\nyes\n")
    assert there[0] == 1
    assert "-parity-dep" in there[1] and "No such file or directory" in there[1], there[1]


def _errors(err: str) -> list[str]:
    lines = err.splitlines()
    return lines[next(i for i, line in enumerate(lines) if line.startswith("error:")) :]


async def _fixed(tmp_path: Path, route: OpRoute) -> dict[str, Any]:
    errors: dict[str, list[str]] = {}
    async with _serve(tmp_path, route) as (url, root):

        async def build(expr: str) -> list[str]:
            _rc, _out, err, _both = await run_subproc(
                [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", expr], expected_retcode=1
            )
            return _errors(err)

        errors["flat"] = await build(FOD_FLAT)
        errors["self"] = await build(FOD_SELF)
        errors["dep"] = await build(_fod(*FOD_DEP))
        got = next(line.split()[-1] for line in errors["dep"] if line.strip().startswith("got:"))
        # The right hash: the path the mismatch registered is valid, and the reference still fails the build.
        errors["dep-right"] = await build(_fod(*FOD_DEP, output_hash=got))
    valid, refs = _rows(root)
    return {"errors": errors, "valid": valid, "refs": refs}


async def test_a_fixed_output_with_the_wrong_hash_stays_valid_under_its_real_one(tmp_path: Path) -> None:
    """`registerOutputs` registers a content-addressed output before `checkOutputs` refuses it."""
    here = await _fixed(tmp_path, OpRoute.pynixd)
    there = await _fixed(tmp_path, OpRoute.daemon)

    assert here["errors"] == there["errors"]
    assert here["valid"] == there["valid"]
    assert here["refs"] == there["refs"]
    assert all(any("hash mismatch" in line for line in here["errors"][k]) for k in ("flat", "self", "dep")), here
    assert any("must not reference store paths" in line for line in here["errors"]["dep-right"]), here
    names = [row[0].split("-", 1)[1] for row in here["valid"]]
    assert {"parity-fod-flat", "parity-fod-self", "parity-fod-dep", "parity-dep"} <= set(names), names


TWINS = (
    'derivation { name = "parity-twins"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'outputs = [ "out" "dev" ]; args = [ "-c" "echo twin > $out; echo twin > $dev" ]; }'
)


async def _optimised(tmp_path: Path, route: OpRoute, *, optimise: bool) -> dict[str, Any]:
    base = tmp_path / f"o{int(optimise)}"
    base.mkdir(exist_ok=True)
    async with _serve(base, route) as (url, root):
        flags = ["--option", "auto-optimise-store", "true"] if optimise else []
        _rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", *flags, "--expr", TWINS]
        )
    store = root / "nix/store"
    links = {entry.stat().st_ino for entry in (store / ".links").iterdir()} if (store / ".links").exists() else set()
    outputs = {}
    for path in out.split():
        st = (store / path.rsplit("/", 1)[-1]).lstat()
        outputs[path.rsplit("/", 1)[-1].split("-", 1)[1]] = (st.st_nlink, st.st_ino in links)
    return outputs


async def test_auto_optimise_store_links_the_outputs_of_a_build(tmp_path: Path) -> None:
    """`registerOutputs` calls `optimisePath` (`derivation-builder.cc:1600`) on each output it registers."""
    for optimise in (True, False):
        here = await _optimised(tmp_path, OpRoute.pynixd, optimise=optimise)
        there = await _optimised(tmp_path, OpRoute.daemon, optimise=optimise)
        assert here == there, optimise
        expected = (3, True) if optimise else (1, False)
        assert here == {"parity-twins": expected, "parity-twins-dev": expected}, here


async def _signed(tmp_path: Path, route: OpRoute, key: Path) -> list[tuple[Any, ...]]:
    async with _serve(tmp_path, route) as (url, root):
        build = [NIX_BIN, "build", "--store", url, "--impure", "--no-link"]
        await run_subproc([*build, "--option", "secret-key-files", str(key), "--expr", TOP])
        await run_subproc(
            [*build, "--option", "extra-secret-key-files", str(key), "--expr", FOD_FLAT], expected_retcode=1
        )
    return _rows(root)[0]


async def test_a_built_output_carries_the_signature_of_each_secret_key(tmp_path: Path) -> None:
    """`registerOutputs` calls `signPathInfo` (`derivation-builder.cc:1605`) for every output it registers."""
    key = tmp_path / "secret"
    _rc, secret, _err, _both = await run_subproc([NIX_BIN, "key", "generate-secret", "--key-name", "parity-1"])
    key.write_text(secret)
    here = await _signed(tmp_path, OpRoute.pynixd, key)
    there = await _signed(tmp_path, OpRoute.daemon, key)

    assert here == there
    sigs = {row[0].split("-", 1)[1]: row[5] for row in here if not row[0].endswith(".drv")}
    assert set(sigs) == {"parity-dep", "parity-top", "parity-fod-flat"}, sigs
    assert all(s and s.startswith("parity-1:") for s in sigs.values()), sigs
