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

# More lines than `log-lines` keeps, and a last line that no newline ends.
FAILS = (
    'derivation { name = "parity-fails"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "i=0; while [ $i -lt 30 ]; do echo line $i; i=$((i+1)); done; printf partial; exit 3" ]; }'
)


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
    lines = err.splitlines()
    return lines[next(i for i, line in enumerate(lines) if line.startswith("error:")) :]


async def test_a_failed_build_says_what_the_daemon_says(tmp_path: Path) -> None:
    """`fixupBuilderFailureErrorMessage`: the reason, the outputs, the last `log-lines` lines, `nix log`."""
    here = await _fail(tmp_path, OpRoute.pynixd)
    there = await _fail(tmp_path, OpRoute.daemon)

    assert here == there
    assert any("Last 25 log lines:" in line for line in here), here
    assert any(line.rstrip().endswith("> partial") for line in here), here
