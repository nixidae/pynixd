"""pynixd's check with a `diff-hook`, as `handleDiffHook` (`unix/build/derivation-builder.cc:121`).

A check whose output differs moves the rebuilt output to `<path>.check` and runs the hook as the build user, with
the valid output, the rebuilt one, the derivation and the build directory (`:1552`). The guest's `root` suite runs
these as root; anywhere else they skip. `tests/functional/test_local_build_parity.py` holds the hook of a daemon
that is not root.
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

HERE = Path("/var/lib/pynixd-root-tests/diff-hook")
IDS_PER_BUILD = 1 << 16
START_ID = 40000
"""Inside a container guest's 65536 ids, as `test_auto_allocate_uids.py`."""

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="build users need root")

DIFFERS = (
    'derivation { name = "hook-differs"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "read u < /proc/sys/kernel/random/uuid; echo $u > $out" ]; }'
)
# Shell builtins only: the hook runs with the daemon's environment.
HOOK = """#!/bin/sh
{{
  echo "$#|$1|$2|$3|${{4##*/}}"
  test -e "$1" && echo a || echo noa
  test -e "$2" && echo b || echo nob
  test -d "$4" && echo tmp || echo notmp
  pwd
  while read k v rest; do case $k in Uid:|Gid:|Groups:) echo $k $v $rest;; esac; done < /proc/self/status
}} > {record}
echo hook says hi
echo hook to stderr >&2
exit {code}
"""


@pytest.fixture(autouse=True)
def _clean() -> None:
    rmtree_robust(HERE)


def _fallthrough() -> float:
    return sum(
        sample.value
        for metric in REGISTRY.collect()
        if metric.name == "pynixd_op_fallthrough"
        for sample in metric.samples
        if sample.name == "pynixd_op_fallthrough_total" and sample.labels.get("op") == "BuildDerivation"
    )


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, route: OpRoute, hook: Path) -> AsyncIterator[tuple[str, Path]]:
    root = HERE / route.name / "store"
    socket = tmp_path / f"{route.name}.sock"
    nix_config = for_test_store(substituters=(), experimental_features=("auto-allocate-uids",))
    nix_config.auto_allocate_uids = True
    nix_config.start_id = START_ID
    nix_config.id_count = IDS_PER_BUILD
    nix_config.run_diff_hook = True
    nix_config.diff_hook = str(hook)
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=nix_config,
        op_routes={"BuildDerivation": route},
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}", root


async def _check(tmp_path: Path, route: OpRoute, code: int) -> dict[str, Any]:
    """A build, then a check of it under a hook that exits *code*: what the client and the hook saw."""
    here = HERE / f"{route.name}-{code}"
    here.mkdir(parents=True)
    here.chmod(0o777)
    record = here / "record"
    hook = here / "hook"
    hook.write_text(HOOK.format(record=record, code=code))
    hook.chmod(0o755)
    async with _serve(tmp_path, route, hook) as (url, root):
        common = [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--expr", DIFFERS]
        await run_subproc(common)
        rc, _out, err, _both = await run_subproc([*common, "--rebuild"], expected_retcode=None)
    checks = sorted(p.name.split("-", 1)[1] for p in (root / "nix" / "store").glob("*.check"))
    seen = record.read_text().replace(str(root), "<root>").splitlines() if record.exists() else None
    return {
        "rc": rc,
        "lines": err.replace(str(hook), "<hook>").replace(str(root), "<root>").splitlines(),
        "checks": checks,
        "hook": seen,
    }


@pytest.mark.parametrize("code", [0, 3])
async def test_a_diff_hook_runs_as_the_daemon_runs_it(tmp_path: Path, code: int) -> None:
    """The hook runs as the slot's uid with no groups, from `/`; its output is an error line, its failure one too."""
    before = _fallthrough()
    here = await _check(tmp_path, OpRoute.pynixd, code)
    assert _fallthrough() == before, "the daemon checked the pynixd route's build"
    there = await _check(tmp_path, OpRoute.daemon, code)

    assert here == there
    assert here["rc"] == 1, here
    assert here["checks"] == ["hook-differs.check"], here
    hook = here["hook"]
    assert hook is not None, here
    assert hook[0].startswith("4|/nix/store/"), hook
    assert hook[0].endswith("|build"), hook
    # NIX-DEFECT (#23): the valid output's logical path, which in a relocated store names nothing, and a build
    # directory that the build user cannot enter (`hooks.diff_hook`).
    assert hook[1:5] == ["noa", "b", "notmp", "/"], hook
    ids = " ".join([str(START_ID)] * 4)
    assert hook[5:] == [f"Uid: {ids}", f"Gid: {ids}", "Groups:"], hook
    said = [line for line in here["lines"] if "hook-differs" not in line]
    if code == 0:
        assert said == ["hook says hi"], here
    else:
        assert len(said) == 1, here
        assert "diff hook execution failed" in said[0], here
