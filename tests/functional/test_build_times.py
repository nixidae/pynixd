"""A build that ran answers when it ran, though the daemon that built it says 0 (nixidae/nixidae#1217).

`Store::buildDerivation` runs a `DerivationTrampolineGoal` (`entry-points.cc:85`), whose answer drops
`timesBuilt`, `startTime` and `stopTime` (`derivation-trampoline-goal.cc:227`). The scheduler fills what the
builder left at 0 (`with_build_times`).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import ExternalUnixStoreSpec
from pynixd.op_routes import OpRoute
from pynixd.serde import BuildMode, BuildPathsWithResultsRequest, BuildResultStatus, DerivedPath
from pynixd.store import ExternalUnixStore, LocalDBStore
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

BUILDS = (
    'derivation { name = "build-times"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo > $out" ]; }'
)


async def test_a_build_on_a_nix_daemon_answers_when_it_ran(tmp_path: Path) -> None:
    root = tmp_path / "store"
    socket = tmp_path / "pynixd.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        op_routes={"BuildDerivation": OpRoute.daemon},
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        _rc, drv, _err, _both = await run_subproc(
            [
                NIX_BIN,
                "eval",
                "--store",
                f"unix://{socket}?root={root}",
                "--impure",
                "--raw",
                "--expr",
                f"({BUILDS}).drvPath",
            ]
        )
        before = int(time.time())
        client_spec = ExternalUnixStoreSpec(store_id=StoreId("client"), socket_path=socket, store_path=root)
        async with ExternalUnixStore(client_spec) as client:
            request = BuildPathsWithResultsRequest(
                derived_paths=[DerivedPath(value=f"{drv.strip()}!*")], build_mode=BuildMode.NORMAL
            )
            response = await client.call(request, skip_probe=True)
        after = int(time.time())

    [keyed] = response.results
    result = keyed.result
    assert result.status == BuildResultStatus.BUILT, result
    assert result.times_built == 1, result
    assert result.start_time is not None and result.stop_time is not None, result
    assert before <= result.start_time <= result.stop_time <= after, (before, result, after)
