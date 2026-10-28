"""A session holds the roots Nix's worker would hold for its client.

Nix roots each path a client adds (`local-store.cc:1052`, `:1264`) and each
output it builds (`derivation-goal.cc:67`) for that client's worker, so the
root lives as long as the client. pynixd left them to a pooled upstream
worker, whose roots end when `CollectGarbage` retires it. And when pynixd
cannot write its own roots file, `AddTempRoot` answered success with no root
at all. nixidae/nixidae#1007.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation
from pynixd.goals.engine import _realised_paths  # noqa: PLC2701 -- the selection is the unit
from pynixd.proxy import DaemonProxy
from pynixd.serde import (
    AddTempRootRequest,
    BuildPathsWithResultsResponse,
    BuildResult,
    BuildResultStatus,
    DerivedPath,
    KeyedBuildResult,
)
from pynixd.store_path import StorePath

OUT = "/nix/store/00000000000000000000000000000001-out"
OPAQUE = "/nix/store/00000000000000000000000000000002-opaque"
FAILED = "/nix/store/00000000000000000000000000000003-failed"


def _keyed(path: str, status: BuildResultStatus, outputs: dict[str, str] | None = None) -> KeyedBuildResult:
    result = BuildResult(status=int(status))
    if outputs:
        result = BuildResult(
            status=int(status),
            built_outputs_by_name={
                name: UnkeyedRealisation(out_path=StorePath(p), signatures=set()) for name, p in outputs.items()
            },
        )
    return KeyedBuildResult(path=DerivedPath(path), result=result)


def test_every_realised_output_and_opaque_path_is_held() -> None:
    response = BuildPathsWithResultsResponse(
        results=[
            _keyed("/nix/store/00000000000000000000000000000004-x.drv!out", BuildResultStatus.BUILT, {"out": OUT}),
            _keyed(OPAQUE, BuildResultStatus.SUBSTITUTED),
            _keyed(FAILED, BuildResultStatus.MISC_FAILURE),
        ]
    )

    assert {str(path) for path in _realised_paths(response)} == {OUT, OPAQUE}


class UnwritableRoots:
    async def add(self, _path: Any) -> bool:
        return False


class Daemon:
    def __init__(self) -> None:
        self.asked: list[Any] = []

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        self.asked.append(request)
        return SimpleNamespace(value=1)


@pytest.mark.anyio
async def test_a_root_pynixd_cannot_write_goes_to_the_daemon() -> None:
    daemon = Daemon()
    proxy = SimpleNamespace(_temp_roots=UnwritableRoots(), local_store=daemon)

    await DaemonProxy.add_temp_root(cast("Any", proxy), OUT)

    assert [type(request) for request in daemon.asked] == [AddTempRootRequest]
    assert str(daemon.asked[0].path) == OUT
