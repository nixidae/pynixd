"""A delegated build that comes back to the instance that delegated it fails at once."""

from __future__ import annotations

import anyio
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.build_queue import BuildQueue
from pynixd.exceptions import DelegationLoopError
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode
from pynixd.store_path import StorePath

DRV = "/nix/store/00000000000000000000000000000001-probe.drv"


def _request() -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=DRV),
        derivation=BasicDerivation(platform="aarch64-linux", builder="/bin/sh"),
        build_mode=BuildMode.NORMAL,
    )


async def _queue_running_on(store_id: StoreId | None) -> BuildQueue:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request())
    if store_id is not None:
        build = queue.by_id[build_id]
        build.worker = anyio.CancelScope()
        build.assigned_store_id = store_id
    return queue


@pytest.mark.anyio
async def test_delegated_request_for_a_build_delegated_from_here_fails() -> None:
    queue = await _queue_running_on(StoreId("macbook"))

    with pytest.raises(DelegationLoopError, match=f"{DRV} is already delegated from here to macbook"):
        await queue.enqueue(_request(), delegated=True)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("store_id", "delegated"),
    [
        (StoreId("macbook"), False),
        (LOCAL_STORE_ID, True),
        (None, True),
    ],
    ids=["local-client-joins-remote-build", "delegated-joins-local-build", "delegated-joins-pending-build"],
)
async def test_other_requests_join_the_queued_build(store_id: StoreId | None, *, delegated: bool) -> None:
    queue = await _queue_running_on(store_id)
    (first,) = queue.by_id

    joined, _ = await queue.enqueue(_request(), delegated=delegated)

    assert joined == first
