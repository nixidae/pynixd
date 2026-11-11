"""A build that no store takes for want of disk says why, and fails when no disk can ever fit it.

pynixd's daemon test (tests.daemon) had a 487 MiB store under the default
`ranking.min_free_bytes` of 2 GiB. The ranking left out the only store,
and every build waited in "scheduling" with no word to the client until
its 300 s timeout. nixidae/nixidae#1013 added the floor.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, BuildId, StoreId
from pynixd.allocator import (
    SHORT_OF_DISK,
    SILENT,
    UNDER_MIN_SCORE,
    BuildAllocator,
    ExcludedStore,
    TelemetryStoreRanker,
    disk_wait,
)
from pynixd.build_queue import BuildQueue
from pynixd.config import StoreRankingSettings
from pynixd.psi import DiskSpace, SystemHealth
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode
from pynixd_store.store_path import StorePath
from tests._conftest.placement import FakeMonitor, FakeStore, Spawned, fake_placer

MIB = 1024**2
GIB = 1024**3
SETTINGS = StoreRankingSettings(min_free_bytes=2 * GIB)
SG1 = StoreId("sg1-n1")
DRV = StorePath("/nix/store/00000000000000000000000000000001-disk.drv")


def _store(free: int, total: int, store_id: StoreId = LOCAL_STORE_ID) -> FakeStore:
    return FakeStore(store_id, monitor=FakeMonitor(SystemHealth(disk=DiskSpace(free_bytes=free, total_bytes=total))))


def test_a_disk_smaller_than_the_floor_never_fits() -> None:
    local = _store(428 * MIB, 487 * MIB)

    verdict = disk_wait([ExcludedStore(LOCAL_STORE_ID, SHORT_OF_DISK)], {LOCAL_STORE_ID: local}, SETTINGS)

    assert verdict is not None
    assert verdict.never_fits
    assert verdict.text == (
        "no store has the disk for a build: local has 487 MiB in all, under ranking.min_free_bytes (2048 MiB)"
    )


def test_a_large_full_disk_waits_and_says_how_full() -> None:
    local = _store(GIB, 100 * GIB)

    verdict = disk_wait([ExcludedStore(LOCAL_STORE_ID, SHORT_OF_DISK)], {LOCAL_STORE_ID: local}, SETTINGS)

    assert verdict is not None
    assert not verdict.never_fits
    assert verdict.text == "waiting for disk space: local has 1024 MiB free, under ranking.min_free_bytes (2048 MiB)"


@pytest.mark.parametrize("reason", [UNDER_MIN_SCORE, SILENT])
def test_a_store_that_may_come_back_keeps_the_build_waiting(reason: str) -> None:
    stores = {LOCAL_STORE_ID: _store(428 * MIB, 487 * MIB), SG1: FakeStore(SG1)}
    excluded = [ExcludedStore(LOCAL_STORE_ID, SHORT_OF_DISK), ExcludedStore(SG1, reason)]

    verdict = disk_wait(excluded, stores, SETTINGS)

    assert verdict is not None
    assert not verdict.never_fits


def test_no_disk_reason_is_no_verdict() -> None:
    assert disk_wait([ExcludedStore(SG1, SILENT)], {SG1: FakeStore(SG1)}, SETTINGS) is None


async def _place(local: FakeStore) -> tuple[BuildQueue, BuildId, Spawned, bool]:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(
        BuildDerivationRequest(
            drv_path=DRV,
            derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"),
            build_mode=BuildMode.NORMAL,
        )
    )
    stores = {LOCAL_STORE_ID: local}
    spawned = Spawned()
    ranking = BuildAllocator(stores, local, TelemetryStoreRanker(SETTINGS))
    placer = fake_placer(queue, ranking, spawned, local=local, stores=stores, settings=SETTINGS)
    waiting = await placer.assign([queue.by_id[build_id]], {})
    return queue, build_id, spawned, bool(waiting)


@pytest.mark.anyio
async def test_the_placer_fails_a_build_no_disk_can_fit() -> None:
    queue, build_id, spawned, waits = await _place(_store(428 * MIB, 487 * MIB))

    build = queue.by_id[build_id]
    assert not waits
    assert spawned.stores == []
    assert build.answer.done()
    assert "ranking.min_free_bytes" in str(build.answer.result().result.error_msg)


@pytest.mark.anyio
async def test_the_placer_tells_a_waiting_build_once() -> None:
    local = _store(GIB, 100 * GIB)
    queue, build_id, spawned, waits = await _place(local)
    build = queue.by_id[build_id]

    told = build.told_wait
    assert told is not None
    await build.tell_wait(told)

    assert waits
    assert spawned.stores == []
    assert told.startswith("waiting for disk space: local has 1024 MiB free")
    assert build._log_writer.get_bytes().count(told.encode()) == 1
