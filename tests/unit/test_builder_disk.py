"""A builder's free disk is measured, shown, and kept from a build when it runs short.

On 2026-10-21 sg1-n1 and sg1-n3 filled their disks while pynixd kept
sending them builds, and croshome's `kubecove` failed with "No space left
on device". nixidae/nixidae#1013.
"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.constants import PROTOCOL_VERSION
from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.allocator import (
    SHORT_OF_DISK,
    BuildAllocator,
    DiskWait,
    ExcludedStore,
    TelemetryStoreRanker,
    disk_wait,
    short_of_disk,
)
from pynixd.build_queue import BuildQueue
from pynixd.config import PressureSettings, StoreRankingSettings
from pynixd.daemon_extensions.pynixd_host_health import (
    HEALTH_JSON,
    PynixdHostHealthRequest,
    PynixdHostHealthResponse,
)
from pynixd.handlers.pynixd_host_health import PynixdHostHealthHandler
from pynixd.monitor import PeerHealthMonitor, local_disk
from pynixd.psi import DiskSpace, ReclaimSample, SystemHealth
from pynixd.serde.auth import Role
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.store.local_host import disk_floor_from_nix_config
from pynixd.store.scheduled import ScheduledStore
from pynixd.wire import BytesReader, BytesWriter
from tests._conftest.execution import fake_executor
from tests._conftest.placement import FakeBuild, FakeMonitor, FakeStore

if TYPE_CHECKING:
    from pathlib import Path

GIB = 1024**3
SETTINGS = StoreRankingSettings(min_free_bytes=2 * GIB)
SG1 = StoreId("sg1-n1")
SG1_MIN_FREE = 16 * GIB
"""`min-free` on sg1's nodes (solid-kubernetes `environments/sg1/nixos/modules/common/node.nix`)."""


def _store(free: int | None, held_until: float = 0.0, store_id: StoreId = SG1, floor: int = 0) -> FakeStore:
    health = SystemHealth(
        disk=None if free is None else DiskSpace(free_bytes=free, total_bytes=100 * GIB, floor_bytes=floor)
    )
    store = FakeStore(store_id, monitor=FakeMonitor(health))
    store.disk_full_until = held_until
    return store


class TestShortOfDisk:
    def test_a_store_below_the_floor_gets_no_build(self) -> None:
        assert short_of_disk(_store(GIB), SETTINGS, now=0.0)

    def test_a_store_with_room_gets_builds(self) -> None:
        assert not short_of_disk(_store(10 * GIB), SETTINGS, now=0.0)

    def test_a_store_with_no_figure_is_held_back_after_running_out(self) -> None:
        assert short_of_disk(_store(None, held_until=100.0), SETTINGS, now=50.0)
        assert not short_of_disk(_store(None, held_until=100.0), SETTINGS, now=150.0)

    def test_a_measured_figure_beats_the_hold_back(self) -> None:
        """A builder that reports room again builds again, hold-back or not."""
        assert not short_of_disk(_store(10 * GIB, held_until=100.0), SETTINGS, now=50.0)

    @pytest.mark.parametrize(("floor", "short"), [(0, False), (SG1_MIN_FREE, True)])
    def test_the_hosts_own_min_free_counts_as_used(self, floor: int, short: bool) -> None:
        """sg1-n1 had 17.9 GB free over a 16 GiB `min-free`: every build there waited on its Nix's collection.

        The same free space with no floor is room; the floor is what keeps the build away.
        """
        assert short_of_disk(_store(17_886_392_320, floor=floor), SETTINGS, now=0.0) is short

    def test_room_over_the_floor_gets_builds(self) -> None:
        assert not short_of_disk(_store(SG1_MIN_FREE + 3 * GIB, floor=SG1_MIN_FREE), SETTINGS, now=0.0)


def test_a_wait_for_disk_names_the_hosts_min_free() -> None:
    stores = {SG1: _store(17 * GIB, floor=SG1_MIN_FREE)}

    verdict = disk_wait([ExcludedStore(SG1, SHORT_OF_DISK)], stores, SETTINGS)

    assert verdict == DiskWait(
        never_fits=False,
        text="waiting for disk space: sg1-n1 has 17408 MiB free, under its min-free (16384 MiB)"
        " plus ranking.min_free_bytes (2048 MiB)",
    )


def test_a_disk_no_bigger_than_its_floor_never_fits() -> None:
    disk = DiskSpace(free_bytes=GIB, total_bytes=17 * GIB, floor_bytes=SG1_MIN_FREE)
    stores = {SG1: FakeStore(SG1, monitor=FakeMonitor(SystemHealth(disk=disk)))}

    verdict = disk_wait([ExcludedStore(SG1, SHORT_OF_DISK)], stores, SETTINGS)

    assert verdict is not None
    assert verdict.never_fits


@pytest.mark.parametrize(
    ("config", "floor"),
    [({"min-free": {"value": SG1_MIN_FREE}}, SG1_MIN_FREE), ({"min-free": {"value": 0}}, 0), ({}, 0)],
)
def test_the_floor_is_the_stores_min_free(config: dict[str, Any], floor: int) -> None:
    assert disk_floor_from_nix_config(config) == floor


@pytest.mark.parametrize(
    ("sample", "floor"),
    [
        ('{"disk": {"free_bytes": 5, "total_bytes": 9}}', 0),
        ('{"disk": {"free_bytes": 5, "total_bytes": 9, "floor_bytes": 3}}', 3),
    ],
)
def test_a_sample_reads_with_or_without_a_floor(sample: str, floor: int) -> None:
    """A peer that predates the floor reports none; one that has it reports it."""
    health = HEALTH_JSON.validate_json(sample)
    assert health.disk == DiskSpace(free_bytes=5, total_bytes=9, floor_bytes=floor)


def test_the_allocator_skips_a_full_store() -> None:
    local = _store(10 * GIB, store_id=LOCAL_STORE_ID)
    stores = {LOCAL_STORE_ID: local, SG1: _store(GIB)}
    ranker = TelemetryStoreRanker(SETTINGS)

    ranked = BuildAllocator(stores, local, ranker).rank_stores(FakeBuild(), {})

    assert [candidate.store_id for candidate in ranked] == [LOCAL_STORE_ID]


@pytest.mark.parametrize(
    ("message", "held"),
    [
        ("error: writing to file: No space left on device", True),
        ("builder for '/nix/store/...-x.drv' failed with exit code 1", False),
    ],
)
def test_no_space_left_holds_the_store_back(message: str, held: bool) -> None:
    store = _store(None)

    fake_executor(BuildQueue(), {SG1: store}, settings=SETTINGS).note_disk_full(store, message)

    assert short_of_disk(store, SETTINGS, now=time.monotonic()) is held


@pytest.mark.anyio
async def test_a_pynixd_peer_answers_its_hosts_health_and_disk(tmp_path: Path) -> None:
    writer = BytesWriter("test")
    await PynixdHostHealthRequest().to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    proxy = SimpleNamespace(
        r=BytesReader(writer.get_bytes()[8:], "test"),
        version=PROTOCOL_VERSION,
        standard_features=frozenset(),
        local_store=SimpleNamespace(
            daemon_client=SimpleNamespace(disk_sampler=lambda: local_disk(tmp_path, lambda: SG1_MIN_FREE))
        ),
    )
    proxy.read_context = ReadContext(reader=proxy.r, version=proxy.version, features=proxy.standard_features)

    answer = await PynixdHostHealthHandler().handle(cast("Any", SimpleNamespace(proxy=proxy, role=Role.ADMIN)))

    assert isinstance(answer, PynixdHostHealthResponse)
    data = json.loads(answer.payload)
    assert data["disk"]["total_bytes"] > 0
    assert data["disk"]["floor_bytes"] == SG1_MIN_FREE
    assert "/proc/meminfo" in data["files"]


@pytest.mark.anyio
async def test_the_peer_monitor_reads_the_disk_the_peer_answered() -> None:
    payload = json.dumps(
        {"files": {"/proc/loadavg": "0.5 0.4 0.3 1/100 1\n"}, "cores": 4.0, "disk": {"free_bytes": 5, "total_bytes": 9}}
    )
    samples: list[SystemHealth] = []

    class Peer:
        store_id = "sg1-n1"
        features = frozenset({PynixdHostHealthRequest.name})

        async def execute(self, _request: Any) -> Any:
            monitor.running = False
            return PynixdHostHealthResponse(payload=payload)

    monitor = PeerHealthMonitor(PressureSettings(interval=0.0), samples.append, Peer())
    monitor.running = True
    await monitor.run()

    assert samples[-1].disk == DiskSpace(free_bytes=5, total_bytes=9)
    assert samples[-1].load1 == 0.5


class TestRoom:
    """Free plus reclaimable, less the floor, from one GC plan. nixidae/nixidae#1527."""

    def test_sg1_n1_has_its_free_space_over_the_floor_and_nothing_more(self) -> None:
        """Every path live, so a collection frees nothing: the room is what sits over min-free."""
        sample = ReclaimSample(free_bytes=17_886_392_320, reclaimable_bytes=0, at=100.0)
        assert sample.room(SG1_MIN_FREE, now=130.0) == (17_886_392_320 - SG1_MIN_FREE, 30.0)

    def test_dead_paths_count_as_room(self) -> None:
        sample = ReclaimSample(free_bytes=GIB, reclaimable_bytes=5 * GIB, at=0.0)
        assert sample.room(0, now=0.0) == (6 * GIB, 0.0)

    @pytest.mark.anyio
    async def test_the_sampler_carries_the_last_plans_room(self, tmp_path: Path) -> None:
        store = SimpleNamespace(
            layout=SimpleNamespace(real_store_dir=tmp_path),
            disk_floor_bytes=GIB,
            disk_reclaim=ReclaimSample(free_bytes=3 * GIB, reclaimable_bytes=2 * GIB, at=time.monotonic()),
        )
        disk = await ScheduledStore.disk_sampler(cast("Any", store))()
        assert disk is not None
        assert disk.room_bytes == 4 * GIB
        assert disk.room_age_s is not None
        assert disk.room_age_s < 60

    @pytest.mark.anyio
    async def test_no_plan_yet_reports_no_room(self, tmp_path: Path) -> None:
        store = SimpleNamespace(layout=SimpleNamespace(real_store_dir=tmp_path), disk_floor_bytes=0, disk_reclaim=None)
        disk = await ScheduledStore.disk_sampler(cast("Any", store))()
        assert disk is not None
        assert disk.room_bytes is None

    def test_a_sample_carries_the_room_across_the_wire(self) -> None:
        sent = SystemHealth(disk=DiskSpace(free_bytes=5, total_bytes=9, floor_bytes=1, room_bytes=7, room_age_s=2.5))
        assert HEALTH_JSON.validate_json(HEALTH_JSON.dump_json(sent)).disk == sent.disk
