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
from pynixd.allocator import BuildAllocator, TelemetryStoreRanker, short_of_disk
from pynixd.config import PressureSettings, StoreRankingSettings
from pynixd.daemon_extensions.pynixd_host_health import (
    PynixdHostHealthRequest,
    PynixdHostHealthResponse,
)
from pynixd.handlers.pynixd_host_health import PynixdHostHealthHandler
from pynixd.monitor import PeerHealthMonitor
from pynixd.psi import DiskSpace, SystemHealth
from pynixd.scheduler import Scheduler
from pynixd.serde import BasicDerivation
from pynixd.serde.auth import Role
from pynixd.serde.context import WriteContext
from pynixd.wire import BytesReader, BytesWriter

if TYPE_CHECKING:
    from pathlib import Path

GIB = 1024**3
SETTINGS = StoreRankingSettings(min_free_bytes=2 * GIB)


def _store(free: int | None, held_until: float = 0.0) -> Any:
    health = SystemHealth(disk=None if free is None else DiskSpace(free_bytes=free, total_bytes=100 * GIB))
    return SimpleNamespace(
        store_id=StoreId("sg1-n1"), monitor=SimpleNamespace(health=health), disk_full_until=held_until
    )


class TestShortOfDisk:
    def test_a_store_below_the_floor_gets_no_build(self):
        assert short_of_disk(_store(GIB), SETTINGS, now=0.0)

    def test_a_store_with_room_gets_builds(self):
        assert not short_of_disk(_store(10 * GIB), SETTINGS, now=0.0)

    def test_a_store_with_no_figure_is_held_back_after_running_out(self):
        assert short_of_disk(_store(None, held_until=100.0), SETTINGS, now=50.0)
        assert not short_of_disk(_store(None, held_until=100.0), SETTINGS, now=150.0)

    def test_a_measured_figure_beats_the_hold_back(self):
        """A builder that reports room again builds again, hold-back or not."""
        assert not short_of_disk(_store(10 * GIB, held_until=100.0), SETTINGS, now=50.0)


def _candidate(store_id: str, free: int | None) -> Any:
    store = _store(free)
    store.__dict__.update(
        store_id=StoreId(store_id),
        cpu_util=None,
        in_flight=0,
        score_penalty=0,
        priority=1.0,
        is_healthy=True,
        draining=False,
        no_schedule=False,
        supports_derivation=lambda *_a: True,
    )
    return store


def test_the_allocator_skips_a_full_store():
    local = _candidate(LOCAL_STORE_ID, 10 * GIB)
    stores = {LOCAL_STORE_ID: local, StoreId("sg1-n1"): _candidate("sg1-n1", GIB)}
    ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=SETTINGS)))
    build = SimpleNamespace(
        request=SimpleNamespace(derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh")),
        is_blacklisted=lambda _store_id: False,
    )

    ranked = BuildAllocator(cast("Any", stores), local, ranker).rank_stores(cast("Any", build), {})

    assert [candidate.store_id for candidate in ranked] == [LOCAL_STORE_ID]


@pytest.mark.parametrize(
    ("message", "held"),
    [
        ("error: writing to file: No space left on device", True),
        ("builder for '/nix/store/...-x.drv' failed with exit code 1", False),
    ],
)
def test_no_space_left_holds_the_store_back(message: str, held: bool):
    store = _store(None)
    scheduler = SimpleNamespace(ranker=SimpleNamespace(settings=SETTINGS))

    Scheduler._note_disk_full(cast("Any", scheduler), store, message)  # noqa: SLF001 -- the hold-back is the unit

    assert short_of_disk(store, SETTINGS, now=time.monotonic()) is held


@pytest.mark.anyio
async def test_a_pynixd_peer_answers_its_hosts_health_and_disk(tmp_path: Path):
    writer = BytesWriter("test")
    await PynixdHostHealthRequest().to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    proxy = SimpleNamespace(
        r=BytesReader(writer.get_bytes()[8:], "test"),
        version=PROTOCOL_VERSION,
        standard_features=frozenset(),
        local_store=SimpleNamespace(layout=SimpleNamespace(real_store_dir=tmp_path)),
    )

    answer = await PynixdHostHealthHandler().handle(cast("Any", SimpleNamespace(proxy=proxy, role=Role.ADMIN)))

    assert isinstance(answer, PynixdHostHealthResponse)
    data = json.loads(answer.payload)
    assert data["disk"]["total_bytes"] > 0
    assert "/proc/meminfo" in data["files"]


@pytest.mark.anyio
async def test_the_peer_monitor_reads_the_disk_the_peer_answered():
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
