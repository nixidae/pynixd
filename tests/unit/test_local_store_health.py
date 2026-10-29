"""The local store reports its monitor's CPU figure, so it earns CPU-idle points. nixidae/nixidae#1125."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd.allocator import score_store
from pynixd.config import LocalSocketStoreSpec, PressureSettings, StoreRankingSettings
from pynixd.monitor import DummyResourceMonitor
from pynixd.psi import CpuUtil, MemInfo, SystemHealth
from pynixd.store.local_daemon import LocalStore

if TYPE_CHECKING:
    from pathlib import Path


def _store(tmp_path: Path, health: SystemHealth) -> LocalStore:
    store = LocalStore(
        LocalSocketStoreSpec(
            store_id=StoreId("local"),
            store_path=tmp_path,
            socket_path=tmp_path / "daemon.socket",
        ),
    )
    monitor = DummyResourceMonitor(PressureSettings(), store.on_pressure_sample)
    monitor.health = health
    store.monitor = monitor
    return store


def test_the_local_store_reports_its_monitors_cpu_and_memory(tmp_path: Path) -> None:
    cpu = CpuUtil(utilization=10.0, cores=16.0, throttled_pct=0.0)
    meminfo = MemInfo(mem_total=64 * 1024**2, mem_available=32 * 1024**2)
    store = _store(tmp_path, SystemHealth(cpu_util=cpu, meminfo=meminfo))

    assert store.cpu_util is cpu
    assert store.meminfo is meminfo


def test_an_idle_local_store_earns_cpu_idle_points(tmp_path: Path) -> None:
    """Without the CPU figure the local store got 0 of 100 points and fell under `min_schedule_score`."""
    store = _store(tmp_path, SystemHealth(cpu_util=CpuUtil(utilization=10.0, cores=16.0, throttled_pct=0.0)))

    terms = score_store(store, StoreRankingSettings(), in_flight=0, assigned_this_pass=0, transfer_seconds=None)

    assert terms.cpu_idle_points == 90.0
    assert terms.passed
