"""The room and pressure of a store's builds come from the cgroup they run in. nixidae/nixidae#1568.

sg1-n1, 2026-11-22: `nix-daemon.service` has `MemoryHigh=20G`, `MemoryMax=24G` and holds 21.5 GB, nearly all of it
anonymous; its `memory.pressure` reads `full avg10=59.85`. The host has 57 of 125 GB free, so the host's figures
read as room, and the build limit held at `steady` while 7-11 builds thrashed in the cap.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.allocator import memory_room
from pynixd.config import PressureSettings, PynixdSettings
from pynixd.daemon_extensions.pynixd_host_health import HEALTH_JSON
from pynixd.monitor import GenericResourcePoller, cgroup_memory
from pynixd.pressure import BuildLimit, LimitReason
from pynixd.psi import CgroupMemory, MemInfo, PsiMetric, PsiSnapshot, SystemHealth

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

GIB = 1024**3
UNIT = Path("/sys/fs/cgroup/system.slice/nix-daemon.service")
SLICE = UNIT.parent
PRESSURE = "some avg10=59.87 avg60=60.55 avg300=61.09 total=1\nfull avg10=59.85 avg60=60.52 avg300=61.04 total=1\n"
QUIET = "some avg10=0.00 avg60=0.00 avg300=0.00 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=1\n"


def _stat(file: int = 13705216, shmem: int = 0, slab_reclaimable: int = 9313168) -> str:
    return f"anon 21402742784\nfile {file}\nshmem {shmem}\nslab_reclaimable {slab_reclaimable}\n"


SG1 = {
    f"{UNIT}/memory.high": "21474836480\n",
    f"{UNIT}/memory.max": "25769803776\n",
    f"{UNIT}/memory.current": "21507383296\n",
    f"{UNIT}/memory.stat": _stat(),
    f"{UNIT}/memory.pressure": PRESSURE,
    f"{SLICE}/memory.high": "max\n",
    f"{SLICE}/memory.max": "max\n",
}


def _reader(files: dict[str, str]) -> Callable[[str], Coroutine[None, None, str | None]]:
    async def read(path: str) -> str | None:
        return files.get(path)

    return read


async def test_sg1_s_daemon_cgroup_has_no_room() -> None:
    memory = await cgroup_memory(UNIT, _reader(SG1))

    assert memory is not None
    assert memory.limit_bytes == 20 * GIB
    assert memory.available_bytes == 20 * GIB - (21507383296 - 13705216 - 9313168)
    assert memory.pressure is not None
    assert memory.pressure.full_avg10 == pytest.approx(59.85)


async def test_page_cache_is_room() -> None:
    """A cgroup at `memory.high` that holds page cache is not full: reclaim frees the cache. `shmem` stays."""
    files = {**SG1, f"{UNIT}/memory.stat": _stat(file=19 * GIB, shmem=1 * GIB), f"{UNIT}/memory.pressure": QUIET}

    memory = await cgroup_memory(UNIT, _reader(files))

    assert memory is not None
    assert memory.available_bytes == 20 * GIB - (21507383296 - 18 * GIB - 9313168)


async def test_a_tighter_ancestor_decides() -> None:
    files = {
        **SG1,
        f"{SLICE}/memory.high": f"{16 * GIB}\n",
        f"{SLICE}/memory.current": f"{12 * GIB}\n",
        f"{SLICE}/memory.stat": _stat(file=0, slab_reclaimable=0),
        f"{UNIT}/memory.high": "max\n",
        f"{UNIT}/memory.max": "max\n",
    }

    memory = await cgroup_memory(UNIT, _reader(files))

    assert memory is not None
    assert (memory.limit_bytes, memory.available_bytes) == (16 * GIB, 4 * GIB)


async def test_no_limit_leaves_the_host_s_figures() -> None:
    files = {**SG1, f"{UNIT}/memory.high": "max\n", f"{UNIT}/memory.max": "max\n"}

    assert await cgroup_memory(UNIT, _reader(files)) is None


def _sg1(build_memory: CgroupMemory | None) -> SystemHealth:
    """sg1-n1's host: 57 of 125 GB free, with some host-wide `full` beside the cgroup's."""
    return SystemHealth(
        psi=PsiSnapshot(memory=PsiMetric(full_avg10=1.5)),
        meminfo=MemInfo(mem_total=125 * 1024 * 1024, mem_available=57 * 1024 * 1024),
        cores=48.0,
        build_memory=build_memory,
    )


def _limit() -> BuildLimit:
    return BuildLimit("sg1-n1", 32, PressureSettings(settle=10.0))


def test_builds_that_thrash_in_their_cap_lower_the_limit() -> None:
    limit = _limit()
    limit.observe(_sg1(None), running=0, now=0.0)

    thrashing = CgroupMemory(limit_bytes=20 * GIB, available_bytes=-1 * GIB, pressure=PsiMetric(full_avg10=59.85))
    limit.observe(_sg1(thrashing), running=11, now=10.0)

    assert (limit.limit, limit.reason) == (5, LimitReason.memory_pressure)


def test_a_cap_full_of_cache_lets_the_limit_rise() -> None:
    limit = _limit()
    limit.observe(_sg1(None), running=0, now=0.0)

    cached = CgroupMemory(limit_bytes=20 * GIB, available_bytes=18 * GIB, pressure=PsiMetric())
    limit.observe(_sg1(cached), running=32, now=10.0)

    assert (limit.limit, limit.reason) == (32, LimitReason.slow_start)


def test_a_thrashing_host_lowers_the_limit_of_an_idle_cap() -> None:
    """Pods outside the build cgroup can starve the host; the builds stall all the same."""
    limit = _limit()
    limit.observe(_sg1(None), running=0, now=0.0)

    idle = CgroupMemory(limit_bytes=20 * GIB, available_bytes=18 * GIB, pressure=PsiMetric())
    starved = SystemHealth(
        psi=PsiSnapshot(memory=PsiMetric(full_avg10=30.0)),
        meminfo=MemInfo(mem_total=125 * 1024 * 1024, mem_available=2 * 1024 * 1024),
        cores=48.0,
        build_memory=idle,
    )
    limit.observe(starved, running=8, now=10.0)

    assert (limit.limit, limit.reason) == (4, LimitReason.memory_pressure)


def test_placement_sizes_builds_to_the_cap() -> None:
    """`choose_cores` reads this room: the host's 57 GB would give a build far more cores than the cap holds."""
    capped = CgroupMemory(limit_bytes=20 * GIB, available_bytes=3 * GIB)
    store = SimpleNamespace(monitor=SimpleNamespace(health=_sg1(capped)))

    assert memory_room(store, reserved_bytes=1 * GIB) == 2 * GIB  # type: ignore[arg-type]  # reads the monitor alone


@pytest.mark.parametrize("declared", [True, False], ids=["declared-local", "implicit-local"])
def test_the_module_s_setting_reaches_the_local_store(declared: bool) -> None:
    """sg1's settings as its NixOS module renders them: a top-level `pressure`, and `stores.local` without one."""
    rendered: dict[str, object] = {"unix_path": None, "pressure": {"build_cgroup": str(UNIT)}}
    if declared:
        rendered["stores"] = {"local": {"type": "local-socket", "monitor": False, "gc_target_usage": 0.5}}

    client = PynixdSettings.model_validate(rendered).to_stores()[LOCAL_STORE_ID].daemon_client

    assert client is not None
    assert client.build_memory_sampler() is not None


def test_a_peer_sends_the_build_cgroup() -> None:
    """A reverse builder's own pynixd reads the cgroup; the controller gets it in the health stream."""
    sent = _sg1(CgroupMemory(limit_bytes=20 * GIB, available_bytes=0, pressure=PsiMetric(full_avg10=59.85)))

    got = HEALTH_JSON.validate_json(HEALTH_JSON.dump_json(sent))

    assert got.build_memory == sent.build_memory
    assert got.memory_full == pytest.approx(59.85)


async def test_each_sample_carries_the_build_cgroup() -> None:
    seen: list[SystemHealth] = []
    poller = GenericResourcePoller(PressureSettings(), seen.append, _reader({}))

    async def sample() -> CgroupMemory | None:
        return await cgroup_memory(UNIT, _reader(SG1))

    poller.build_memory_fn = sample
    await poller._publish_sample(SystemHealth())  # noqa: SLF001 -- the sample hook is the unit

    assert [h.build_memory.limit_bytes for h in seen if h.build_memory] == [20 * GIB]
    assert seen[0].memory_full == pytest.approx(59.85)
