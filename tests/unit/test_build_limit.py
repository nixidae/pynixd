"""A store's build limit follows the pressure of its host. Issue #98.

The gate this replaces held every pool acquire, reads included, on one
threshold, and failed the operation after 5 s. The limit moves between 1 and
the build pool size, holds inside a band so one sample cannot flap it, and
gates only the start of a build.
"""

from __future__ import annotations

import anyio

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId
from pynixd import state as state_collector
from pynixd.config import PressureSettings, PynixdSettings
from pynixd.context import PynixdContext
from pynixd.monitor import GenericResourcePoller
from pynixd.pressure import BuildLimit, LimitReason
from pynixd.psi import MemInfo, PsiMetric, PsiSnapshot, SystemHealth
from pynixd.scheduler import Scheduler
from tests.functional.mock_store import MockStore
from tests.unit.test_local_build_slots import _options, _request

GIB_KB = 1024 * 1024


def _health(
    cpu_some: float = 0.0,
    memory_full: float = 0.0,
    io_full: float = 0.0,
    available_gib: float = 32.0,
    cores: float = 8.0,
) -> SystemHealth:
    return SystemHealth(
        psi=PsiSnapshot(
            cpu=PsiMetric(some_avg10=cpu_some),
            memory=PsiMetric(full_avg10=memory_full),
            io=PsiMetric(full_avg10=io_full),
        ),
        meminfo=MemInfo(mem_total=64 * GIB_KB, mem_available=int(available_gib * GIB_KB)),
        cores=cores,
    )


def _limit(ceiling: int = 64, **settings: float) -> BuildLimit:
    return BuildLimit("builder", ceiling, PressureSettings(settle=10.0, **settings))  # type: ignore[arg-type]  # floats only


def test_no_signal_leaves_the_ceiling() -> None:
    limit = _limit()
    limit.observe(SystemHealth(), running=0, now=0.0)
    assert (limit.limit, limit.reason) == (64, LimitReason.unmonitored)


def test_disabled_leaves_the_ceiling_under_pressure() -> None:
    limit = BuildLimit("builder", 64, PressureSettings(enabled=False))
    limit.observe(_health(memory_full=50.0), running=10, now=0.0)
    assert limit.limit == 64


def test_slow_start_begins_at_the_core_count_and_doubles_once_per_settle() -> None:
    limit = _limit()
    limit.observe(_health(cores=8.0), running=0, now=0.0)
    assert (limit.limit, limit.reason) == (8, LimitReason.slow_start)

    assert not limit.observe(_health(), running=8, now=5.0), "a rise before the settle time"
    assert limit.observe(_health(), running=8, now=10.0)
    assert limit.limit == 16
    assert not limit.observe(_health(), running=10, now=25.0), "free slots: nothing to grow for"
    assert limit.limit == 16


def test_memory_stall_halves_the_running_builds_and_ends_slow_start() -> None:
    limit = _limit()
    limit.observe(_health(), running=0, now=0.0)
    limit.observe(_health(), running=8, now=10.0)
    limit.observe(_health(memory_full=5.0), running=12, now=11.0)
    assert (limit.limit, limit.reason) == (6, LimitReason.memory_pressure)
    assert not limit.slow_start

    limit.observe(_health(memory_full=5.0), running=12, now=15.0)
    assert limit.limit == 6, "a second fall before avg10 can show the first"
    limit.observe(_health(memory_full=5.0), running=6, now=21.0)
    assert limit.limit == 3


def test_the_band_between_the_marks_holds() -> None:
    """One mark would flap at every sample that crosses it."""
    limit = _limit()
    limit.observe(_health(), running=0, now=0.0)
    for t, memory_full in enumerate((1.0, 1.9, 0.6, 1.5), start=20):
        limit.observe(_health(memory_full=memory_full), running=8, now=float(t * 10))
        assert (limit.limit, limit.reason) == (8, LimitReason.memory_pressure)


def test_cpu_pressure_holds_but_never_falls() -> None:
    limit = _limit()
    limit.observe(_health(), running=0, now=0.0)
    limit.observe(_health(cpu_some=99.0), running=8, now=20.0)
    assert (limit.limit, limit.reason) == (8, LimitReason.cpu_pressure)


def test_low_memory_falls_and_the_floor_is_one() -> None:
    limit = _limit()
    limit.observe(_health(), running=0, now=0.0)
    for t in range(10):
        limit.observe(_health(available_gib=0.5), running=1, now=float(t * 10))
    assert (limit.limit, limit.reason) == (1, LimitReason.low_memory)
    assert limit.has_room(0)


def test_after_a_fall_the_limit_grows_one_at_a_time() -> None:
    limit = _limit()
    limit.observe(_health(), running=0, now=0.0)
    limit.observe(_health(io_full=40.0), running=8, now=1.0)
    assert limit.limit == 4
    limit.observe(_health(), running=4, now=11.0)
    limit.observe(_health(), running=5, now=21.0)
    assert (limit.limit, limit.reason) == (6, LimitReason.steady)


def test_load_holds_a_builder_without_psi() -> None:
    limit = _limit()
    health = SystemHealth(load1=24.0, cores=8.0)
    limit.observe(health, running=0, now=0.0)
    limit.observe(health, running=8, now=20.0)
    assert (limit.limit, limit.reason) == (8, LimitReason.load)


async def test_the_poller_reads_a_host_without_psi_or_cgroups() -> None:
    files = {"/proc/meminfo": "MemTotal: 1000 kB\nMemAvailable: 400 kB\n", "/proc/loadavg": "3.5 2 1 1/2 3\n"}

    async def read(path: str) -> str | None:
        return files.get(path)

    seen: list[SystemHealth] = []
    poller = GenericResourcePoller(PressureSettings(), seen.append, read)
    health = await poller.sample()
    assert health.psi is None
    assert health.meminfo is not None and health.meminfo.mem_available == 400
    assert health.load1 == 3.5
    assert health.has_signal


async def test_a_store_at_its_limit_holds_the_build_and_says_why() -> None:
    store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(settings=PynixdSettings(), _stores={LOCAL_STORE_ID: store})
    ctx.scheduler = scheduler = Scheduler(ctx)
    store.build_limit.limit = 0
    await scheduler.queue.enqueue(_request("1"), from_goal_path=True, goal_request_id=RequestId(7), options=_options(4))

    await scheduler.schedule()

    builds = state_collector.collect(ctx, ["builds"]).builds
    assert builds is not None
    assert [(row.state, row.waits_for) for row in builds] == [("pending", "pressure")]


async def test_reads_never_wait_for_the_build_limit() -> None:
    store = MockStore("lanes-limit", feature_matrix={"x86_64-linux": set()})
    store.build_limit.limit = 0
    with anyio.fail_after(1):
        async with store.transfer_conn():
            pass


async def test_a_rising_limit_runs_a_scheduling_pass() -> None:
    store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(settings=PynixdSettings(), _stores={LOCAL_STORE_ID: store})
    scheduler = Scheduler(ctx)
    store.build_limit = BuildLimit("local", 64, PressureSettings(initial_limit=1, settle=0.0))
    store.on_pressure_sample(_health())
    assert store.build_limit.limit == 1
    scheduler.trigger_event = anyio.Event()

    store.build_pool.active_connections = 1
    store.on_pressure_sample(_health())

    assert store.build_limit.limit == 2
    assert scheduler.trigger_event.is_set()
