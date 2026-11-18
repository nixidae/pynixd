"""The waiting builds a provisioner launches for. nixidae/nixidae#1198."""

from __future__ import annotations

import functools

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId, StoreId
from pynixd import state as state_collector
from pynixd.build_queue import QueuedBuild
from pynixd.config import PynixdSettings
from pynixd.context import PynixdContext
from pynixd.provisioning.decide import Pool, Want
from pynixd.provisioning.demand import launchable, wants
from pynixd.scheduler import Scheduler
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode, BuildResultStatus, StorePath
from tests.functional.mock_store import MockStore
from tests.unit.test_local_build_slots import _options


def _scheduler() -> tuple[Scheduler, MockStore]:
    store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(settings=PynixdSettings(), _stores={LOCAL_STORE_ID: store})
    ctx.scheduler = scheduler = Scheduler(ctx)
    return scheduler, store


async def _enqueue(scheduler: Scheduler, name: str, platform: str, **env: str) -> QueuedBuild:
    request = BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/0000000000000000000000000000000{name}-{name}.drv"),
        derivation=BasicDerivation(platform=platform, builder="", env=env),
        build_mode=BuildMode.NORMAL,
    )
    build_id, _ = await scheduler.queue.enqueue(
        request, from_goal_path=True, goal_request_id=RequestId(7), options=_options(4)
    )
    return scheduler.queue.by_id[build_id]


def _waiting(scheduler: Scheduler) -> list[Want]:
    held = scheduler.no_candidate | scheduler.held_by_pressure
    return wants(build for build in scheduler.queue.queue if build.build_id in held)


async def test_a_build_for_a_platform_no_store_has_waits_for_a_store() -> None:
    scheduler, _ = _scheduler()
    scheduler.add_dynamic_feature("aarch64-linux", "big")
    await _enqueue(scheduler, "1", "aarch64-linux", requiredSystemFeatures="big")

    await scheduler.schedule()

    builds = state_collector.collect(scheduler.ctx, ["builds"]).builds
    assert builds is not None
    assert [(row.state, row.waits_for) for row in builds] == [("pending", "no_store")]
    assert _waiting(scheduler) == [Want("aarch64-linux", frozenset({"big"}))]


async def test_a_build_at_the_build_limit_counts_too() -> None:
    scheduler, store = _scheduler()
    store.build_limit.limit = 0
    await _enqueue(scheduler, "1", "x86_64-linux")

    await scheduler.schedule()

    assert scheduler.no_candidate == set()
    assert _waiting(scheduler) == [Want("x86_64-linux")]


async def test_a_build_a_new_machine_would_not_take_counts_nowhere() -> None:
    scheduler, _ = _scheduler()
    scheduler.add_dynamic_feature("aarch64-linux")
    await _enqueue(scheduler, "1", "aarch64-linux")
    named = await _enqueue(scheduler, "2", "aarch64-linux")
    named.builders = frozenset({StoreId("named")})
    await _enqueue(scheduler, "3", "aarch64-linux", preferLocalBuild="1")

    await scheduler.schedule()

    assert _waiting(scheduler) == [Want("aarch64-linux")]


SPOT = Pool("spot", frozenset({"aarch64-linux"}), features=frozenset({"aws"}), mandatory_features=frozenset({"aws"}))


async def test_a_build_that_names_a_pool_waits_for_it() -> None:
    """No store takes it yet, and it lacks the pool's mandatory feature: it waits, and counts as demand for
    the pool it named."""
    scheduler, _ = _scheduler()
    scheduler.launchable = functools.partial(launchable, (SPOT,))
    steered = await _enqueue(scheduler, "1", "aarch64-linux")
    steered.builders = frozenset()
    steered.pools = frozenset({"spot"})

    await scheduler.schedule()

    assert steered.build_id in scheduler.no_candidate
    assert not steered.answer.done()
    assert _waiting(scheduler) == [Want("aarch64-linux", pools=frozenset({"spot"}))]


async def test_a_build_that_launches_nothing_still_waits_for_a_pool() -> None:
    """`preferLocalBuild` with no local platform: no reason to launch, but a machine another build launched takes
    it, so it waits instead of failing."""
    scheduler, _ = _scheduler()
    scheduler.launchable = functools.partial(launchable, (Pool("arm", frozenset({"aarch64-linux"})),))
    build = await _enqueue(scheduler, "1", "aarch64-linux", preferLocalBuild="1")

    await scheduler.schedule()

    assert not build.answer.done()
    assert _waiting(scheduler) == []


async def test_a_build_no_pool_takes_fails_as_nix_fails_it() -> None:
    """The control: unsteered, the pool's mandatory feature refuses the build, and no store takes it
    (`derivation-building-goal.cc:289`)."""
    scheduler, _ = _scheduler()
    scheduler.launchable = functools.partial(launchable, (SPOT,))
    build = await _enqueue(scheduler, "1", "aarch64-linux")

    await scheduler.schedule()

    assert build.answer.done()
    result = build.answer.result().result
    assert result.status == BuildResultStatus.INPUT_REJECTED
    assert result.error_msg.startswith("Cannot build"), result.error_msg
    assert _waiting(scheduler) == []
