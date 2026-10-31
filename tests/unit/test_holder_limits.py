"""A holder of a shared build enforces its own timeouts. Issue nixidae/nixidae#231.

The upstream build runs with the options of the client that made it, as the
first of two Nix clients builds while the second waits on the output lock.
A later client that set a tighter `max-silent-time` or `timeout` waited on
the owner's build without end -- the shape of the hang in #89.
"""

from __future__ import annotations

import contextlib
import time
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import anyio

from nix_daemon_protocol.ids import RequestId
from pynixd.build_queue import BuildQueue, QueuedBuild
from pynixd.connection import ClientConn
from pynixd.goals.build_derivation import BuildDerivationGoal, limit_reached, limits_the_owner_leaves
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    SetOptionsRequest,
    StorePath,
    TimeSpan,
    Verbosity,
)
from pynixd.wire import BytesWriter
from tests.goal_group import goals_running, result_of
from tests.unit.test_build_derivation_goal import FakeEngine

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine


def _options(*, silent: int = 0, timeout: int | None = None) -> SetOptionsRequest:
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=False,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=1,
        max_silent_time=TimeSpan(silent),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=1,
        use_substitutes=True,
        overrides={} if timeout is None else {"timeout": str(timeout)},
    )


def test_a_holder_enforces_only_what_the_owner_leaves() -> None:
    assert limits_the_owner_leaves(_options(silent=600), _options()) == (600, 0)
    assert limits_the_owner_leaves(_options(silent=600), _options(silent=60)) == (0, 0)
    assert limits_the_owner_leaves(_options(silent=60), _options(silent=600)) == (60, 0)
    assert limits_the_owner_leaves(_options(timeout=30), _options(timeout=0)) == (0, 30)
    assert limits_the_owner_leaves(_options(silent=0), _options(silent=600)) == (0, 0)
    assert limits_the_owner_leaves(None, _options()) == (0, 0)


def test_the_owner_enforces_nothing_itself() -> None:
    """The upstream daemon already applies the owner's limits."""
    owner = _options(silent=5, timeout=5)
    assert limits_the_owner_leaves(owner, owner) == (0, 0)


def _build(*, started: float | None, output: float | None) -> QueuedBuild:
    return cast("QueuedBuild", SimpleNamespace(started_at=started, last_output_at=output))


def test_queue_time_counts_against_no_limit() -> None:
    passed, wait = limit_reached(_build(started=None, output=None), 1, 1, now=1000.0)
    assert passed is None
    assert wait > 0


def test_silence_counts_from_the_last_output() -> None:
    build = _build(started=100.0, output=150.0)
    assert limit_reached(build, 60, 0, now=200.0) == (None, 10.0)
    assert limit_reached(build, 60, 0, now=210.0) == (60, 0.0)


def test_timeout_counts_from_the_start_whatever_the_output() -> None:
    build = _build(started=100.0, output=195.0)
    assert limit_reached(build, 0, 100, now=200.0) == (100, 0.0)


class _QueueScheduler:
    """`build_derivation` onto a real queue, with nothing that schedules."""

    def __init__(self) -> None:
        self.queue = BuildQueue()

    async def build_derivation(
        self,
        request: BuildDerivationRequest,
        *,
        from_goal_path: bool,
        goal_request_id: RequestId,
        options: SetOptionsRequest | None,
        delegated: bool = False,
    ) -> object:
        return await self.queue.enqueue(
            request,
            from_goal_path=from_goal_path,
            goal_request_id=goal_request_id,
            options=options,
            delegated=delegated,
        )


async def test_a_later_holder_with_a_tighter_silence_limit_times_out_alone() -> None:
    scheduler = _QueueScheduler()
    request = BuildDerivationRequest(
        drv_path=StorePath(path="/nix/store/00000000000000000000000000000001-silent.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )
    build_id, shared = await scheduler.queue.enqueue(
        request, from_goal_path=True, goal_request_id=RequestId(99), options=_options()
    )
    build = scheduler.queue.by_id[build_id]
    build.started_at = build.last_output_at = time.monotonic() - 10

    client = ClientConn(BytesWriter("client"))
    client.options = _options(silent=2)
    goal = BuildDerivationGoal(cast("GoalEngine", FakeEngine(cast("object", scheduler))), request)  # type: ignore[arg-type]  # a fake scheduler
    await goal.subscribe(client)

    result = await result_of(goal)

    assert result.result.status == BuildResultStatus.TIMED_OUT
    assert result.result.error_msg == "timed out after 2 seconds"
    # The owner still holds the build, and it runs on.
    assert not shared.done()


async def test_a_holder_that_goes_away_leaves_the_build_to_the_others() -> None:
    """A reaped goal must not cancel the future that every holder awaits. nixidae/nixidae#15."""
    scheduler = _QueueScheduler()
    request = BuildDerivationRequest(
        drv_path=StorePath(path="/nix/store/00000000000000000000000000000001-shared.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )
    async with contextlib.AsyncExitStack() as requests:
        goals: list[BuildDerivationGoal] = []
        for request_id in (1, 2):
            engine = FakeEngine(cast("object", scheduler))  # type: ignore[arg-type]  # a fake scheduler
            engine.request_id = RequestId(request_id)
            await requests.enter_async_context(goals_running(engine))
            goal = BuildDerivationGoal(cast("GoalEngine", engine), request)
            await goal.start()
            await goal.wait_until_it_reached_the_queue()
            goals.append(goal)
        hung_up, waiting = goals
        shared = next(iter(scheduler.queue.by_id.values())).answer

        hung_up.cancel()
        await anyio.sleep(0)
        await anyio.sleep(0)

        assert not shared.done()
        shared.set(
            BuildDerivationResponse(
                result=BuildResult(
                    status=BuildResultStatus.BUILT,
                    error_msg="",
                    times_built=1,
                    is_non_deterministic=0,
                    start_time=0,
                    stop_time=0,
                    built_outputs={},
                ),
            )
        )
        result = await waiting.result()
        assert result.result.status == BuildResultStatus.BUILT
