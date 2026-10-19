"""The order in which the root goals of one request enqueue a build.

**This module stands for `build.sh:167` and `build.sh:176`.**
`nix build -f fod-failing.nix -j1 -L` builds four fixed-output derivations,
and every one of them gives the wrong hash. `build.sh:167` asserts one
`error:` line, and `build.sh:176` asserts that the line names x1. Both hold
only when the one slot of `-j1` goes to the goal that sorts first by
derivation name. `dispatch_order.py` gives the mechanism and the measurement.
Issue nixidae/nixidae#246, and issue nixidae/nixidae#235 holds the assertion.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

import anyio
import pytest

from pynixd.derived_path import DerivedPath
from pynixd.goals.dispatch_order import DispatchOrder
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.goal import Goal
from pynixd.goals.results import GoalResult
from pynixd.serde import BuildMode

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    """Every test of this module runs on asyncio, which the goals use."""
    return "asyncio"


async def test_the_first_goal_waits_for_nobody() -> None:
    """The goal at the front of the order enqueues as soon as it is ready."""
    order = DispatchOrder(3)
    with anyio.fail_after(1):
        await order.turn(0).wait()


async def test_a_goal_waits_for_every_goal_before_it() -> None:
    """The second goal moves only after the first one decided."""
    order = DispatchOrder(2)
    arrived = anyio.Event()
    passed = anyio.Event()

    async def second() -> None:
        arrived.set()
        await order.turn(1).wait()
        passed.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(second)
        await arrived.wait()
        await anyio.sleep(0)
        assert not passed.is_set()
        order.turn(0).decided()

    assert passed.is_set()


async def test_a_decision_repeats_without_harm() -> None:
    """`decided` runs on every road out of a goal, so it must repeat."""
    order = DispatchOrder(2)
    order.turn(0).decided()
    order.turn(0).decided()
    with anyio.fail_after(1):
        await order.turn(1).wait()


async def test_the_release_frees_a_goal_that_nobody_decided_for() -> None:
    """A request that stops early must leave no goal waiting."""
    order = DispatchOrder(3)
    order.release_every_goal()
    with anyio.fail_after(1):
        await order.turn(2).wait()


def test_the_turns_follow_the_order_of_the_key() -> None:
    """`turns_in_the_order_of` reads the goals by the index of the client."""
    # The client wrote the goals as x2, x1, x3, and the order of the key is
    # x1, x2, x3, so the goal at index 1 is the first to enqueue.
    turns = DispatchOrder(3).turns_in_the_order_of([1, 0, 2])
    assert [turn.position for turn in turns] == [1, 0, 2]


def test_an_order_that_names_no_place_for_a_goal_is_an_error() -> None:
    """A short order would leave a goal with no turn, and no way to enqueue."""
    with pytest.raises(ValueError, match="does not name every goal"):
        DispatchOrder(3).turns_in_the_order_of([0, 1])


def test_an_order_that_names_one_goal_twice_is_an_error() -> None:
    """Two turns for one goal would leave another goal with none."""
    with pytest.raises(ValueError, match="does not name every goal once"):
        DispatchOrder(3).turns_in_the_order_of([0, 1, 1])


def test_a_request_with_no_goal_has_an_order_and_releases_it() -> None:
    """A client may name no derived path, and that must reach no index error."""
    order = DispatchOrder(0)
    assert order.turns_in_the_order_of([]) == []
    order.release_every_goal()


class _Child(Goal[str]):
    """A child goal that answers at once."""

    async def _run(self) -> str:
        return "done"


class _ChildThatReachesARootGoal(_Child):
    may_reach_a_root_goal: ClassVar[bool] = True


class _ChildThatReachesNoRootGoal(_Child):
    may_reach_a_root_goal: ClassVar[bool] = False


class _NeverEndingEnsureGoal(EnsureDerivedPathGoal):
    """A goal that starts and does not finish, so `has_started` is True."""

    async def _run(self) -> GoalResult:
        await anyio.sleep_forever()
        raise AssertionError("sleep_forever does not return")

    def stop_for_the_test(self) -> None:
        """End the task, so the loop does not close with it still there."""
        task = self._task
        if task is not None:
            task.cancel()


def _ensure_goal[G: EnsureDerivedPathGoal = EnsureDerivedPathGoal](cls: type[G] = EnsureDerivedPathGoal) -> G:
    """A root goal with no engine behind it, for the turn alone."""
    return cls(
        engine=cast("GoalEngine", cast("Any", object())),
        derived_path=DerivedPath("/nix/store/" + "0" * 32 + "-x1.drv!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )


async def test_a_wait_for_a_root_goal_gives_up_the_place() -> None:
    """The goal that waits cannot enqueue now, so it must not hold the line."""
    order = DispatchOrder(2)
    goal = _ensure_goal()
    goal.take_a_turn(order.turn(0))

    await goal.run_child(_ChildThatReachesARootGoal(goal.engine))

    with anyio.fail_after(1):
        await order.turn(1).wait()


async def test_a_wait_for_a_build_keeps_the_place() -> None:
    """A build goal reaches no root goal, so the order stays visible."""
    order = DispatchOrder(2)
    goal = _ensure_goal()
    goal.take_a_turn(order.turn(0))

    await goal.run_child(_ChildThatReachesNoRootGoal(goal.engine))

    with pytest.raises(TimeoutError):
        with anyio.fail_after(0.05):
            await order.turn(1).wait()


async def test_a_second_request_does_not_take_the_goal_from_the_first() -> None:
    """One goal serves two requests, and the second turn must not hold anybody.

    The engine gives one goal to every request that names the same derived
    path. That goal keeps the order of the request that reached it first, and
    the order of the second request must move without it. Issue nixidae/nixidae#246.
    """
    first = DispatchOrder(2)
    second = DispatchOrder(2)
    goal = _ensure_goal()

    goal.take_a_turn(first.turn(1))
    goal.take_a_turn(second.turn(0))

    # The turn of the second request is decided at once, so the goal behind it
    # enqueues although this goal still waits for the first request.
    with anyio.fail_after(1):
        await second.turn(1).wait()

    with pytest.raises(TimeoutError), anyio.fail_after(0.05):
        await goal._wait_for_my_turn()  # noqa: SLF001 -- the gate under test is private


async def test_two_root_goals_that_depend_on_each_other_do_not_deadlock() -> None:
    """The early goal of the order waits for the late one, and both finish.

    A request names the derivations in the order of the key, and that order
    does not follow the dependencies. `a` sorts before `z` and can still
    depend on `z`. `a` holds the first place; `z` waits for `a` to decide;
    and `a` gives its place up before it waits for `z`. Issue nixidae/nixidae#246.
    """
    order = DispatchOrder(2)
    early = _ensure_goal()
    late = _ensure_goal()
    early.take_a_turn(order.turn(0))
    late.take_a_turn(order.turn(1))

    reached_the_gate: list[str] = []

    class _Late(Goal[str]):
        async def _run(self) -> str:
            # The late goal reaches its own gate, which waits for the early
            # goal, and only then answers the early goal.
            await late._wait_for_my_turn()  # noqa: SLF001 -- the gate under test is private
            reached_the_gate.append("z")
            return "z"

    with anyio.fail_after(2):
        answer = await early.run_child(_Late(early.engine))
        await early._wait_for_my_turn()  # noqa: SLF001 -- the gate under test is private

    assert answer == "z"
    assert reached_the_gate == ["z"]


async def test_a_goal_that_already_started_takes_no_turn() -> None:
    """Such a goal is past its gate, and the turn would never be decided.

    The engine gives one goal to every request that names the same derived
    path, so a request can meet a goal that another one runs. That goal took
    no turn of this request, and it never reads one, so a turn it held would
    hold every goal behind it for ever. Issue nixidae/nixidae#246.
    """
    order = DispatchOrder(2)
    goal = _ensure_goal(_NeverEndingEnsureGoal)
    await goal.start()
    try:
        goal.take_a_turn(order.turn(0))

        # The goal behind it enqueues, and the started goal reads no turn.
        with anyio.fail_after(1):
            await order.turn(1).wait()
        with anyio.fail_after(1):
            await goal._wait_for_my_turn()  # noqa: SLF001 -- the gate under test is private
    finally:
        goal.stop_for_the_test()
