"""Unit tests for shared goal lifecycle helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from pynixd.goals.engine import GoalEngine
from pynixd.goals.goal import ExecutionGoal, Goal
from pynixd.goals.requests import _the_result_unless_it_stops

if TYPE_CHECKING:
    from pynixd.context import PynixdContext
    from pynixd.goals.ensure import EnsureDerivedPathGoal


@dataclass
class StaticGoal[T](Goal[T]):
    engine: GoalEngine
    value: T

    def __post_init__(self) -> None:
        Goal.__init__(self, self.engine)

    async def _run(self) -> T:
        return self.value


@dataclass
class ParentGoal(ExecutionGoal[list[str | None]]):
    engine: GoalEngine
    children: list[Goal[str | None]]

    def __post_init__(self) -> None:
        ExecutionGoal.__init__(self, self.engine)

    async def _run(self) -> list[str | None]:
        return await self.run_children(self.children)


def _engine() -> GoalEngine:
    """A real engine. Nothing touches the context."""
    return GoalEngine(cast("PynixdContext", None))


@pytest.mark.anyio
async def test_execution_goal_preserves_none_child_results() -> None:
    engine = _engine()
    async with engine.running():
        parent = ParentGoal(
            engine=engine,
            children=[
                StaticGoal[str | None](engine, None),
                StaticGoal[str | None](engine, "done"),
            ],
        )

        assert await parent.result() == [None, "done"]


@dataclass
class HangingGoal(Goal[None]):
    engine: GoalEngine

    def __post_init__(self) -> None:
        Goal.__init__(self, self.engine)

    async def _run(self) -> None:
        await anyio.sleep_forever()


@dataclass
class FailingGoal(Goal[None]):
    engine: GoalEngine

    def __post_init__(self) -> None:
        Goal.__init__(self, self.engine)

    async def _run(self) -> None:
        raise ValueError("failed")


def _hold(engine: GoalEngine, *goals: Goal[Any]) -> None:
    for index, goal in enumerate(goals):
        engine._goals[index] = goal


@pytest.mark.anyio
async def test_reap_cancels_an_unfinished_goal() -> None:
    engine = _engine()
    async with engine.running():
        goal = HangingGoal(engine)
        _hold(engine, goal)
        await goal.start()
        assert engine.reap() == 1
        with anyio.fail_after(5), pytest.raises(RuntimeError, match="cancelled"):
            await goal.result()


@pytest.mark.anyio
async def test_reap_leaves_a_finished_goal_alone() -> None:
    engine = _engine()
    async with engine.running():
        goal = StaticGoal(engine, "done")
        _hold(engine, goal)
        assert await goal.result() == "done"
        assert engine.reap() == 0
        assert await goal.result() == "done"


@pytest.mark.anyio
async def test_reap_counts_only_the_unfinished() -> None:
    engine = _engine()
    async with engine.running():
        finished = StaticGoal(engine, "done")
        _hold(engine, HangingGoal(engine), finished, HangingGoal(engine))
        assert await finished.result() == "done"
        for goal in engine._goals.values():
            await goal.start()
        assert engine.reap() == 2


@pytest.mark.anyio
async def test_a_failed_goal_leaves_its_siblings_and_the_request_ends() -> None:
    """The error reaches the waiter alone, and the end of the request cancels what still runs."""
    engine = _engine()
    hanging = HangingGoal(engine)
    with anyio.fail_after(5):
        async with engine.running():
            await hanging.start()
            with pytest.raises(ValueError, match="failed"):
                await FailingGoal(engine).result()
            await anyio.sleep(0)
            assert not hanging._answer.done()
    with pytest.raises(RuntimeError, match="cancelled"):
        hanging._answer.result()


@pytest.mark.anyio
async def test_a_stop_ends_the_wait_and_the_goal_runs_on() -> None:
    """nixidae/nixidae#235: the request stops waiting, and the goal keeps its work for the others."""
    engine = _engine()
    async with engine.running():
        goal = HangingGoal(engine)
        stop = anyio.Event()

        async def stop_soon() -> None:
            await anyio.sleep(0.01)
            stop.set()

        with anyio.fail_after(5):
            async with anyio.create_task_group() as tg:
                tg.start_soon(stop_soon)
                assert await _the_result_unless_it_stops(cast("EnsureDerivedPathGoal", goal), stop) is None
        assert goal.has_started()
        assert not goal._answer.done()
