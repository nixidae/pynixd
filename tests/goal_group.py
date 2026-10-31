"""The task group that `GoalEngine.running` opens, for a fake engine. nixidae/nixidae#1143."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from pynixd.background import Background

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pynixd.goals.goal import Goal


@contextlib.asynccontextmanager
async def goals_running(engine: object) -> AsyncIterator[None]:
    """Run the goals of *engine* for the body of this block, as one request does."""
    group = Background("test engine")
    async with group.running():
        engine.goal_group = group  # type: ignore[attr-defined]  # each fake engine is its own class
        yield


async def result_of[T](goal: Goal[T]) -> T:
    """The result of *goal*, run as the one goal a request asked for."""
    async with goals_running(goal.engine):
        return await goal.result()
