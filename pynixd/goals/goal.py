"""Shared goal lifecycle primitives."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, ClassVar, TypeVar, cast

import anyio

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .engine import GoalEngine


T = TypeVar("T")
U = TypeVar("U")
_MISSING = object()


class Goal[T]:
    """A deduped async computation shared by multiple request waiters."""

    may_reach_a_root_goal: ClassVar[bool] = True
    """True when a wait for this goal can reach a root goal of a request.

    A root goal holds a place in the order of its request, and it gives that
    place up before it waits for a goal that carries this flag. The two would
    otherwise wait for each other: the root goal behind it cannot start, and
    this goal cannot finish until that one does. Issue nixidae/nixidae#246.

    The value is True here, so a new kind of goal is safe before anybody reads
    this file. A goal that reaches no other goal of the request sets it False,
    and `EnsureDerivedPathGoal.run_child` names what that buys.
    """

    def __init__(self, engine: GoalEngine) -> None:
        self.engine = engine
        self._lock = anyio.Lock()
        self._task: asyncio.Task[T] | None = None

    async def start(self) -> None:
        """Begin the goal, and do not wait for the answer.

        `result` waits as well, and a caller that wants the goal to run
        beside it needs the two apart. `EnsureDerivedPathGoal` starts a build
        goal and then waits for the build to reach the queue, which is
        earlier than the end of the build. Issue nixidae/nixidae#246.

        The instance keeps the task, so nothing else must hold a reference to
        it.
        """
        async with self._lock:
            if self._task is None:
                self._task = asyncio.create_task(self._run())

    def has_started(self) -> bool:
        """Did `start` or `result` make the task of this goal?

        A goal serves every request that names its derived path, so a request
        can meet a goal that another one already runs. Such a goal is past
        every gate that a request sets for it. Issue nixidae/nixidae#246.
        """
        return self._task is not None

    async def result(self) -> T:
        """Await the goal's result, starting execution on first call."""
        async with self._lock:
            if self._task is None:
                self._task = asyncio.create_task(self._run())
            task = self._task
        return await task

    def cancel(self) -> bool:
        """Cancel the goal's task. True when a live task was cancelled.

        Sync, so a request teardown runs it even under cancellation. A
        finished goal answers False: nothing watches it, nothing reaps it.
        """
        task = self._task
        if task is None or task.done():
            return False
        task.cancel()
        return True

    async def _run(self) -> T:
        """Execute the goal's core logic. Must be overridden by subclasses."""
        raise NotImplementedError


class GoalHolder(Goal[T]):
    """A coordinator goal that advances through child goals serially."""

    async def run_child(self, child: Goal[U]) -> U:
        """Run a single child goal and return its result."""
        return await child.result()

    async def run_children(self, children: Sequence[Goal[U]]) -> list[U]:
        """Run child goals serially and return their results in order."""
        return [await child.result() for child in children]


class ExecutionGoal(Goal[T]):
    """A fan-out goal that waits for child goals in parallel."""

    async def run_children(self, children: Sequence[Goal[U]]) -> list[U]:
        """Run child goals concurrently and return their results in order."""
        results: list[U | object] = [_MISSING] * len(children)

        async def run_one(index: int, child: Goal[U]) -> None:
            results[index] = await child.result()

        async with anyio.create_task_group() as tg:
            for index, child in enumerate(children):
                tg.start_soon(run_one, index, child)

        collected: list[U] = []
        for result in results:
            if result is _MISSING:
                raise RuntimeError("goal child did not record a result")
            collected.append(cast("U", result))
        return collected
