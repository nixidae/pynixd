"""`asyncio.gather` in a task group: a failure cancels the rest, and nothing outlives the call."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

if TYPE_CHECKING:
    from collections.abc import Awaitable


async def all_of[T](*awaitables: Awaitable[T]) -> list[T]:
    """Await every one at once, and answer their results in order."""
    results: dict[int, T] = {}

    async def one(index: int, awaitable: Awaitable[T]) -> None:
        results[index] = await awaitable

    async with anyio.create_task_group() as tg:
        for index, awaitable in enumerate(awaitables):
            tg.start_soon(one, index, awaitable)
    return [results[index] for index in range(len(awaitables))]
