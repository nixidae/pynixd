"""Path locks wanted by more tasks than anyio has worker threads.

sg1's central deadlocked when 40 imports of one path each waited in `flock` on a worker thread, and the
import that held the lock needed a thread to finish.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
import pytest

from pynixd_store import add
from pynixd_store.add import PathLock, release_locks

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

THREADS = 2


@pytest.fixture
async def few_threads() -> AsyncIterator[None]:
    limiter = anyio.to_thread.current_default_thread_limiter()
    before = limiter.total_tokens
    limiter.total_tokens = THREADS
    yield
    limiter.total_tokens = before


@pytest.mark.usefixtures("few_threads")
async def test_waiters_past_the_thread_limit_leave_the_holder_a_thread(tmp_path: Path) -> None:
    path = tmp_path / "x-reapi-directory"
    order: list[int] = []

    async def wait_then_hold(n: int) -> None:
        lock = PathLock(path)
        await lock.acquire_async()
        order.append(n)
        await anyio.to_thread.run_sync(lambda: None)
        await release_locks([lock], delete=False)

    holder = PathLock(path)
    await holder.acquire_async()
    deadlocked = False

    async def watchdog() -> None:
        # A thread in `flock` takes no cancellation: only more threads end the deadlock.
        nonlocal deadlocked
        await anyio.sleep(5)
        deadlocked = True
        anyio.to_thread.current_default_thread_limiter().total_tokens = THREADS + 8

    async with anyio.create_task_group() as outer:
        outer.start_soon(watchdog)
        async with anyio.create_task_group() as tg:
            for n in range(THREADS + 2):
                tg.start_soon(wait_then_hold, n)
            await anyio.wait_all_tasks_blocked()
            await anyio.to_thread.run_sync(lambda: None)
            await release_locks([holder], delete=False)
        outer.cancel_scope.cancel()
    assert not deadlocked, "the holder found no worker thread"
    assert sorted(order) == list(range(THREADS + 2))
    assert add._GATES == {}


async def test_a_waiter_cancelled_in_the_queue_leaves_no_gate(tmp_path: Path) -> None:
    path = tmp_path / "y"
    holder = PathLock(path)
    await holder.acquire_async()

    async def give_up() -> None:
        with anyio.move_on_after(0.05):
            await PathLock(path).acquire_async()

    async with anyio.create_task_group() as tg:
        tg.start_soon(give_up)
    await release_locks([holder], delete=False)
    assert add._GATES == {}
    again = PathLock(path)
    with anyio.fail_after(5):
        await again.acquire_async()
    await release_locks([again], delete=True)
    assert add._GATES == {}
