"""A background task that dies says so at once, not at shutdown.

The daemon's GC loop, liveness watch and loop-lag monitor run as tasks
that only shutdown awaits. One that raised kept its exception, and the
journal said nothing while the work it did stopped.
"""

from __future__ import annotations

import asyncio

import anyio
import pytest
import structlog.testing

from pynixd.instance import _log_task_death


async def _fails() -> None:
    raise RuntimeError("boom")


async def _waits() -> None:
    await anyio.sleep(3600)


@pytest.mark.anyio
async def test_a_failed_task_is_logged_with_its_name_and_exception() -> None:
    with structlog.testing.capture_logs() as logs:
        task = asyncio.create_task(_fails(), name="the-gc-loop")
        task.add_done_callback(_log_task_death)
        with pytest.raises(RuntimeError):
            await task
        await anyio.sleep(0)
    failed = [entry for entry in logs if entry["event"] == "background_task_failed"]
    assert [entry["task"] for entry in failed] == ["the-gc-loop"]
    assert isinstance(failed[0]["exc_info"], RuntimeError)


@pytest.mark.anyio
async def test_a_cancelled_task_is_not_a_failure() -> None:
    with structlog.testing.capture_logs() as logs:
        task = asyncio.create_task(_waits())
        task.add_done_callback(_log_task_death)
        await anyio.sleep(0)
        task.cancel()
        with pytest.raises(anyio.get_cancelled_exc_class()):
            await task
        await anyio.sleep(0)
    assert not [entry for entry in logs if entry["event"] == "background_task_failed"]
