"""A background loop that dies says so at once, and takes the server with it.

The daemon's GC loop, liveness watch and loop-lag monitor run in the
server's background task group. One that raised used to keep its
exception until shutdown while the work it did stopped. Now the error
is logged when it happens and leaves `async with Server(...)`.
"""

from __future__ import annotations

import anyio
import pytest
import structlog.testing

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from tests.functional.mock_store import MockStore


async def _fails() -> None:
    raise RuntimeError("boom")


async def _waits() -> None:
    await anyio.sleep(3600)


def _server() -> Server:
    return Server(
        stores={StoreId("local"): MockStore("local", feature_matrix={"x86_64-linux": set()})},
        ssh_port=None,
        unix_path=None,
        stall_traceback_seconds=0,
    )


@pytest.mark.anyio
async def test_a_failed_loop_is_logged_and_stops_the_server() -> None:
    reached_the_end = False
    with structlog.testing.capture_logs() as logs, pytest.raises(ExceptionGroup) as raised:
        async with _server() as server:
            server._spawn(_fails)
            await anyio.sleep(3600)
            reached_the_end = True
    assert raised.group_contains(RuntimeError, match="boom", depth=None)
    assert not reached_the_end
    failed = [entry for entry in logs if entry["event"] == "background_task_failed"]
    assert len(failed) == 1
    assert "_fails" in failed[0]["task"]


@pytest.mark.anyio
async def test_a_loop_cancelled_at_close_is_not_a_failure() -> None:
    with structlog.testing.capture_logs() as logs:
        async with _server() as server:
            server._spawn(_waits)
            await anyio.sleep(0)
    assert not [entry for entry in logs if entry["event"] == "background_task_failed"]


@pytest.mark.anyio
async def test_spawn_outside_async_with_refuses() -> None:
    with pytest.raises(RuntimeError, match="async with"):
        _server()._spawn(_waits)
