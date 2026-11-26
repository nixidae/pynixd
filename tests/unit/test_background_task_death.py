"""A background loop that dies says so at once.

The daemon's GC loop, liveness watch and loop-lag monitor run in the
server's background task group: one that raises is logged when it happens
and leaves `async with Server(...)`. A store's loops run in that store's
group: one that raises closes that store, and the server serves on.
nixidae/nixidae#1143.
"""

from __future__ import annotations

from typing import Self

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


class _RefusesStart(MockStore):
    async def __aenter__(self) -> Self:
        raise RuntimeError("managed nix daemon exited")


@pytest.mark.anyio
async def test_a_store_that_fails_to_start_leaves_the_stores() -> None:
    """Left in the stores, it was priced and sent builds that failed on its unopened task group. nixidae/nixidae#1572."""
    broken = _RefusesStart("broken", feature_matrix={"x86_64-linux": {"pod"}})
    server = Server(
        stores={
            StoreId("local"): MockStore("local", feature_matrix={"x86_64-linux": set()}),
            StoreId("broken"): broken,
        },
        ssh_port=None,
        unix_path=None,
        stall_traceback_seconds=0,
    )
    async with server:
        await server._stores_opened()
        assert StoreId("broken") not in server.ctx.stores
        assert server.scheduler is not None
        assert StoreId("broken") not in server.scheduler.stores
        failure = server.scheduler.placer.start_failures()[StoreId("broken")]
        assert "managed nix daemon exited" in failure.error


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


@pytest.mark.anyio
async def test_a_failed_store_loop_closes_that_store_alone() -> None:
    builder = MockStore("builder", feature_matrix={"x86_64-linux": set()})
    with structlog.testing.capture_logs() as logs:
        async with _server() as server:
            assert await server.add_store(builder)
            host = server._store_hosts[StoreId("builder")]
            builder._background.spawn(_fails)
            with anyio.fail_after(5):
                await host.done.wait()
            assert StoreId("builder") not in server.ctx.stores
            assert StoreId("local") in server.ctx.stores
            server._spawn(_waits)
    assert [entry["store_id"] for entry in logs if entry["event"] == "store_background_failed"] == ["builder"]


@pytest.mark.anyio
async def test_a_local_store_with_its_own_id_opens_once() -> None:
    """The key is "local", the store's own id need not be. Registering it under its id opened it twice."""
    local = MockStore("b-local", feature_matrix={"x86_64-linux": set()})
    server = Server(stores={StoreId("local"): local}, ssh_port=None, unix_path=None, stall_traceback_seconds=0)
    with structlog.testing.capture_logs() as logs:
        async with server:
            assert StoreId("b-local") not in server.ctx.stores
    assert not [entry for entry in logs if entry["event"] == "store_start_failed"]


@pytest.mark.anyio
async def test_remove_store_closes_it() -> None:
    builder = MockStore("builder", feature_matrix={"x86_64-linux": set()})
    async with _server() as server:
        assert await server.add_store(builder)
        builder._background.spawn(_waits)
        await server.remove_store(StoreId("builder"), drain_timeout=0)
        assert StoreId("builder") not in server.ctx.stores
        assert StoreId("builder") not in server._store_hosts
        with pytest.raises(RuntimeError, match="async with"):
            builder._background.spawn(_waits)


@pytest.mark.anyio
@pytest.mark.parametrize(("missing_ok", "warnings"), [(False, 1), (True, 0)])
async def test_a_second_removal_warns_unless_it_may_race(missing_ok: bool, warnings: int) -> None:
    """The provisioner removes a builder's store, which closes its connection; the acceptor then
    removes it again. On dynhetz every idle end logged `remove_store_not_found`."""
    builder = MockStore("builder", feature_matrix={"x86_64-linux": set()})
    with structlog.testing.capture_logs() as logs:
        async with _server() as server:
            assert await server.add_store(builder)
            await server.remove_store(StoreId("builder"), drain_timeout=0)
            await server.remove_store(StoreId("builder"), drain_timeout=0, missing_ok=missing_ok)
    assert len([entry for entry in logs if entry["event"] == "remove_store_not_found"]) == warnings


@pytest.mark.anyio
async def test_a_reconnect_runs_in_the_store_and_stops_with_it() -> None:
    store = MockStore("flaky", feature_matrix={"x86_64-linux": set()})
    store.reconnect_min_delay = 0.0
    reconnected = anyio.Event()

    async def on_reconnect() -> None:
        reconnected.set()

    store._on_reconnect = on_reconnect
    async with store:
        assert store._reconnect_scope is not None
        store.record_failure()
        with anyio.fail_after(5):
            await reconnected.wait()
    assert store._reconnect_scope is None
    store.record_failure()  # a closed store schedules nothing
