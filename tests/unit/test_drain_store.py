"""A drained store is waited on until its last connection goes back, or until the timeout."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.config import PynixdSettings
from pynixd.context import PynixdContext
from pynixd.scheduler import Scheduler
from tests.functional.mock_store import MockStore

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from anyio.abc import TaskStatus

    from pynixd.store import Store


def _scheduler(store: MockStore) -> Scheduler:
    local = MockStore(LOCAL_STORE_ID, feature_matrix={"x86_64-linux": set()})
    stores: dict[StoreId, Store] = {LOCAL_STORE_ID: local, StoreId(store.store_id): store}
    return Scheduler(PynixdContext(settings=PynixdSettings(), _stores=stores))


def _timeouts(logs: Sequence[Mapping[str, object]]) -> list[Mapping[str, object]]:
    return [entry for entry in logs if entry["event"] == "drain_timeout_reached"]


async def test_an_idle_store_drains_at_once_even_with_no_timeout() -> None:
    store = MockStore("drain-idle", feature_matrix={"x86_64-linux": set()})
    with structlog.testing.capture_logs() as logs:
        await _scheduler(store).drain_store(store.store_id, drain_timeout=0)
    assert _timeouts(logs) == []


async def test_a_drain_ends_when_the_last_connection_goes_back() -> None:
    store = MockStore("drain-busy", feature_matrix={"x86_64-linux": set()})
    scheduler = _scheduler(store)
    held = anyio.Event()
    release = anyio.Event()
    drained_at: list[float] = []

    async def build() -> None:
        async with store.build_conn():
            held.set()
            await release.wait()

    async def drain() -> None:
        await scheduler.drain_store(store.store_id, drain_timeout=10)
        drained_at.append(anyio.current_time())

    with structlog.testing.capture_logs() as logs, anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(build)
            await held.wait()
            tg.start_soon(drain)
            await anyio.sleep(0.05)
            assert drained_at == [], "the drain ended with a connection out"
            released_at = anyio.current_time()
            release.set()

    assert drained_at[0] - released_at < 0.5
    assert _timeouts(logs) == []


async def test_a_drain_past_its_timeout_says_so() -> None:
    store = MockStore("drain-stuck", feature_matrix={"x86_64-linux": set()})
    release = anyio.Event()
    with structlog.testing.capture_logs() as logs, anyio.fail_after(5):
        async with anyio.create_task_group() as tg:

            async def build(*, task_status: TaskStatus[None] = anyio.TASK_STATUS_IGNORED) -> None:
                async with store.build_conn():
                    task_status.started()
                    await release.wait()

            await tg.start(build)
            await _scheduler(store).drain_store(store.store_id, drain_timeout=0.05)
            release.set()

    [timeout] = _timeouts(logs)
    assert timeout["in_flight"] == 1
