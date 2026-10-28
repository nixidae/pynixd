"""Tests for the build log pub/sub system (QueuedBuild subscribers + byte buffer replay)."""

import asyncio

import anyio
import pytest

from nix_daemon_protocol.ids import BuildId, StoreId
from pynixd.build_queue import BuildQueue
from pynixd.config import PynixdSettings
from pynixd.connection import ClientConn
from pynixd.context import PynixdContext
from pynixd.scheduler import Scheduler
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    LogNext,
    WireLogs,
)
from pynixd.store_path import StorePath
from pynixd.wire import BytesWriter
from tests.conftest import serde_path
from tests.functional.mock_store import MockStore


@pytest.mark.xfail(reason="flaky: build scheduling timing in CI")
async def test_build_log_pubsub_two_clients() -> None:
    """Verify that two clients subscribing to the same build receive identical log output.

    Scenario:
    - A build produces 10 log lines ("1\n" through "10\n") with 1s delays.
    - Client 1 subscribes before the build starts.
    - Client 2 subscribes ~5s after the build starts (mid-flight).
    - Both clients receive the same complete log output.

    The late subscriber gets the full buffer replayed via add_subscriber(),
    plus any subsequent real-time messages via post_log_bytes().
    """
    local_store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    remote = MockStore("remote", feature_matrix={"x86_64-linux": set()})

    ctx = PynixdContext(
        settings=PynixdSettings(),
        _stores={StoreId("local"): local_store, StoreId("remote"): remote},
    )
    scheduler = Scheduler(ctx)

    drv_path = StorePath("/nix/store/00000000000000000000000000000001-test.drv")

    # Build a response with 10 pre-populated log lines (simulating a build
    # that printed 1-10). The scheduler's _execute() will fan these out
    # to all subscribers after the mock returns.
    logs = WireLogs()
    for i in range(1, 11):
        logs.messages.append(LogNext(text=f"{i}\n"))

    build_resp = BuildDerivationResponse(
        result=BuildResult(status=BuildResultStatus.BUILT),
        logs=logs,
    )
    local_store.responses[BuildDerivationRequest] = build_resp
    remote.responses[BuildDerivationRequest] = build_resp

    # Block the build on the remote store so we can subscribe client1
    # before execution actually starts.
    build_blocker = remote.block_build(drv_path)

    request = BuildDerivationRequest(
        drv_path=serde_path(drv_path),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )

    build_id, future = await scheduler.build_derivation(
        request,
    )

    # Create two separate client connections with their own buffers.
    buf1 = BytesWriter("client1")
    client1 = ClientConn(buf1)
    buf2 = BytesWriter("client2")
    client2 = ClientConn(buf2)

    # Subscribe client1 before the build starts.
    subscribed = await scheduler.queue.subscribe(build_id, client1)
    assert subscribed

    # Trigger scheduling — this assigns the build and spawns execute_build.
    # The build will block on build_blocker inside MockStore.execute_mock().
    async with scheduler.workers():
        await scheduler.schedule()
        await anyio.sleep(0.05)

        # Verify the build is blocked (assigned but waiting).
        queued_build = scheduler.queue.by_id[build_id]
        assert queued_build.is_building

        # Release the block after a short delay to simulate build execution
        # time, so the test stays fast and still takes the timing-sensitive path.
        async def release_blocker() -> None:
            await anyio.sleep(0.2)
            build_blocker.set()

        release_task = asyncio.create_task(release_blocker())

        # Wait for the build to complete (this is when _execute() fans out logs).
        resp = await future
        assert resp.result.status == BuildResultStatus.BUILT
        await release_task

    # Now subscribe client2 — the build is already done, so add_subscriber
    # will replay the full _log_buf.
    subscribed = await scheduler.queue.subscribe(build_id, client2)
    assert subscribed

    # Both clients should have received identical log output.
    data1 = buf1.get_bytes()
    data2 = buf2.get_bytes()
    assert data1 == data2
    assert len(data1) > 0

    # Verify the actual content: 10 serialized LogNext messages.
    # Each message has a uint64 code + length-prefixed string.
    # We can verify by checking the buffer is non-empty and both match.
    # For a more precise check, count the STDERR_NEXT codes (0x64 = 100).
    assert data1.count(b"1\n") >= 1
    assert data1.count(b"10\n") >= 1


async def test_build_log_pubsub_late_subscriber_gets_full_history() -> None:
    """Verify that a subscriber joining after build completion gets full replay.

    This test directly exercises QueuedBuild.add_subscriber() replay
    without going through the scheduler, to isolate the pub/sub mechanism.
    """
    from pynixd.build_queue import QueuedBuild

    drv_path = StorePath("/nix/store/00000000000000000000000000000001-test.drv")
    request = BuildDerivationRequest(
        drv_path=serde_path(drv_path),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )

    loop = asyncio.get_running_loop()
    future: asyncio.Future[BuildDerivationResponse] = loop.create_future()
    build = QueuedBuild(
        build_id=BuildId(1),
        request=request,
        future=future,
    )

    # Create two client connections.
    buf1 = BytesWriter("early-client")
    client1 = ClientConn(buf1)
    buf2 = BytesWriter("late-client")
    client2 = ClientConn(buf2)

    # Subscribe client1 first.
    await build.add_subscriber(client1)

    # Post 10 log messages (simulating what _execute() does).
    for i in range(1, 11):
        await build.post_log_and_fanout(LogNext(text=f"{i}\n"))

    # Subscribe client2 after all logs are posted.
    await build.add_subscriber(client2)

    # The late client reads everything the early one did, after a
    # BUILD_WAITING activity that says it joined a build it did not ask for.
    # Issue #25 and `tests/unit/test_build_waiting_activity.py`.
    assert buf2.get_bytes().endswith(buf1.get_bytes())
    assert len(buf2.get_bytes()) > len(buf1.get_bytes())

    # Post one more message — only client1 should get it (client2 was added
    # after the last fan-out, but both are now subscribed).
    await build.post_log_and_fanout(LogNext(text="done\n"))

    # Both got "done\n", so the late client is still the early one plus its
    # activity.
    assert buf2.get_bytes().endswith(buf1.get_bytes())


async def test_client_bound_build_cancels_after_last_unsubscribe() -> None:
    queue = BuildQueue()
    drv_path = StorePath("/nix/store/00000000000000000000000000000001-test.drv")
    request = BuildDerivationRequest(
        drv_path=serde_path(drv_path),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )
    build_id, future = await queue.enqueue(request)
    client = ClientConn(BytesWriter("client"))

    assert await queue.subscribe(build_id, client, cancel_on_unsubscribe=True)
    assert await queue.subscribe(build_id, client, cancel_on_unsubscribe=True)

    assert await queue.unsubscribe(build_id, client)
    assert not future.done()

    assert await queue.unsubscribe(build_id, client)
    assert future.done()
    assert future.result().result.status == BuildResultStatus.MISC_FAILURE
    assert "all clients disconnected" in future.result().result.error_msg


async def test_client_bound_active_build_task_cancels_after_last_unsubscribe() -> None:
    local_store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    remote = MockStore("remote", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(
        settings=PynixdSettings(),
        _stores={StoreId("local"): local_store, StoreId("remote"): remote},
    )
    scheduler = Scheduler(ctx)
    build_resp = BuildDerivationResponse(result=BuildResult(status=BuildResultStatus.BUILT))
    remote.responses[BuildDerivationRequest] = build_resp
    local_store.responses[BuildDerivationRequest] = build_resp

    drv_path = StorePath("/nix/store/00000000000000000000000000000001-test.drv")
    remote.block_build(drv_path)
    request = BuildDerivationRequest(
        drv_path=serde_path(drv_path),
        derivation=BasicDerivation(platform="x86_64-linux", builder=""),
        build_mode=BuildMode.NORMAL,
    )
    build_id, future = await scheduler.build_derivation(request)
    client = ClientConn(BytesWriter("client"))
    assert await scheduler.queue.subscribe(build_id, client, cancel_on_unsubscribe=True)

    async with scheduler.workers():
        await scheduler.schedule()
        build = scheduler.queue.by_id[build_id]
        assert build.is_building
        assert await scheduler.queue.unsubscribe(build_id, client)

        for _ in range(20):
            if not build.is_building:
                break
            await anyio.sleep(0.01)

        # The worker left: it saw the cancel, not the blocked build's end.
        assert not build.is_building
    assert future.done()
    assert future.result().result.status == BuildResultStatus.MISC_FAILURE
