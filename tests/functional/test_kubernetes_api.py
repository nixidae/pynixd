from __future__ import annotations

import asyncio

import aiohttp
import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
)
from pynixd.store_path import StorePath
from tests.conftest import serde_path
from tests.functional.mock_store import MockStore


@pytest.mark.xfail(reason="MockStore missing BuildDerivationRequest response")
async def test_dynamic_store_management() -> None:
    """Verify adding and removing stores at runtime works correctly."""
    local_store = MockStore("local", feature_matrix={"x86_64-linux": set()})

    server = Server(stores={StoreId("local"): local_store}, http_port=0, http_enable_metrics=True)
    await server.start()

    try:
        scheduler = server.scheduler
        assert scheduler is not None
        assert len(scheduler.stores) == 0

        # 1. Add stores dynamically
        remote1 = MockStore("remote1", feature_matrix={"x86_64-linux": set()})
        remote2 = MockStore("remote2", feature_matrix={"x86_64-linux": set()})
        await server.add_store(remote1)
        await server.add_store(remote2)
        assert "remote1" in scheduler.stores
        assert "remote2" in scheduler.stores
        assert scheduler.allocator.stores == scheduler.stores

        # 2. Enqueue a build and block it
        drv_path = StorePath("/nix/store/00000000000000000000000000000001-test.drv")
        request = BuildDerivationRequest(
            drv_path=serde_path(drv_path),
            derivation=BasicDerivation(platform="x86_64-linux", builder=""),
            build_mode=BuildMode.NORMAL,
        )

        # Mock build responder
        resp = BuildDerivationResponse(
            result=BuildResult(status=BuildResultStatus.BUILT),
        )
        remote1.responses[BuildDerivationRequest] = resp
        remote2.responses[BuildDerivationRequest] = resp

        # Block the build
        remote1.block_build(drv_path)

        build_id, _future = await scheduler.build_derivation(
            request,
        )

        await scheduler.schedule()

        # Wait for assignment
        queued_build = scheduler.queue.by_id[build_id]
        for _ in range(50):
            if queued_build.assigned_store_id == "remote1":
                break
            await anyio.sleep(0.05)
        assert queued_build.assigned_store_id == "remote1"
        assert queued_build.is_building

        # 3. Remove store with short timeout (force kill)
        # We start the removal in background because it will wait for the build
        remove_task = asyncio.create_task(
            server.remove_store(StoreId("remote1"), drain_timeout=0.1),
        )

        # Wait for drain timeout to trigger hard-kill
        await anyio.sleep(0.2)
        await remove_task

        assert "remote1" not in scheduler.stores

        # Verify the build was requeued and moved to another store or is pending retry
        # (It might have already been picked up by remote2 if scheduling loop is fast)
        for _ in range(50):
            if queued_build.assigned_store_id == "remote2" or queued_build.is_pending:
                break
            await anyio.sleep(0.05)

        assert queued_build.assigned_store_id != "remote1"
        assert queued_build.retries == 1

    finally:
        await server.close()


async def test_prometheus_metrics_endpoint() -> None:
    """Verify that the /metrics endpoint serves Prometheus data."""
    local_store = MockStore("local", feature_matrix={"x86_64-linux": set()})

    # Start server with metrics enabled on random port
    server = Server(
        stores={StoreId("local"): local_store},
        http_port=0,
        http_enable_metrics=True,
        http_metrics_no_auth=True,
    )
    await server.start()

    try:
        port = server.http_bound_port
        url = f"http://127.0.0.1:{port}/metrics"

        async with aiohttp.ClientSession() as session, session.get(url) as resp:
            assert resp.status == 200
            text = await resp.text()

            # Check for our custom metrics
            assert "pynixd_build_queue_size" in text

    finally:
        await server.close()
