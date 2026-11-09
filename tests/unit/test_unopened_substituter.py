"""A substituter that a client names and pynixd cannot open is left out, as Nix leaves it out.

`BinaryCacheStore::init` (`binary-cache-store.cc:48`) throws when it cannot
read `nix-cache-info`, and `getDefaultSubstituters` (`store-registration.cc:88`)
drops that cache. The daemon then has no substituter for a floating output,
and it builds. pynixd kept the cache and asked it, and the failed query
failed the goal. nixidae/nixidae#1200.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from aiohttp import web

from nix_daemon_protocol.ids import StoreId
from pynixd.config import HTTPBinaryCacheSpec, PynixdSettings
from pynixd.store.http_binary_cache import HTTPBinaryCacheStore
from pynixd.substituters import ClientSubstituters

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@asynccontextmanager
async def _cache(status: int, asked: list[int]) -> AsyncIterator[str]:
    """A cache that answers `nix-cache-info` with *status*, and counts each request in *asked*."""
    app = web.Application()

    async def cache_info(request: web.Request) -> web.Response:
        asked.append(status)
        if status != 200:
            return web.Response(status=status)
        return web.Response(text="StoreDir: /nix/store\nPriority: 40\n")

    app.router.add_get("/nix-cache-info", cache_info)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


async def test_a_cache_that_fails_to_open_is_left_out_and_tried_again() -> None:
    asked: list[int] = []
    async with _cache(500, asked) as dead, _cache(404, asked) as empty, _cache(200, asked) as live:
        substituters = ClientSubstituters(PynixdSettings())
        async with substituters.running():
            first = await substituters.resolve([dead, empty, live], [])
            second = await substituters.resolve([dead, empty, live], [])

    assert first is not None
    assert len(first) == 1
    assert [store.url for store in first if isinstance(store, HTTPBinaryCacheStore)] == [f"{live}/"]
    assert second == first
    assert sorted(asked) == [200, 404, 404, 500, 500], "each resolve opens a failed cache again"


async def test_a_configured_cache_that_fails_to_open_starts_with_nix_defaults() -> None:
    asked: list[int] = []
    async with _cache(500, asked) as dead:
        spec = HTTPBinaryCacheSpec(store_id=StoreId("configured"), url=dead)
        store = HTTPBinaryCacheStore(spec)
        await store.start()
        try:
            assert store.cache_info["WantMassQuery"] == "0"
        finally:
            await store.close()
    assert asked == [500]
