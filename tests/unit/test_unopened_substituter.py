"""A substituter that pynixd cannot open is left out, as Nix leaves it out, and stays out while it cools down.

`BinaryCacheStore::init` (`binary-cache-store.cc:48`) throws when it cannot
read `nix-cache-info`, and `getDefaultSubstituters` (`store-registration.cc:88`)
drops that cache. The daemon then has no substituter for a floating output,
and it builds. pynixd kept the cache and asked it, and the failed query
failed the goal. nixidae/nixidae#1200.

Each Nix daemon worker opens the cache again, 30 s for a dead host. pynixd
remembers the failure for every client and opens the cache again after a
cooldown that doubles at each failure. nixidae/nixidae#1257.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
from aiohttp import web

from pynixd.config import PynixdSettings
from pynixd.cooldown import Cooldown
from pynixd.instance import _add_substituter_stores
from pynixd.store.http_binary_cache import HTTPBinaryCacheStore
from pynixd.substituters import ClientSubstituters
from pynixd.substitution_queue import SubstitutionQueue

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pynixd.context import PynixdContext


@asynccontextmanager
async def _cache(status: list[int], asked: list[int]) -> AsyncIterator[str]:
    """A cache that answers `nix-cache-info` with `status[0]`, and counts each request in *asked*."""
    app = web.Application()

    async def cache_info(request: web.Request) -> web.Response:
        asked.append(status[0])
        if status[0] != 200:
            return web.Response(status=status[0])
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


def test_a_cooldown_doubles_to_its_ceiling_and_a_success_clears_it() -> None:
    clock = [0.0]
    cooldown = Cooldown(base=60, ceiling=200, now=lambda: clock[0])
    assert not cooldown.cooling
    assert [cooldown.failed(), cooldown.failed(), cooldown.failed()] == [60, 120, 200]
    clock[0] = 199
    assert cooldown.cooling
    clock[0] = 200
    assert not cooldown.cooling
    cooldown.succeeded()
    assert cooldown.failed() == 60


async def test_a_client_named_cache_that_fails_to_open_is_left_out_until_its_cooldown_ends() -> None:
    asked: list[int] = []
    clock = [0.0]
    dead_status, empty_status = [500], [404]
    async with (
        _cache(dead_status, asked) as dead,
        _cache(empty_status, asked) as empty,
        _cache([200], asked) as live,
    ):
        urls = [dead, empty, live]
        substituters = ClientSubstituters(PynixdSettings(), now=lambda: clock[0])
        async with substituters.running():
            first = await substituters.resolve(urls, [])
            second = await substituters.resolve(urls, [])
            asked_while_cooling = sorted(asked)
            clock[0] = 60
            dead_status[0] = 200
            third = await substituters.resolve(urls, [])
            clock[0] = 60 + 119
            fourth = await substituters.resolve(urls, [])
            asked_before_the_second_wait_ends = sorted(asked)
            clock[0] = 60 + 120
            await substituters.resolve(urls, [])

    assert first is not None
    assert [store.url for store in first if isinstance(store, HTTPBinaryCacheStore)] == [f"{live}/"]
    assert second == first
    assert asked_while_cooling == [200, 404, 500], "no client opens a cache while it cools down"
    assert third is not None
    assert [store.url for store in third if isinstance(store, HTTPBinaryCacheStore)] == [f"{dead}/", f"{live}/"]
    assert fourth == third
    assert asked_before_the_second_wait_ends == [200, 200, 404, 404, 500], "a second failure waits twice as long"
    assert sorted(asked) == [200, 200, 404, 404, 404, 500]


async def test_a_configured_cache_that_fails_to_open_is_absent_until_it_opens() -> None:
    """The cache stays covered, so a client's query does not go to the daemon, and no query asks it."""
    asked: list[int] = []
    status = [500]
    async with _cache(status, asked) as dead:
        settings = PynixdSettings()
        stores: dict[Any, Any] = {}
        _add_substituter_stores(stores, [dead], settings)
        (store,) = stores.values()
        assert isinstance(store, HTTPBinaryCacheStore)
        store.opening = Cooldown(base=0.05)
        queue = SubstitutionQueue(cast("PynixdContext", SimpleNamespace(settings=settings, stores=stores)))
        async with store:
            assert not store.opened
            assert store.cache_info["WantMassQuery"] == "0"
            queue.note_daemon_substituters([dead])
            assert queue.uncovered == ()
            assert await queue.substituters_for(None) == ()
            status[0] = 200
            with anyio.fail_after(5):
                while not store.opened:
                    await anyio.sleep(0.01)
            assert await queue.substituters_for(None) == (store,)
            assert store.cache_info["Priority"] == "40"
    assert asked[0] == 500
    assert asked[-1] == 200
