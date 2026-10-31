"""The HTTP cache answers misses from a race of the HTTP substituters.

Nix asks each substituter in turn for every path. The `.narinfo` handler
asks all HTTP substituters at once instead: the first store that answers
wins the narinfo, served as a description of the raw NAR, and a NAR request
streams it decompressed from the highest-priority store that answered.
The losing requests are not cancelled; they land in the substitution
caches and rank that later choice. Issue #85, nixidae/nixidae#1137.
"""

from __future__ import annotations

import lzma
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from nix_daemon_protocol.ids import StoreId
from pynixd.config import HTTPBinaryCacheSpec, PynixdSettings
from pynixd.http_server import PynixdHttpServer, _uncompressed_narinfo
from pynixd.store.http_binary_cache import HTTPBinaryCacheStore, _parse_narinfo
from pynixd.store_path import StorePath
from pynixd.substitution_queue import SubstitutionQueryResult, SubstitutionQueue

if TYPE_CHECKING:
    from pynixd.context import PynixdContext

_HASH = "a" * 32
_NARINFO = (
    f"StorePath: /nix/store/{_HASH}-widget\n"
    "URL: nar/deadbeef.nar.xz\n"
    "Compression: xz\n"
    "FileHash: sha256:bbbb\n"
    "FileSize: 123\n"
    "NarHash: sha256:cccc\n"
    "NarSize: 456\n"
    "References: \n"
    "Sig: cache.nixos.org-1:dddd\n"
)


def _store(store_id: str, url: str, priority: float = 10.0) -> HTTPBinaryCacheStore:
    store = HTTPBinaryCacheStore(HTTPBinaryCacheSpec(store_id=StoreId(store_id), url=url))
    store.priority = priority
    return store


def _queue(*stores: HTTPBinaryCacheStore, **settings_kwargs: Any) -> SubstitutionQueue:
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            settings=PynixdSettings(**settings_kwargs),
            stores={store.store_id: store for store in stores},
        ),
    )
    return SubstitutionQueue(ctx)


def _fetched(raw: str | None) -> tuple[str, Any] | None:
    """The `(raw, parsed)` answer `HTTPBinaryCacheStore.fetch_narinfo` gives."""
    if raw is None:
        return None
    return raw, _parse_narinfo(raw, expected_path=None)


async def _wait_for(condition: Any, timeout: float = 5.0) -> None:
    with anyio.fail_after(timeout):
        while not condition():
            await anyio.sleep(0.01)


async def test_race_returns_first_responder_and_keeps_the_stragglers() -> None:
    """The narinfo goes out from whoever answers first, uncancelled rest.

    A fast store answers after 50 ms, a slow one after 150 ms. The race
    returns the fast answer in well under the slow one's time, and the
    slow answer still lands in the positive cache afterwards, which is
    what the later NAR redirect ranks.
    """
    fast = _store("cache-fast", "https://fast.example/")
    slow = _store("cache-slow", "https://slow.example/")

    async def fast_fetch(hash_part: str) -> tuple[str, Any] | None:
        await anyio.sleep(0.05)
        return _fetched(_NARINFO)

    async def slow_fetch(hash_part: str) -> tuple[str, Any] | None:
        await anyio.sleep(0.15)
        return _fetched(_NARINFO)

    fast.fetch_narinfo = fast_fetch  # type: ignore[method-assign]
    slow.fetch_narinfo = slow_fetch  # type: ignore[method-assign]
    queue = _queue(fast, slow)

    async with queue.running():
        winner = await queue.race_http_narinfo(_HASH)
        assert winner is not None
        assert winner.store.store_id == StoreId("cache-fast")

        path = winner.info.path
        await _wait_for(lambda: slow.store_id in queue.positive.get(path, {}))
        assert queue.positive[path][slow.store_id].found


async def test_race_total_miss_is_remembered_briefly() -> None:
    """A hash no store has races once, then answers from the miss cache."""
    calls = 0
    store = _store("cache-a", "https://a.example/")

    async def miss(hash_part: str) -> tuple[str, Any] | None:
        nonlocal calls
        calls += 1
        await anyio.sleep(0.01)
        return None

    store.fetch_narinfo = miss  # type: ignore[method-assign]
    queue = _queue(store)

    async with queue.running():
        assert await queue.race_http_narinfo(_HASH) is None
        assert await queue.race_http_narinfo(_HASH) is None
    assert calls == 1


async def _race_after_scheduler_misses(*missed_by: HTTPBinaryCacheStore) -> int:
    """Race the hash after the scheduler's own query found it on none of *missed_by*; the upstream calls made."""
    calls = 0
    first = _store("cache-a", "https://a.example/")
    second = _store("cache-b", "https://b.example/")

    async def miss(hash_part: str) -> tuple[str, Any] | None:
        nonlocal calls
        calls += 1
        return None

    first.fetch_narinfo = miss  # type: ignore[method-assign]
    second.fetch_narinfo = miss  # type: ignore[method-assign]
    queue = _queue(first, second)
    path = StorePath(path=f"/nix/store/{_HASH}-widget")
    for store in (first, second):
        if store.store_id in {missed.store_id for missed in missed_by}:
            queue.record_query_result(
                path, SubstitutionQueryResult(store_id=store.store_id, path_info=None, query_succeeded=True)
            )

    async with queue.running():
        assert await queue.race_http_narinfo(_HASH) is None
    return calls


async def test_a_miss_the_scheduler_just_saw_asks_nobody() -> None:
    """dynhetz, 2026-10-29: a trivial local build waited 2 s, the query
    timeout, for caches the scheduler had asked a moment before. A remote
    build waits for none. nixidae/nixidae#1125."""
    stores = (_store("cache-a", "https://a.example/"), _store("cache-b", "https://b.example/"))

    assert await _race_after_scheduler_misses(*stores) == 0
    assert await _race_after_scheduler_misses(stores[0]) == 2, "a store nobody asked yet is still raced"


async def test_upstream_narinfo_describes_the_raw_nar() -> None:
    """The winner's text goes out as a raw NAR, signatures and hashes intact."""
    store = _store("cache-a", "https://a.example/")

    async def hit(hash_part: str) -> tuple[str, Any] | None:
        return _fetched(_NARINFO)

    store.fetch_narinfo = hit  # type: ignore[method-assign]
    queue = _queue(store)
    server = PynixdHttpServer(
        SimpleNamespace(),  # type: ignore[arg-type]
        substitution_queue=queue,
    )

    async with queue.running():
        response = await server.handle_upstream_narinfo(_HASH)
    assert response.status == 200
    assert response.text is not None
    assert f"URL: nar/{_HASH}.nar\n" in response.text
    assert "Compression: none\n" in response.text
    assert "URL: nar/deadbeef.nar.xz" not in response.text
    assert "Sig: cache.nixos.org-1:dddd" in response.text
    assert "NarHash: sha256:cccc" in response.text


def test_uncompressed_narinfo_drops_the_upstream_file() -> None:
    """The upstream's file fields go; every other line stays, byte for byte."""
    rewritten = _uncompressed_narinfo(_NARINFO, _HASH)
    assert rewritten.splitlines() == [
        f"StorePath: /nix/store/{_HASH}-widget",
        "NarHash: sha256:cccc",
        "NarSize: 456",
        "References: ",
        "Sig: cache.nixos.org-1:dddd",
        f"URL: nar/{_HASH}.nar",
        "Compression: none",
    ]


def test_uncompressed_narinfo_names_none_when_upstream_names_nothing() -> None:
    """An absent `Compression` means bzip2 to Nix (`nar-info.cc:101`)."""
    bare = "".join(line for line in _NARINFO.splitlines(keepends=True) if not line.startswith("Compression:"))
    assert "Compression: none" in _uncompressed_narinfo(bare, _HASH).splitlines()


async def test_nar_source_picks_highest_priority_responder() -> None:
    """The NAR's source ranks by priority, not by who answered fastest."""
    cheap = _store("cache-cheap", "https://cheap.example/", priority=40.0)
    dear = _store("cache-dear", "https://dear.example/", priority=5.0)

    async def hit(hash_part: str) -> tuple[str, Any] | None:
        return _fetched(_NARINFO)

    cheap.fetch_narinfo = hit  # type: ignore[method-assign]
    dear.fetch_narinfo = hit  # type: ignore[method-assign]
    queue = _queue(cheap, dear)
    server = PynixdHttpServer(
        SimpleNamespace(),  # type: ignore[arg-type]
        substitution_queue=queue,
    )

    async def narinfo(path: Any) -> Any:
        return SimpleNamespace(url="nar/deadbeef.nar.xz")

    async with queue.running():
        # One narinfo race teaches the queue the hash and fills both caches.
        response = await server.handle_upstream_narinfo(_HASH)
        assert response.status == 200

        cheap.get_narinfo = narinfo  # type: ignore[method-assign]
        dear.get_narinfo = narinfo  # type: ignore[method-assign]

        source = await queue.upstream_nar_source(_HASH)
    assert source is not None
    assert source[0].store_id == StoreId("cache-dear")


def _nar_client(server: PynixdHttpServer) -> TestClient[web.Request, web.Application]:
    """A client of `handle_upstream_nar` alone; no local store is consulted."""
    app = web.Application()

    async def nar(request: web.Request) -> web.StreamResponse:
        return await server.handle_upstream_nar(request, request.match_info["hash"])

    app.router.add_get("/nar/{hash}.nar", nar)
    return TestClient(TestServer(app))


async def test_upstream_nar_streams_decompressed() -> None:
    """An upstream's xz file reaches the client as the raw NAR the narinfo hashes."""
    raw = b"nix-archive-1 widget " * 4096
    narinfo = _NARINFO.replace("NarSize: 456", f"NarSize: {len(raw)}")
    upstream = web.Application()

    async def get_nar(request: web.Request) -> web.Response:
        return web.Response(body=lzma.compress(raw))

    upstream.router.add_get("/nar/deadbeef.nar.xz", get_nar)
    async with TestServer(upstream) as upstream_server:
        store = _store("cache-a", str(upstream_server.make_url("/")))
        await store.start()
        try:

            async def hit(hash_part: str) -> tuple[str, Any] | None:
                return _fetched(narinfo)

            store.fetch_narinfo = hit  # type: ignore[method-assign]
            queue = _queue(store)
            server = PynixdHttpServer(
                SimpleNamespace(),  # type: ignore[arg-type]
                substitution_queue=queue,
            )
            async with queue.running(), _nar_client(server) as client:
                response = await client.get(f"/nar/{_HASH}.nar")
                assert response.status == 200
                assert response.headers["Content-Length"] == str(len(raw))
                assert await response.read() == raw
        finally:
            await store.close()


async def test_upstream_nar_misses_when_nobody_answered() -> None:
    """No upstream answer means no source, and the handler says 404."""
    store = _store("cache-a", "https://a.example/")

    async def miss(hash_part: str) -> tuple[str, Any] | None:
        return None

    store.fetch_narinfo = miss  # type: ignore[method-assign]
    queue = _queue(store)
    server = PynixdHttpServer(
        SimpleNamespace(),  # type: ignore[arg-type]
        substitution_queue=queue,
    )

    async with queue.running():
        assert await queue.upstream_nar_source(_HASH) is None
        async with _nar_client(server) as client:
            assert (await client.get(f"/nar/{_HASH}.nar")).status == 404


async def test_upstream_race_disabled_keeps_misses_404() -> None:
    """With the race off, a local miss is a 404 and asks nobody."""
    calls = 0
    store = _store("cache-a", "https://a.example/")

    async def hit(hash_part: str) -> tuple[str, Any] | None:
        nonlocal calls
        calls += 1
        return _fetched(_NARINFO)

    store.fetch_narinfo = hit  # type: ignore[method-assign]
    queue = _queue(store)
    server = PynixdHttpServer(
        SimpleNamespace(),  # type: ignore[arg-type]
        substitution_queue=queue,
        upstream_race=False,
    )

    assert (await server.handle_upstream_narinfo(_HASH)).status == 404
    async with _nar_client(server) as client:
        assert (await client.get(f"/nar/{_HASH}.nar")).status == 404
    assert calls == 0
