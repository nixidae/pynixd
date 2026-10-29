"""The HTTP cache answers misses from a race of the HTTP substituters.

Nix asks each substituter in turn for every path. The `.narinfo` handler
asks all HTTP substituters at once instead: the first store that answers
wins the narinfo, served with only the URL line rewritten to this cache,
and a NAR request redirects to the highest-priority store that answered.
The losing requests are not cancelled; they land in the substitution
caches and rank the later redirect. Issue #85.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio

from nix_daemon_protocol.ids import StoreId
from pynixd.config import HTTPBinaryCacheSpec, PynixdSettings
from pynixd.http_server import PynixdHttpServer, _rewrite_narinfo_url
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

    assert await queue.race_http_narinfo(_HASH) is None
    return calls


async def test_a_miss_the_scheduler_just_saw_asks_nobody() -> None:
    """dynhetz, 2026-10-29: a trivial local build waited 2 s, the query
    timeout, for caches the scheduler had asked a moment before. A remote
    build waits for none. nixidae/nixidae#1125."""
    stores = (_store("cache-a", "https://a.example/"), _store("cache-b", "https://b.example/"))

    assert await _race_after_scheduler_misses(*stores) == 0
    assert await _race_after_scheduler_misses(stores[0]) == 2, "a store nobody asked yet is still raced"


async def test_upstream_narinfo_rewrites_only_the_url() -> None:
    """The winner's text goes out with signatures and hashes intact."""
    store = _store("cache-a", "https://a.example/")

    async def hit(hash_part: str) -> tuple[str, Any] | None:
        return _fetched(_NARINFO)

    store.fetch_narinfo = hit  # type: ignore[method-assign]
    queue = _queue(store)
    server = PynixdHttpServer(
        SimpleNamespace(),  # type: ignore[arg-type]
        substitution_queue=queue,
    )

    response = await server.handle_upstream_narinfo(_HASH)
    assert response.status == 200
    assert response.text is not None
    assert f"URL: nar/{_HASH}.nar\n" in response.text
    assert "URL: nar/deadbeef.nar.xz" not in response.text
    assert "Sig: cache.nixos.org-1:dddd" in response.text
    assert "NarHash: sha256:cccc" in response.text


def test_rewrite_narinfo_url_leaves_other_lines_alone() -> None:
    """The rewrite touches the URL line and nothing else, byte for byte."""
    rewritten = _rewrite_narinfo_url(_NARINFO, _HASH)
    assert rewritten.splitlines() == [
        f"StorePath: /nix/store/{_HASH}-widget",
        f"URL: nar/{_HASH}.nar",
        "Compression: xz",
        "FileHash: sha256:bbbb",
        "FileSize: 123",
        "NarHash: sha256:cccc",
        "NarSize: 456",
        "References: ",
        "Sig: cache.nixos.org-1:dddd",
    ]


async def test_nar_redirect_picks_highest_priority_responder() -> None:
    """The redirect ranks by priority, not by who answered fastest."""
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

    # One narinfo race teaches the queue the hash and fills both caches.
    response = await server.handle_upstream_narinfo(_HASH)
    assert response.status == 200

    async def narinfo(path: Any) -> Any:
        return SimpleNamespace(url="nar/deadbeef.nar.xz")

    cheap.get_narinfo = narinfo  # type: ignore[method-assign]
    dear.get_narinfo = narinfo  # type: ignore[method-assign]

    redirect = await server.redirect_to_upstream(_HASH)
    assert redirect is not None
    assert redirect.status == 307
    assert redirect.headers["Location"] == "https://dear.example/nar/deadbeef.nar.xz"


async def test_nar_redirect_misses_when_nobody_answered() -> None:
    """No upstream answer means no redirect, and the handler says 404."""
    store = _store("cache-a", "https://a.example/")

    async def miss(hash_part: str) -> tuple[str, Any] | None:
        return None

    store.fetch_narinfo = miss  # type: ignore[method-assign]
    queue = _queue(store)
    server = PynixdHttpServer(
        SimpleNamespace(),  # type: ignore[arg-type]
        substitution_queue=queue,
    )

    assert await server.redirect_to_upstream(_HASH) is None
    response = await server.handle_upstream_nar(_HASH)
    assert response.status == 404


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
    assert (await server.handle_upstream_nar(_HASH)).status == 404
    assert calls == 0
