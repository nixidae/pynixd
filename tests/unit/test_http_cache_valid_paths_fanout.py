"""`HTTPBinaryCacheStore.query_valid_paths` fans its .narinfo lookups out.

One HTTPS round trip per path, so a serial loop pays the full latency of
every miss. The semaphore in `_get_narinfo_raw` bounds the flight; the
queries below prove the loop takes every slot instead of one, and that the
concurrency changes the timing and not the answer. The stores are never
started: `get_narinfo` is stubbed, which is the only network these queries
touch. Issue #54: asking substituters whether a NAR exists is the
read-only half pynixd keeps.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import anyio

from nix_daemon_protocol.ids import StoreId
from pynixd.config import HTTPBinaryCacheSpec
from pynixd.serde import QueryValidPathsRequest
from pynixd.store.http_binary_cache import HTTPBinaryCacheStore
from pynixd_store.store_path import StorePath


def _cache_store() -> HTTPBinaryCacheStore:
    return HTTPBinaryCacheStore(HTTPBinaryCacheSpec(store_id=StoreId("cache"), url="https://cache.nixos.org/"))


def _store_path(i: int) -> StorePath:
    return StorePath(path=f"/nix/store/{i:032d}-path-{i}")


async def test_cache_valid_paths_checks_overlap() -> None:
    """The lookups run together, not one after another.

    A serial loop holds one in flight at a time no matter how many slots
    the semaphore allows. Eight stubbed lookups that each sleep 50 ms must
    all overlap: the deepest pile-up equals the number of paths.
    """
    store = _cache_store()
    in_flight = 0
    deepest = 0

    async def stub(path: StorePath) -> None:
        nonlocal in_flight, deepest
        in_flight += 1
        deepest = max(deepest, in_flight)
        await anyio.sleep(0.05)
        in_flight -= 1
        return None

    store.get_narinfo = stub  # type: ignore[method-assign]
    paths = {_store_path(i) for i in range(8)}
    response = await store.query_valid_paths(QueryValidPathsRequest(paths=paths))
    assert response.paths == set()
    assert deepest == len(paths)


async def test_cache_valid_paths_keeps_the_hits() -> None:
    """Concurrency changes the timing, not the answer: held paths report."""
    store = _cache_store()
    held = {_store_path(1), _store_path(3)}

    async def stub(path: StorePath) -> Any:
        await anyio.sleep(0)
        return SimpleNamespace() if path in held else None

    store.get_narinfo = stub  # type: ignore[method-assign]
    paths = {_store_path(i) for i in range(4)}
    response = await store.query_valid_paths(QueryValidPathsRequest(paths=paths))
    assert response.paths == held
