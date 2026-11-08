"""The substituters a client names become its own set of backends.

The Nix client folds `extra-substituters` into `substituters` before it sends
`SetOptions` (`configuration.cc:27`), so a client's list replaces the
daemon's. A substituter pynixd has no backend for sends the question to the
daemon, as before.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from pynixd.config import HTTPBinaryCacheSpec, LocalBinaryCacheSpec, PynixdSettings
from pynixd.store.local_binary_cache import LocalBinaryCacheStore
from pynixd.substitution_queue import SubstitutionQueue
from pynixd_store.store_path import StorePath

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.connection import ClientConn
    from pynixd.context import PynixdContext
    from pynixd.store import Store


def _client(*, use_substitutes: bool = True, **overrides: str) -> ClientConn:
    return cast(
        "ClientConn", SimpleNamespace(options=SimpleNamespace(use_substitutes=use_substitutes, overrides=overrides))
    )


def _queue(stores: dict[Any, Store] | None = None, settings: PynixdSettings | None = None) -> SubstitutionQueue:
    ctx = SimpleNamespace(settings=settings or PynixdSettings(), stores=stores or {})
    return SubstitutionQueue(cast("PynixdContext", ctx))


def _cache(root: Path) -> Path:
    root.mkdir()
    (root / "nix-cache-info").write_text("StoreDir: /nix/store\nPriority: 30\n")
    return root


async def test_without_a_list_the_daemons_set_holds() -> None:
    daemon_cache = HTTPBinaryCacheSpec(url="https://cache.example/").to_store("http-cache.example")
    queue = _queue({"http-cache.example": daemon_cache})
    assert await queue.substituters_for(None) == (daemon_cache,)
    assert await queue.substituters_for(_client()) == (daemon_cache,)


async def test_an_uncovered_daemon_substituter_sends_a_client_without_a_list_to_the_daemon() -> None:
    queue = _queue()
    queue.note_daemon_substituters(["s3://bucket"])
    assert await queue.substituters_for(_client()) is None


async def test_use_substitutes_off_leaves_none() -> None:
    """`worker.cc:32`: no substituter at all, which the queue answers without the daemon."""
    assert await _queue().substituters_for(_client(use_substitutes=False, substituters="s3://bucket")) == ()


async def test_a_list_replaces_the_daemons_set(tmp_path: Path) -> None:
    daemon_cache = HTTPBinaryCacheSpec(url="https://cache.example/").to_store("http-cache.example")
    queue = _queue({"http-cache.example": daemon_cache})
    root = _cache(tmp_path / "cache")
    async with queue.running():
        stores = await queue.substituters_for(_client(substituters=f"file://{root}"))
    assert stores is not None
    assert len(stores) == 1
    store = stores[0]
    assert isinstance(store, LocalBinaryCacheStore)
    assert store.path == root


async def test_a_named_cache_takes_its_priority_from_nix_cache_info_or_the_url(tmp_path: Path) -> None:
    """`binary-cache-store.cc:67` sets the cache's priority as a default; `?priority=` wins."""
    first, second = _cache(tmp_path / "first"), _cache(tmp_path / "second")
    queue = _queue()
    async with queue.running():
        stores = await queue.substituters_for(_client(substituters=f"file://{first} file://{second}?priority=10"))
    assert stores is not None
    assert [store.priority for store in stores] == [30.0, 10.0]


async def test_a_named_backend_is_made_once_and_kept(tmp_path: Path) -> None:
    root = _cache(tmp_path / "cache")
    queue = _queue()
    async with queue.running():
        one = await queue.substituters_for(_client(substituters=f"file://{root}"))
        two = await queue.substituters_for(_client(substituters=f"file://{root}"))
    assert one == two
    assert one is not None
    assert len(one) == 1


async def test_a_named_substituter_the_daemon_has_reuses_its_backend() -> None:
    daemon_cache = HTTPBinaryCacheSpec(url="https://cache.example/").to_store("http-cache.example")
    queue = _queue({"http-cache.example": daemon_cache})
    assert await queue.substituters_for(_client(substituters="https://cache.example")) == (daemon_cache,)


async def test_a_trusted_spelling_gets_a_backend_of_its_own(tmp_path: Path) -> None:
    """`?trusted=1` skips the signature check (`substitution-goal.cc:116`), so it is another store."""
    root = _cache(tmp_path / "cache")
    daemon_cache = LocalBinaryCacheSpec(path=root).to_store("file-cache")
    queue = _queue({"file-cache": daemon_cache})
    async with queue.running():
        stores = await queue.substituters_for(_client(substituters=f"file://{root}?trusted=1"))
        assert stores is not None
        assert len(stores) == 1
        assert stores[0] is not daemon_cache
        assert isinstance(stores[0], LocalBinaryCacheStore)
        assert stores[0].trusted
        assert await queue.substituters_for(_client(substituters=f"file://{root}")) == (daemon_cache,)


async def test_a_named_substituter_with_no_backend_sends_the_client_to_the_daemon(tmp_path: Path) -> None:
    root = _cache(tmp_path / "cache")
    queue = _queue()
    assert await queue.substituters_for(_client(substituters=f"file://{root} s3://bucket")) is None
    assert await queue.substituters_for(_client(substituters=f"file://{tmp_path}/missing")) is None


async def test_pynixds_own_cache_named_by_a_client_goes_to_the_daemon() -> None:
    queue = _queue(settings=PynixdSettings(http_port=8080))
    assert await queue.substituters_for(_client(substituters="http://127.0.0.1:8080")) is None


async def test_one_import_per_path_and_set(monkeypatch: Any) -> None:
    """A client must not join an import from another client's set: each set imports on its own."""
    queue = _queue()
    sets: list[tuple[Any, ...]] = []

    async def uncached(path: StorePath, stores: tuple[Any, ...], *, repair: bool = False, ca: Any = None) -> Any:
        sets.append(stores)
        return SimpleNamespace(substituted=False, path=path)

    monkeypatch.setattr(queue, "_substitute_uncached", uncached)
    first = SimpleNamespace(store_id="a")
    second = SimpleNamespace(store_id="b")
    path = StorePath("/nix/store/00000000000000000000000000000001-hello")
    async with queue.running():
        await queue.substitute(path, cast("list[Store]", [first]))
        await queue.substitute(path, cast("list[Store]", [second]))
    assert sets == [(first,), (second,)]
