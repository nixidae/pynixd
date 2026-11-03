"""Substituter URLs, and the backends that stand for the ones a client names.

A client's `substituters` setting replaces the daemon's list: the Nix client
folds `extra-substituters` into it before it sends `SetOptions`
(`Config::set`, `configuration.cc:27`). For an untrusted client the handler
has already kept only the trusted ones (`trust.restrict_overrides`).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final
from urllib.parse import parse_qs, urlsplit

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId

from .config import HTTPBinaryCacheSpec, LocalBinaryCacheSpec
from .store import ExternalUnixStore, is_binary_cache, is_http_binary_cache

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from .config import PynixdSettings
    from .store import BinaryCacheStore, Store

log = structlog.get_logger(__name__)

_LOOPBACK_HOSTS: Final = frozenset({"127.0.0.1", "::1", "localhost"})


@dataclass(frozen=True)
class FileCache:
    """A `file://` substituter: Nix's `LocalBinaryCacheStore` URL."""

    path: Path
    trusted: bool
    priority: float | None


@dataclass(frozen=True)
class HttpCache:
    """An `http://` or `https://` substituter, its query taken apart from its URL."""

    url: str
    trusted: bool
    priority: float | None


def _params(query: str) -> tuple[bool, float | None]:
    params = {key: values[-1] for key, values in parse_qs(query).items()}
    priority = params.get("priority")
    return params.get("trusted") in {"1", "true"}, float(priority) if priority else None


def file_cache(url: str) -> FileCache | None:
    """*url* as a `file://` cache, or `None` for another scheme or a relative path."""
    split = urlsplit(url)
    if split.scheme != "file":
        return None
    path = Path(split.netloc + split.path)
    if not path.is_absolute():
        return None
    trusted, priority = _params(split.query)
    return FileCache(path=path, trusted=trusted, priority=priority)


def http_cache(url: str) -> HttpCache | None:
    """*url* as an HTTP cache, or `None` for another scheme."""
    split = urlsplit(url)
    if split.scheme not in {"http", "https"}:
        return None
    trusted, priority = _params(split.query)
    return HttpCache(url=split._replace(query="", fragment="").geturl(), trusted=trusted, priority=priority)


def _priority(priority: float | None) -> dict[str, float]:
    # Unset, not 1.0: a spec that names no priority takes the one in the cache's `nix-cache-info`.
    return {} if priority is None else {"priority": priority}


def http_spec(cache: HttpCache, store_id: StoreId) -> HTTPBinaryCacheSpec:
    """The spec of a backend for *cache*, its URL parameters applied."""
    spec = HTTPBinaryCacheSpec(store_id=store_id, url=cache.url, trusted=cache.trusted)
    return spec.model_copy(update=_priority(cache.priority))


def file_spec(cache: FileCache, store_id: StoreId) -> LocalBinaryCacheSpec:
    """The spec of a backend for *cache*, its URL parameters applied."""
    spec = LocalBinaryCacheSpec(store_id=store_id, path=cache.path, trusted=cache.trusted)
    return spec.model_copy(update=_priority(cache.priority))


def is_own_cache(url: str, settings: PynixdSettings) -> bool:
    """Whether *url* is this pynixd's own HTTP cache.

    A node's nix.conf can list it, for Nix. As a substituter of pynixd, a
    narinfo miss there asks pynixd itself, joins the race it is part of, and
    waits out `substitution_query_timeout`: 2 s on every new local build on
    dynhetz, 675 timeouts in 30 minutes (nixidae/nixidae#1125).
    """
    split = urlsplit(url)
    port = settings.http_port if split.scheme == "http" else settings.https_port
    if port is None or split.port != port:
        return False
    host = split.hostname or ""
    return host in _LOOPBACK_HOSTS or (bool(settings.http_host) and host == settings.http_host)


def substituter_key(url: str) -> object | None:
    """What makes two spellings of *url* one substituter, or `None` for a scheme pynixd has no backend for.

    `?trusted=1` is part of it: Nix reads another parameter as another store,
    and a trusted cache skips the signature check (`substitution-goal.cc:116`).
    """
    if (cache := http_cache(url)) is not None:
        return ("http", cache.url.rstrip("/"), cache.trusted)
    if (local := file_cache(url)) is not None:
        return ("file", local.path, local.trusted)
    split = urlsplit(url)
    if split.scheme == "unix":
        roots = parse_qs(split.query).get("root")
        return ("unix", Path(split.path), Path(roots[0]) if roots else Path("/"))
    return None


def store_key(store: Store) -> object | None:
    """The `substituter_key` that *store* answers for."""
    if is_http_binary_cache(store):
        return ("http", store.url.rstrip("/"), store.trusted)
    if is_binary_cache(store) and isinstance(path := getattr(store, "path", None), Path):
        return ("file", path, store.trusted)
    if isinstance(store, ExternalUnixStore):
        return ("unix", store.socket_path, store.store_path)
    return None


class ClientSubstituters:
    """The backends for the substituters clients name, made on first use and kept.

    A backend the daemon's own list made already serves a client that names
    the same substituter, with its caches. Each new one has an id derived
    from its URL, so the queue's caches and health stay keyed the same
    across clients.
    """

    def __init__(self, settings: PynixdSettings) -> None:
        self._settings = settings
        self._made: dict[object, Store] = {}
        self._lock = anyio.Lock()

    async def resolve(
        self, urls: Sequence[str], configured: Iterable[tuple[StoreId, Store]]
    ) -> tuple[Store, ...] | None:
        """A backend for each of *urls*, in order, or `None` when one has none.

        *configured* is every store pynixd runs. `None` sends the question to
        the daemon, which reaches every kind of substituter.
        """
        backends = {
            key: store
            for store_id, store in configured
            if store_id != LOCAL_STORE_ID and store.no_schedule and (key := store_key(store)) is not None
        }
        resolved: list[Store] = []
        for url in urls:
            store = await self._backend(url, backends)
            if store is None:
                log.debug("client_substituter_only_the_daemon_reaches", url=url)
                return None
            if store not in resolved:
                resolved.append(store)
        return tuple(resolved)

    async def _backend(self, url: str, backends: dict[object, Store]) -> Store | None:
        key = substituter_key(url)
        if key is None or is_own_cache(url, self._settings):
            return None
        if (store := backends.get(key) or self._made.get(key)) is not None:
            return store
        # Only a new backend waits here: its `start` reads `nix-cache-info`, 30 s for a dead host.
        async with self._lock:
            if (store := self._made.get(key)) is not None:
                return store
            store = self._make(url)
            if store is None:
                return None
            await store.start()
            self._made[key] = store
            return store

    def _make(self, url: str) -> BinaryCacheStore | None:
        cache: HttpCache | FileCache
        if (http := http_cache(url)) is not None:
            cache, name = http, f"client-{http.url.split('://', 1)[1].rstrip('/').replace('/', '-').replace(':', '-')}"
        elif (local := file_cache(url)) is not None:
            if not os.access(local.path, os.R_OK | os.X_OK):
                return None
            cache, name = local, f"client-file-{str(local.path).strip('/').replace('/', '-')}"[:120]
        else:
            return None
        # One id per key: the queue's caches and health are keyed by id.
        store_id = StoreId(f"{name}-trusted" if cache.trusted else name)
        if isinstance(cache, HttpCache):
            return http_spec(cache, store_id).to_store(str(store_id))
        return file_spec(cache, store_id).to_store(str(store_id))

    def stores(self) -> tuple[Store, ...]:
        return tuple(self._made.values())

    async def close(self) -> None:
        made = list(self._made.values())
        self._made.clear()
        for store in made:
            await store.close()
