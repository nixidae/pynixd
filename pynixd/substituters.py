"""Substituter URLs, and the backends that stand for the ones a client names.

A client's `substituters` setting replaces the daemon's list: the Nix client
folds `extra-substituters` into it before it sends `SetOptions`
(`Config::set`, `configuration.cc:27`). For an untrusted client the handler
has already kept only the trusted ones (`trust.restrict_overrides`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId

from .store_url import reference_key, substituter_spec

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from .config import PynixdSettings
    from .store import Store

log = structlog.get_logger(__name__)


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
            if store_id != LOCAL_STORE_ID and store.no_schedule and (key := store.reference_key) is not None
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
        key = reference_key(url)
        if key is None:
            return None
        if (store := backends.get(key) or self._made.get(key)) is not None:
            return store
        # Only a new backend waits here: its `start` reads `nix-cache-info`, 30 s for a dead host.
        async with self._lock:
            if (store := self._made.get(key)) is not None:
                return store
            spec = substituter_spec(url, self._settings, prefix="client-")
            if spec is None:
                return None
            store = spec.to_store(str(spec.store_id))
            await store.start()
            self._made[key] = store
            return store

    def stores(self) -> tuple[Store, ...]:
        return tuple(self._made.values())

    async def close(self) -> None:
        made = list(self._made.values())
        self._made.clear()
        for store in made:
            await store.close()
