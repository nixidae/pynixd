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
from pynixd_store.background import Background

from .store_url import reference_key, substituter_spec

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from contextlib import AbstractAsyncContextManager

    from anyio.abc import TaskStatus

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
        self._held = Background("ClientSubstituters")

    def running(self) -> AbstractAsyncContextManager[None]:
        """Hold the backends open for the body of this block; outside it none is made."""
        return self._held.running()

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
            try:
                await self._held.start(self._hold, key, store, url)
            except Exception:
                # Nix drops a substituter it cannot open, with a warning
                # (`store-registration.cc:114`); the daemon answers for this one.
                log.warning("client_substituter_start_failed", url=url, exc_info=True)
                return None
            self._made[key] = store
            return store

    async def _hold(self, key: object, store: Store, url: str, *, task_status: TaskStatus[None]) -> None:
        """Keep *store* open, its loops in its own `async with`, as `Server._host_store` does.

        A failure before the start raises to `_backend`. One after it drops
        this backend alone: a group would cancel every other one.
        """
        started = False
        try:
            async with store:
                started = True
                task_status.started()
                await anyio.sleep_forever()
        except Exception:
            if not started:
                raise
            log.exception("client_substituter_failed", url=url)
        finally:
            if self._made.get(key) is store:
                del self._made[key]

    def stores(self) -> tuple[Store, ...]:
        return tuple(self._made.values())

    async def close(self) -> None:
        await self._held.aclose()
        self._made.clear()
