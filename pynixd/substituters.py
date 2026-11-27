"""Substituter URLs, and the backends that stand for the ones a client names.

A client's `substituters` setting replaces the daemon's list: the Nix client
folds `extra-substituters` into it before it sends `SetOptions`
(`Config::set`, `configuration.cc:27`). For an untrusted client the handler
has already kept only the trusted ones (`trust.restrict_overrides`).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd_store.background import Background

from .config import BinaryCacheSpecBase
from .cooldown import Cooldown
from .exceptions import CacheUnopened, StoreDirMismatch
from .store_url import reference_key, substituter_spec

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from contextlib import AbstractAsyncContextManager

    from anyio.abc import TaskStatus

    from .config import PynixdSettings
    from .store import Store

log = structlog.get_logger(__name__)


class _StartFailed(Exception):
    """A backend whose start failed on a store with no daemon: it is left out while it cools down."""


class ClientSubstituters:
    """The backends for the substituters clients name, made on first use and kept.

    A backend the daemon's own list made already serves a client that names
    the same substituter, with its caches. Each new one has an id derived
    from its URL, so the queue's caches and health stay keyed the same
    across clients.
    """

    def __init__(
        self,
        settings: PynixdSettings,
        *,
        now: Callable[[], float] = time.monotonic,
        runs_daemon: Callable[[], bool] = lambda: True,
    ) -> None:
        """*runs_daemon* answers whether a Nix daemon takes the ops a backend cannot; it is asked at a failed start."""
        self._settings = settings
        self._now = now
        self._runs_daemon = runs_daemon
        self._made: dict[object, Store] = {}
        self._dropped: set[object] = set()
        self._unopened: dict[object, Cooldown] = {}
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
            key = reference_key(url)
            if key in self._dropped:
                continue
            if (cooldown := self._unopened.get(key)) is not None and cooldown.cooling:
                continue
            try:
                store = await self._backend(url, backends)
            except (StoreDirMismatch, CacheUnopened, _StartFailed):
                continue
            if store is None:
                log.debug("client_substituter_only_the_daemon_reaches", url=url)
                return None
            if not store.opened:
                continue
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
            # `substituter_spec` asks whether a `file://` directory is readable.
            spec = await anyio.to_thread.run_sync(lambda: substituter_spec(url, self._settings, prefix="client-"))
            if spec is None:
                return None
            if isinstance(spec, BinaryCacheSpecBase):
                spec = spec.model_copy(update={"drop_unopened": True})
            store = spec.to_store(str(spec.store_id))
            try:
                await self._held.start(self._hold, key, store, url)
            except StoreDirMismatch:
                # Nix's own refusal (`binary-cache-store.cc:58`): `getDefaultSubstituters` drops the cache
                # with a warning (`store-registration.cc:88`), so the daemon answers without it too.
                # NIX-DEFECT (#23): `HttpBinaryCacheStore::init` (`http-binary-cache-store.cc:69`) skips that
                # check on a hit in the narinfo disk cache, whose row holds the StoreDir unread, so Nix keeps
                # the cache once any client of `/nix/store` has used it. pynixd checks every time; the
                # cache's answers are for paths of another store. nixidae/nixidae#1241.
                log.warning("client_substituter_dropped", url=url, exc_info=True)
                self._dropped.add(key)
                raise
            except CacheUnopened:
                # NIX-DEVIATION (#27): `init` throws (`binary-cache-store.cc:48`), and `getDefaultSubstituters`
                # drops the cache with a warning (`store-registration.cc:88`). Nothing remembers that: each Nix
                # daemon worker opens the cache again, 30 s for a dead host. pynixd leaves the cache out for
                # every client while a `Cooldown` runs, and opens it again after. Measure how often a cache
                # opens on the first try after its cooldown to reverse it. nixidae/nixidae#1257.
                cooldown = self._unopened.setdefault(key, Cooldown(now=self._now))
                log.warning("client_substituter_unopened", url=url, retry_in=cooldown.failed(), exc_info=True)
                raise
            except Exception as e:
                # Another store opens without a connection: `RemoteStore`'s pool connects on first use
                # (`remote-store.cc:35`), so Nix keeps a dead ssh-ng host. The daemon answers for this one.
                if self._runs_daemon():
                    log.warning("client_substituter_start_failed", url=url, exc_info=True)
                    return None
                # NIX-DEVIATION (#27): Nix keeps the dead store (`remote-store.cc:35`), and each op fails when it
                # uses it. pynixd has no daemon here to answer for it, so `None` would fail every substituting op
                # with `NoDaemonError`. It leaves the backend out for every client while a `Cooldown` runs, and
                # starts it again after: the op answers without that substituter. Measure how often a start
                # succeeds on the first try after its cooldown to reverse it. nixidae/nixidae#1665.
                cooldown = self._unopened.setdefault(key, Cooldown(now=self._now))
                log.warning("client_substituter_start_failed", url=url, retry_in=cooldown.failed(), exc_info=True)
                raise _StartFailed(url) from e
            self._unopened.pop(key, None)
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

    def start_failed(self, url: str) -> float:
        """Count a failed start of a backend for *url* made elsewhere, and return the wait before one starts here."""
        return self._unopened.setdefault(reference_key(url), Cooldown(now=self._now)).failed()

    def stores(self) -> tuple[Store, ...]:
        return tuple(self._made.values())

    async def close(self) -> None:
        await self._held.aclose()
        self._made.clear()
