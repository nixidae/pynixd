"""
Shared application context for pynixd dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId

from .sessions import ClientSessions
from .store.daemon import DaemonStore
from .trust import TrustPolicy

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .config import PynixdSettings
    from .local_store_db import LocalStoreDB
    from .scheduler import Scheduler
    from .store.base import Store
    from .store.local_daemon import LocalStore


@dataclass
class PynixdContext:
    """Encapsulates shared services and state for dependency injection.

    All stores (including local) live in ``_stores``. The ``local_store``
    property looks up ``StoreId(\"local\")`` in the dict.
    """

    settings: PynixdSettings
    _stores: dict[StoreId, Store]
    db: LocalStoreDB | None = None
    scheduler: Scheduler | None = None
    trust: TrustPolicy = field(default_factory=TrustPolicy)
    """Who may connect over the Unix listener, and who is trusted.

    `Server.start` replaces the default with what the local store's Nix reads.
    """
    sessions: ClientSessions = field(default_factory=ClientSessions)
    """Established client sessions, for the shutdown path.

    `DaemonProxy.run` registers each session on entry and unregisters on
    exit; `Server.close` answers and closes them after closing the
    listeners. Issue #64.
    """
    output_locations: dict[str, StoreId] = field(default_factory=dict)
    """Where each output a backend built now lives.

    A fleet build leaves its outputs on the backend that ran it, and pynixd
    serves reads of those paths from there. Every part of pynixd that asks
    "does this path exist" must consult this map as well as the local store,
    or it reports a path that pynixd can serve as missing.
    """

    def store_for_output_path(self, path: str) -> DaemonStore | None:
        """The backend that holds *path*, or `None` when no backend does.

        `None` means only that this map does not know the path. The local
        store may still hold it, and the caller asks the local store itself.

        This lives on the context, and not on `DaemonProxy`, because both the
        proxy and the goal engine have to ask the question. It was a method of
        the proxy alone, so `EnsureDerivedPathGoal` could not reach it and
        reported every backend-resident path as invalid -- issue nixidae/nixidae#199.
        """
        store_id = self.output_locations.get(path)
        if store_id is None:
            return None
        store = self._stores.get(store_id)
        if not isinstance(store, DaemonStore):
            return None
        return store

    @property
    def local_store(self) -> LocalStore:
        """The primary local Nix daemon store for this context."""
        return self._stores[LOCAL_STORE_ID]  # type: ignore[return-value]

    @property
    def stores(self) -> Mapping[StoreId, Store]:
        """Read-only view of all connected stores (including local)."""
        return self._stores
