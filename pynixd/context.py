"""
Shared application context for pynixd dependencies.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId

from ._lazy import ssh_errors
from .exceptions import BackendError, InfrastructureError, NoDaemonError
from .serde import QueryValidPathsRequest
from .session_options import DaemonHeader
from .sessions import ClientSessions
from .tracing import NO_TRACING, Tracing
from .trust import TrustPolicy

if TYPE_CHECKING:
    from collections.abc import Mapping

    from nix_daemon_protocol.store_path import StorePath
    from pynixd_store.local_store_db import LocalStoreDB

    from .config import PynixdSettings
    from .connection import ClientConn
    from .provisioning.provisioner import Provisioner
    from .scheduler import Scheduler
    from .session_builders import SessionBuilders
    from .store.base import Store
    from .store.daemon import DaemonStore
    from .store.local_daemon import LocalStore
    from .store.local_db import LocalDBStore


log = structlog.get_logger(__name__)

OUTPUT_LOCATIONS_KEPT = 100_000
"""How many backend locations pynixd remembers: about 20 MB at 200 bytes an entry."""


class OutputLocations(OrderedDict[str, StoreId]):
    """Where each pulled output lives, the newest `OUTPUT_LOCATIONS_KEPT` of them.

    Every output pynixd ever pulled stayed here, so the map grew for the life
    of the daemon. The local store holds a pulled path anyway; the entry only
    serves a path the local collector removed since. nixidae/nixidae#1003.
    """

    def __setitem__(self, key: str, value: StoreId) -> None:
        super().__setitem__(key, value)
        self.move_to_end(key)
        while len(self) > OUTPUT_LOCATIONS_KEPT:
            self.popitem(last=False)


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
    session_builders: SessionBuilders | None = None
    """The stores of trusted clients' `builders`; `Server` sets it."""
    provisioner: Provisioner | None = None
    """The builders it launches, for `pynixd state`; set when `provisioning.pools` is."""
    trust: TrustPolicy = field(default_factory=TrustPolicy)
    """Who may connect over the Unix listener, and who is trusted.

    `Server.start` replaces the default with what the local store's Nix reads.
    """
    daemon_header: DaemonHeader = field(default_factory=DaemonHeader)
    """The header settings the local store's Nix has, under a remote client's `SetOptions`; `Server.start` reads it."""
    tracing: Tracing = NO_TRACING
    """The spans of client operations; `Server.start` sets it when `tracing.endpoint` is set."""
    sessions: ClientSessions = field(default_factory=ClientSessions)
    """Established client sessions, for the shutdown path.

    `DaemonProxy.run` registers each session on entry and unregisters on
    exit; `Server.close` answers and closes them after closing the
    listeners. Issue #64.
    """
    output_locations: dict[str, StoreId] = field(default_factory=OutputLocations)
    """Where each output a backend built now lives.

    A fleet build leaves its outputs on the backend that ran it, and pynixd
    serves reads of those paths from there. Every part of pynixd that asks
    "does this path exist" must consult this map as well as the local store,
    or it reports a path that pynixd can serve as missing.
    """

    def store_for_output_path(self, path: str) -> DaemonStore | LocalDBStore | None:
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
        return None if store is None else store.output_source

    async def backend_holds(self, paths: set[StorePath], client: ClientConn | None = None) -> set[StorePath]:
        """Those of *paths* the backend `output_locations` names for each still holds.

        A mapping the backend denies is dropped. Nothing answers "valid" from the map alone: a backend that lost
        the path made `IsValidPath` say "valid" while `QueryPathInfo` said "not valid" (nixidae/nixidae#1381),
        and a goal read success and failed on the first read (nixidae/nixidae#1382).
        """
        by_store: dict[int, tuple[DaemonStore | LocalDBStore, set[StorePath]]] = {}
        for path in paths:
            store = self.store_for_output_path(str(path))
            if store is not None:
                by_store.setdefault(id(store), (store, set()))[1].add(path)
        held: set[StorePath] = set()
        for store, asked in by_store.values():
            try:
                resp = await store.execute(QueryValidPathsRequest(paths=asked, substitute=0), client=client)
            except (
                TimeoutError,
                BackendError,
                InfrastructureError,
                NoDaemonError,
                OSError,
                EOFError,
                *ssh_errors(),
            ) as error:
                # Unreachable is not "lost": the mapping stays for a later read.
                log.warning("output_location_unreachable", store_id=store.store_id, error=str(error))
                continue
            found = set(getattr(resp, "paths", set()))
            held |= found
            for path in asked - found:
                self.forget_output(str(path), store)
        return held

    def forget_output(self, path: str, store: DaemonStore | LocalDBStore) -> None:
        # Only this store's mapping: a build elsewhere may have remapped the
        # path while the read waited.
        if self.output_locations.get(path) != store.store_id:
            return
        log.info("output_location_stale", path=path, store_id=store.store_id)
        del self.output_locations[path]

    @property
    def local_store(self) -> LocalStore | LocalDBStore:
        """The primary local Nix daemon store for this context."""
        return self._stores[LOCAL_STORE_ID]  # type: ignore[return-value]

    @property
    def stores(self) -> Mapping[StoreId, Store]:
        """Read-only view of all connected stores (including local)."""
        return self._stores
