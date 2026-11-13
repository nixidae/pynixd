"""The daemon side of a daemonless local store: the store's facts, and no daemon (nixidae/nixidae#1155).

`LocalDBStore` holds a `LocalStore` in a mode with a daemon and a `NoDaemonStore`
in the `daemonless` mode (`pynixd/store_mode.py`). Both give the layout, the
monitor and what the store builds; this one reads the Nix version and nix.conf
instead of a daemon, and refuses every operation it would send to one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.constants import FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS, FEATURE_REALISATION_WITH_PATH
from pynixd_store.nix_config import nix_at_least, nix_environment, read_nix_version

from ..exceptions import NoDaemonError
from .daemon import ProbeState
from .local_daemon import LocalStore

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireRequest

    from ..config import LocalSocketStoreSpec
    from ..connection import ClientConn, Connection

__all__ = ["NoDaemonStore"]

_NO_DAEMON_FEATURES = frozenset({FEATURE_REALISATION_WITH_PATH, FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS})
"""What a store with no daemon offers: `WorkerProto::latest` of Nix 2.35 (`worker-protocol.cc:20`).

`open_local_store` gives a store of Nix 2.35 its schema, so its realisations are
in `BuildTraceV3`, keyed by derivation path. A store of Nix 2.34 offers none: its
schema has the `Realisations` table, keyed by the derivation's hash.
"""


class NoDaemonStore(LocalStore):
    runs_daemon = False

    def __init__(self, spec: LocalSocketStoreSpec) -> None:
        super().__init__(spec)
        self._features = set(_NO_DAEMON_FEATURES)

    async def _prepare(self) -> None:
        self.nix_version = await read_nix_version(self.nix_bin, nix_environment(self.layout, self.extra_env))
        # `WorkerProto::latest` of Nix 2.34 has no features (`worker-protocol.cc:17` of 2.34).
        if not nix_at_least(self.nix_version, 2, 35):
            self._features = set()

    @property
    def _features_from_config(self) -> bool:
        return True

    async def ensure_daemon(self) -> None:
        """There is no daemon to start or reach."""

    async def probe(self) -> None:
        """There is nothing to probe: `start` read what the store builds from nix.conf."""
        self.probe_state = ProbeState.PROBED
        self._probe_event.set()

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        raise NoDaemonError(str(self.store_id), type(request).__name__)

    async def create_conn(self) -> Connection:
        raise NoDaemonError(str(self.store_id), "a connection to the daemon")
