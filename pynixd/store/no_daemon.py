"""The daemon side of a daemonless local store: the store's facts, and no daemon (nixidae/nixidae#1155).

`LocalDBStore` holds a `LocalStore` in a mode with a daemon and a `NoDaemonStore`
in the `daemonless` mode (`pynixd/store_mode.py`). Both are a `LocalHost`: the
layout, the monitor, nix.conf and what the store builds. This one has no
connection pool, no reconnect loop and no probe, and refuses every operation it
would send to a daemon.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.constants import FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS, FEATURE_REALISATION_WITH_PATH
from pynixd_store.nix_config import nix_at_least, nix_environment, read_nix_version

from ..exceptions import NoDaemonError
from .local_host import LocalHost

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from nix_daemon_protocol.wire_ops import WireRequest

    from ..config import LocalSocketStoreSpec
    from ..connection import ClientConn, Connection
    from ..serde import SetOptionsRequest

__all__ = ["NoDaemonStore"]

_NO_DAEMON_FEATURES = frozenset({FEATURE_REALISATION_WITH_PATH, FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS})
"""What a store with no daemon offers: `WorkerProto::latest` of Nix 2.35 (`worker-protocol.cc:20`).

`open_local_store` gives a store of Nix 2.35 its schema, so its realisations are
in `BuildTraceV3`, keyed by derivation path. A store of Nix 2.34 offers none: its
schema has the `Realisations` table, keyed by the derivation's hash.
"""


class NoDaemonStore(LocalHost):
    def __init__(self, spec: LocalSocketStoreSpec) -> None:
        super().__init__(spec)
        self._features = set(_NO_DAEMON_FEATURES)

    async def _prepare(self) -> None:
        self.nix_version = await read_nix_version(self.nix_bin, nix_environment(self.layout, self.extra_env))
        # `WorkerProto::latest` of Nix 2.34 has no features (`worker-protocol.cc:17` of 2.34).
        if not nix_at_least(self.nix_version, 2, 35):
            self._features = set()

    async def ensure_daemon(self) -> None:
        """There is no daemon to start or reach."""

    def _refuse(self, what: str) -> NoDaemonError:
        return NoDaemonError(str(self.store_id), what)

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        raise self._refuse(type(request).__name__)

    async def execute(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        skip_probe: bool = False,
        mark: bool = True,
    ) -> Any:
        if not skip_probe:
            await self.probe()
        raise self._refuse(type(request).__name__)

    async def create_conn(self) -> Connection:
        raise self._refuse("a connection to the daemon")

    @contextlib.asynccontextmanager
    async def build_conn(self, options: SetOptionsRequest | None = None) -> AsyncIterator[Connection]:
        raise self._refuse("a connection to the daemon")
        yield  # pragma: no cover  # an async generator needs a yield; the raise comes first

    @contextlib.asynccontextmanager
    async def transfer_conn(self, options: SetOptionsRequest | None = None) -> AsyncIterator[Connection]:
        raise self._refuse("a connection to the daemon")
        yield  # pragma: no cover  # an async generator needs a yield; the raise comes first

    async def answer_handshake(self) -> None:
        raise self._refuse("a connection to the daemon")

    async def sign_path_info(
        self, request: Any, client: Any = None, suppress_last: bool = False, conn: Connection | None = None
    ) -> Any:
        raise self._refuse(type(request).__name__)

    async def is_valid_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        raise self._refuse(type(request).__name__)

    async def query_path_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        raise self._refuse(type(request).__name__)

    async def query_path_from_hash_part(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        raise self._refuse(type(request).__name__)

    async def query_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        raise self._refuse(type(request).__name__)

    async def nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        raise self._refuse(type(request).__name__)
