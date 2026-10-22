"""Daemon-backed store for an existing local Unix socket."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from nix_daemon_protocol.store_dir import on_disk

from ..connection import Connection
from ..drv_parser import drv_text, parse_drv
from ..store_path import StorePath
from ..unix_socket import open_unix_connection
from ..wire import UnixNixReader, UnixNixWriter
from .daemon import DaemonStore

if TYPE_CHECKING:
    from ..config import ExternalUnixStoreSpec
    from ..drv_parser import Derivation


class ExternalUnixStore(DaemonStore):
    """Connect to an already-running Nix daemon over a Unix socket.

    This is used for ``unix://...`` substituters. It never starts, stops, or
    schedules builds on the daemon; it only exposes store queries and NAR reads
    through the normal daemon store interface.
    """

    def __init__(self, spec: ExternalUnixStoreSpec) -> None:
        """Configure external Unix socket connection."""
        super().__init__(spec)
        self.socket_path = spec.socket_path
        self.monitor_enabled = spec.monitor
        self.monitor = None

    async def create_conn(self) -> Connection:
        """Create a connection by connecting to the external Unix socket."""
        conn_id = f"{self.store_id}-{self.conn_counter}"
        r, w = await open_unix_connection(self.socket_path)
        conn = Connection(
            UnixNixReader(r, identifier=conn_id),
            UnixNixWriter(w, identifier=conn_id),
            conn_id,
            store_path=self.store_path,
        )
        await conn.connect()
        return conn

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        """Fast-path: read .drv file directly from the store filesystem."""

        # `on_disk` and not `self.store_path / "nix" / "store" / str(sp)`.
        # `str(sp)` is an absolute path, and pathlib drops every part in front
        # of an absolute one, so that read went to the store of the machine.
        # See `read_drv_file` in `drv_parser.py` for what the mistake cost.
        drv_file = Path(on_disk(str(StorePath(str(drv_store_path)))))
        try:
            contents = drv_file.read_bytes()
        except (FileNotFoundError, OSError):
            return await super().read_derivation(drv_store_path)

        return parse_drv(drv_text(contents))
