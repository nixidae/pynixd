"""pynixd names `open-telemetry` and reads each `traceparent` a client sends (nixidae/nixidae#1250).

A client of Determinate v3.24.0 sends one after the handshake (`remote-store.cc:115`) and one after each op code
(`worker-protocol-connection.cc:261`). An unread string shifts every field after it.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.connection import Connection
from pynixd.serde import IsValidPathRequest, StorePath
from pynixd.store import LocalSocketStore
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


async def test_a_traced_session_reads_each_operation_whole(tmp_path: Path) -> None:
    store_path = tmp_path / "store"
    store_path.mkdir()
    socket_path = tmp_path / "pynixd.sock"
    local_store = LocalSocketStore(make_test_spec(store_id="local", store_path=store_path, no_probe=True))

    async with Server(stores={StoreId("local"): local_store}, unix_path=socket_path, ssh_port=None, http_port=None):
        link = await open_unix_connection(socket_path)
        conn = Connection(UnixNixReader(link, identifier="test"), UnixNixWriter(link, identifier="test"), "test-otel")
        await conn.connect()
        assert "open-telemetry" in conn.standard_features
        client = cast("Any", SimpleNamespace(traceparent=TRACEPARENT, options=None))

        absent = StorePath(path="/nix/store/00000000000000000000000000000000-absent")
        for _ in range(2):
            resp = await conn.call(IsValidPathRequest(path=absent), client=client, raise_on_error=True)
            assert not resp.valid
        await conn.close()
