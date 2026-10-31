"""Batch temporary roots (op 49). Issue #66.

`copyPaths` of Nix pins the destination set with `AddTempRoots` before
every copy, and the feature gate has no fallback: a daemon that does not
name `addTempRoots` makes new clients silently hold no roots at all.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.connection import Connection
from pynixd.serde import AddTempRootsRequest, StorePath
from pynixd.store import LocalSocketStore
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path


async def test_add_temp_roots_batch(tmp_path: Path) -> None:
    """Op 49 holds every path of the set for the session that sent it."""
    store_path = tmp_path / "store"
    store_path.mkdir()
    socket_path = tmp_path / "pynixd.sock"

    local_store = LocalSocketStore(
        make_test_spec(store_id="local", store_path=store_path, no_probe=True),
    )

    async with Server(
        stores={StoreId("local"): local_store},
        unix_path=socket_path,
        ssh_port=None,
        http_port=None,
    ):
        link = await open_unix_connection(socket_path)
        conn = Connection(
            UnixNixReader(link, identifier="test"),
            UnixNixWriter(link, identifier="test"),
            "test-temp-roots",
        )
        await conn.connect()

        # The greeting names the feature, or a new client silently holds nothing.
        assert "addTempRoots" in conn.standard_features

        paths = {
            StorePath(path="/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-batch-one"),
            StorePath(path="/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-batch-two"),
        }
        resp = await conn.call(AddTempRootsRequest(paths=paths), raise_on_error=True)
        assert resp.value == 1

        roots_dir = store_path / "nix" / "var" / "nix" / "temproots"
        held = "".join(path.read_text() for path in roots_dir.glob("pynixd-*"))
        for path in paths:
            assert str(path) in held
        await conn.close()
