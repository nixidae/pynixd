"""pynixd names `provenance` only when its store keeps the field (nixidae/nixidae#1250).

A client of Determinate v3.24.0 sends it in `AddToStore` after `repair` (`remote-store.cc:427`), and reads it back in
each path info (`worker-protocol.cc:390`). A store with no daemon keeps it where its database has Determinate's
column (`local-store.cc:679`). The guest's Nix names no `provenance` experimental feature, so the test creates the
column itself, as Determinate's migration does.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import anyio.to_thread

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.connection import Connection
from pynixd.serde import AddToStoreRequest, QueryPathInfoRequest
from pynixd.store import LocalDBStore, Store
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from pynixd_store.create_store import open_local_store
from pynixd_store.store_layout import StoreLayout
from tests._conftest.config import TEST_OP_ROUTES
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

PROVENANCE = '{"type":"flake","flake":{"type":"path","path":"/src"},"subpath":"","next":null}'


async def _session(tmp_path: Path, *, column: bool) -> tuple[bool, str, str]:
    """Add a text file over pynixd's socket and query it back: the feature named, and the two provenances read."""
    store_path = tmp_path / "store"
    store_path.mkdir()
    features = ["nix-command", "provenance"] if column else ["nix-command"]
    # The big lock it returns is held shared; the server's own open takes it exclusively for a migration.
    os.close(
        await anyio.to_thread.run_sync(
            open_local_store, StoreLayout.chroot(store_path), {"experimental-features": {"value": features}}, "2.35.0"
        )
    )
    socket_path = tmp_path / "pynixd.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=store_path,
        no_probe=True,
        daemon=False,
        op_routes={op: "pynixd" for op in TEST_OP_ROUTES},
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(stores=stores, unix_path=socket_path, ssh_port=None, http_port=None):
        link = await open_unix_connection(socket_path)
        conn = Connection(UnixNixReader(link, identifier="test"), UnixNixWriter(link, identifier="test"), "test-prov")
        await conn.connect()
        try:
            named = "provenance" in conn.standard_features
            request = AddToStoreRequest(
                path_name="t",
                cam=ContentAddress(value="text:sha256"),
                references=set(),
                repair=0,
                provenance=PROVENANCE,
            )
            added = await conn.call_with_payload(request, b"provenance\n")
            queried = await conn.call(QueryPathInfoRequest(path=added.info.path), raise_on_error=True)
        finally:
            await conn.close()
    return named, added.info.info.provenance, queried.info.provenance


async def test_a_store_with_the_column_names_and_keeps_provenance(tmp_path: Path) -> None:
    named, added, queried = await _session(tmp_path, column=True)
    assert named
    assert added == PROVENANCE
    assert queried == PROVENANCE


async def test_a_store_without_the_column_does_not_name_it(tmp_path: Path) -> None:
    named, added, queried = await _session(tmp_path, column=False)
    assert not named
    assert added == queried == ""
