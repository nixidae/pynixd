"""pynixd names `provenance` only when its store keeps the field (nixidae/nixidae#1250).

A client of Determinate v3.24.0 sends it in `AddToStore` after `repair` (`remote-store.cc:427`), and reads it back in
each path info (`worker-protocol.cc:390`). Op 44 carries it only under `versionedAddToStoreMultiple`
(`daemon.cc:552`). A store with no daemon keeps it where its database has Determinate's column
(`local-store.cc:679`). The guest's Nix names no `provenance` experimental feature, so the test creates the column
itself, as Determinate's migration does.
"""

from __future__ import annotations

import hashlib
import os
from typing import TYPE_CHECKING

import anyio.to_thread

from nix_daemon_protocol.add_multiple_to_store import (
    AddMultipleToStoreRequest,
    AddMultipleToStoreResponse,
    path_info_version,
)
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd import Server
from pynixd.connection import Connection
from pynixd.serde import AddToStoreRequest, QueryPathInfoRequest
from pynixd.serde.context import ReadContext
from pynixd.store import LocalDBStore, Store
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from pynixd_store.content_address import ContentAddress as StoreContentAddress, Method, content_addressed_path
from pynixd_store.create_store import open_local_store
from pynixd_store.nar_fs import dump_path
from pynixd_store.store_layout import StoreLayout
from tests._conftest.config import TEST_OP_ROUTES
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

PROVENANCE = '{"type":"flake","flake":{"type":"path","path":"/src"},"subpath":"","next":null}'


async def _add_multiple(conn: Connection, tmp_path: Path) -> str:
    """Add one path with op 44 in the shape `remote-store.cc:589` writes, and answer the provenance it reads back."""
    source = tmp_path / "multiple"
    source.write_bytes(b"added with op 44\n")
    nar = b"".join(dump_path(source))
    digest = hashlib.sha256(nar).digest()
    ca = StoreContentAddress(Method.NAR, "sha256", digest)
    info = ValidPathInfo(
        path=StorePath(path=content_addressed_path(ca, "multiple", (), "/nix/store")),
        info=UnkeyedValidPathInfo(
            nar_hash=NARHash(hash=digest.hex()),
            references=set(),
            registration_time=Time(ts=0),
            nar_size=len(nar),
            ultimate=False,
            sigs=set(),
            ca=ContentAddress(value=ca.render()),
            provenance=PROVENANCE,
        ),
    )
    await conn.send_request(AddMultipleToStoreRequest(repair=0, dont_check_sigs=0))
    framed = conn.w.framed()
    framed.write_uint64(1)
    framed.write(await info.bytes_wire(*path_info_version(conn.version, conn.standard_features)))
    framed.write(nar)
    await framed.finalize()
    await AddMultipleToStoreResponse.from_reader(ReadContext.from_conn(conn))
    queried = await conn.call(QueryPathInfoRequest(path=info.path), raise_on_error=True)
    provenance: str = queried.info.provenance
    return provenance


async def _session(tmp_path: Path, *, column: bool) -> tuple[bool, str, str, str]:
    """Add over pynixd's socket and query back: the feature named, and the provenances read after op 7 and op 44."""
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
            multiple = await _add_multiple(conn, tmp_path)
        finally:
            await conn.close()
    return named, added.info.info.provenance, queried.info.provenance, multiple


async def test_a_store_with_the_column_names_and_keeps_provenance(tmp_path: Path) -> None:
    named, added, queried, multiple = await _session(tmp_path, column=True)
    assert named
    assert added == PROVENANCE
    assert queried == PROVENANCE
    assert multiple == PROVENANCE


async def test_a_store_without_the_column_does_not_name_it(tmp_path: Path) -> None:
    named, added, queried, multiple = await _session(tmp_path, column=False)
    assert not named
    assert added == queried == multiple == ""
