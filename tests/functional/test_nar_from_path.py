"""pynixd's dump of a store path is the NAR Nix dumps of it, byte for byte.

`nix store dump-path` calls `Store::narFromPath` (`store-api.cc:385`) on the
lab store, and `LocalDBStore.nar_pieces` dumps the same files in pynixd. The
path holds every node shape: a directory, an executable and a plain file, an
empty file, a symlink, and a file over the 64 KiB piece size.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import anyio

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.serde import StorePath
from pynixd.store import LocalDBStore, Store
from tests.conftest import NIX_BIN, make_test_spec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


@asynccontextmanager
async def _pynixd(store_path: Path) -> AsyncIterator[Server]:
    spec = make_test_spec(store_id="local", store_path=store_path, no_probe=True)
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(stores=stores, ssh_port=None, http_port=None) as server:
        yield server


async def _nix(*args: str) -> bytes:
    result = await anyio.run_process([str(NIX_BIN), *args], check=True)
    return result.stdout


async def test_pynixd_dumps_what_nix_dumps(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    (tree / "bin").mkdir(parents=True)
    (tree / "bin" / "hello").write_bytes(b"#!/bin/sh\necho hi\n")  # noqa: ASYNC240 -- test setup
    (tree / "bin" / "hello").chmod(0o755)
    (tree / "big").write_bytes(bytes(range(256)) * 600)  # noqa: ASYNC240 -- test setup
    (tree / "empty").write_bytes(b"")  # noqa: ASYNC240 -- test setup
    (tree / "link").symlink_to("bin/hello")
    store_path = tmp_path / "store"
    store_path.mkdir()
    added = (await _nix("store", "add-path", "--store", str(store_path), str(tree))).decode().strip()

    nix_dump = await _nix("store", "dump-path", "--store", str(store_path), added)
    async with _pynixd(store_path) as server:
        local = server.ctx.local_store
        assert isinstance(local, LocalDBStore)
        ours = b"".join([piece async for piece in local.nar_pieces(StorePath(path=added))])

    assert ours == nix_dump
