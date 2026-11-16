"""A read of a path that only a backend holds, through pynixd. nixidae/nixidae#1358.

`ctx.output_locations` maps the path to a `LocalDBStore` backend, and pynixd answers `QueryPathInfo` and
`NarFromPath` from that backend. A session reads through a synchronous reader of the local store's database; a
backend that read through it found nothing and answered "not valid".

The client speaks the protocol itself, as a remote one does: a `unix://` `nix` reads the store directory and never
sends `NarFromPath` (`tests/differential/test_build_equivalence.py` says why).

No read waits for `stores_opened`: a backend `LocalDBStore` held before its `start()` answered both reads
correctly (measured). Its closure query does need the database (nixidae/nixidae#1356).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.serde import NarFromPathRequest, QueryPathInfoRequest, StorePath, WriteContext
from pynixd.store import LocalDBStore, Store
from tests._conftest.import_lab import IA, REFS, client, lab, nar_of, rows
from tests._conftest.nix_config import for_test_store
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.config import LocalSocketStoreSpec
    from pynixd.connection import Connection

BUILDER = StoreId("builder")


def _spec(store_id: str, root: Path, **update: Any) -> LocalSocketStoreSpec:
    return make_test_spec(
        store_id=store_id, store_path=root, no_probe=True, nix_config=for_test_store(substituters=()), **update
    )


async def _path_info(conn: Connection, path: str) -> bytes:
    info = await conn.call(QueryPathInfoRequest(path=StorePath(path=path)), raise_on_error=True)
    if not info.valid or info.info is None:
        return b"not valid"
    return str(info.info.nar_size).encode()


async def _nar(conn: Connection, path: str, size: int) -> bytes:
    await NarFromPathRequest(path=StorePath(path=path)).to_writer(WriteContext.from_conn(conn))
    await conn.w.drain()
    await conn.r.drain_stderr()
    return await conn.r.readexactly(size)


@pytest.mark.parametrize("op", ["QueryPathInfo", "NarFromPath"])
async def test_a_read_of_a_path_a_backend_holds_answers_from_that_backend(tmp_path: Path, op: str) -> None:
    a, _, _ = await lab(tmp_path)
    [tree] = [ref for referrer, ref in rows(a, REFS) if referrer == IA and ref != IA]
    nar = nar_of(a, tree)
    socket = tmp_path / "pynixd.sock"
    stores: dict[StoreId, Store] = {
        StoreId("local"): LocalDBStore(_spec("local", tmp_path / "b")),
        # On SQLite: Nix's CLI fills the backend's store.
        BUILDER: LocalDBStore(_spec("builder", a, postgres=None)),
    }
    async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None) as server:
        server.ctx.output_locations[tree] = BUILDER
        if server.ctx.scheduler is not None and server.ctx.scheduler.stores_opened is not None:
            await server.ctx.scheduler.stores_opened.wait()
        # The backend's path-info cache would answer first, and hide which database the read goes to.
        stores[BUILDER].forget_path_info(tree)
        async with client(socket) as conn:
            with anyio.fail_after(30):
                read = await (_path_info(conn, tree) if op == "QueryPathInfo" else _nar(conn, tree, len(nar)))

    assert read == (str(len(nar)).encode() if op == "QueryPathInfo" else nar)
