"""Validity of a path `ctx.output_locations` maps to a backend, through pynixd.

pynixd answered `IsValidPath` and `QueryValidPaths` "valid" from the map alone, while `QueryPathInfo` asked the
backend. A backend that had lost the path made the two disagree: ekn's push read "valid", then failed walking the
closure ("Path ... not found in store closure"; dynhetz, 2026-11-15). Every validity read now asks the backend, and
a mapping the backend no longer holds is dropped.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.serde import IsValidPathRequest, QueryPathInfoRequest, QueryValidPathsRequest, StorePath
from pynixd.store import LocalDBStore, Store
from tests._conftest.import_lab import IA, REFS, client, lab, rows
from tests._conftest.nix_config import for_test_store
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.config import LocalSocketStoreSpec

BUILDER = StoreId("builder")
# In no store: the builder that made it has lost it.
GONE = "/nix/store/2222222222222222222222222222222b-gone"


def _spec(store_id: str, root: Path) -> LocalSocketStoreSpec:
    return make_test_spec(store_id=store_id, store_path=root, no_probe=True, nix_config=for_test_store(substituters=()))


async def test_a_mapped_path_is_valid_only_while_its_backend_holds_it(tmp_path: Path) -> None:
    a, _, _ = await lab(tmp_path)
    [tree] = [ref for referrer, ref in rows(a, REFS) if referrer == IA and ref != IA]
    socket = tmp_path / "pynixd.sock"
    stores: dict[StoreId, Store] = {
        StoreId("local"): LocalDBStore(_spec("local", tmp_path / "b")),
        BUILDER: LocalDBStore(_spec("builder", a)),
    }
    async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None) as server:
        locations = server.ctx.output_locations
        locations[tree] = BUILDER
        locations[GONE] = BUILDER
        if server.ctx.scheduler is not None and server.ctx.scheduler.stores_opened is not None:
            await server.ctx.scheduler.stores_opened.wait()
        async with client(socket) as conn:
            with anyio.fail_after(30):
                held = await conn.call(IsValidPathRequest(path=StorePath(path=tree)), raise_on_error=True)
                gone = await conn.call(IsValidPathRequest(path=StorePath(path=GONE)), raise_on_error=True)
                dropped = [GONE not in locations]
                locations[GONE] = BUILDER
                both = await conn.call(
                    QueryValidPathsRequest(paths={StorePath(path=tree), StorePath(path=GONE)}, substitute=0),
                    raise_on_error=True,
                )
                dropped.append(GONE not in locations)
                locations[GONE] = BUILDER
                info = await conn.call(QueryPathInfoRequest(path=StorePath(path=GONE)), raise_on_error=True)
                dropped.append(GONE not in locations)

        assert held.valid
        assert not gone.valid
        assert {str(p) for p in both.paths} == {tree}
        assert not info.valid
        # The stale mapping is gone after each of the three reads, and the held one stays.
        assert dropped == [True, True, True]
        assert locations.get(tree) == BUILDER
