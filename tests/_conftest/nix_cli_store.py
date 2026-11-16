"""The store that Nix's CLI opens for a test store (nixidae/nixidae#1209).

Nix's CLI on a store directory reads its `db.sqlite`. A store of the PostgreSQL lane keeps its paths on the lane's
server, so the CLI reaches that store through a pynixd of its own, over the same database.
"""

from __future__ import annotations

import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store.local_db import LocalDBStore
from tests._conftest.config import make_test_spec
from tests._conftest.nix_config import for_test_store
from tests._conftest.postgres import is_lane_store, lane_postgres

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@asynccontextmanager
async def nix_cli_store(root: Path, **config: Any) -> AsyncIterator[str]:
    """The `--store` argument for Nix's CLI on the store at *root*: *root*, or a pynixd over it on the lane.

    That pynixd has no substituter; *config* goes to `for_test_store`.
    """
    if not is_lane_store(root):
        yield str(root)
        return
    # Not `tmp_path`: a unix socket path holds at most 107 bytes.
    with tempfile.TemporaryDirectory(prefix="pynixd-cli-") as directory:
        socket = Path(directory) / "sock"
        spec = make_test_spec(
            store_id="local",
            store_path=root,
            no_probe=True,
            daemon=False,
            postgres=lane_postgres(root),
            nix_config=for_test_store(substituters=(), **config),
        )
        async with Server(
            stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None
        ):
            yield f"unix://{socket}?root={root}"
