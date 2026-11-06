"""What a build in pynixd's own sandbox used reaches its observation (nixidae/nixidae#1187)."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from pynixd_store.db_migrations import BUILD_OBSERVATION_TABLE
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

# Busy for a while in the shell itself: the CPU time is the builder's, not a child's it never reaped.
SPIN = (
    'derivation { name = "usage-spin"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "i=0; while [ $i -lt 500000 ]; do i=$((i+1)); done; echo $i > $out" ]; }'
)


async def test_a_build_in_the_sandbox_records_its_cpu_time(tmp_path: Path) -> None:
    root = tmp_path / "store"
    socket = tmp_path / "pynixd.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        op_routes={"BuildDerivation": OpRoute.pynixd},
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        await run_subproc(
            [NIX_BIN, "build", "--store", f"unix://{socket}?root={root}", "--no-link", "--impure", "--expr", SPIN]
        )

    with closing(sqlite3.connect(root / "nix/var/nix/db/db.sqlite")) as conn:
        rows = conn.execute(
            f"SELECT cpuUserUs, cpuSystemUs FROM {BUILD_OBSERVATION_TABLE} WHERE name = 'usage-spin'"
        ).fetchall()

    assert len(rows) == 1
    user_us, system_us = rows[0]
    assert user_us > 50_000, "500000 shell iterations take well over 50 ms of user time"
    assert system_us is not None
