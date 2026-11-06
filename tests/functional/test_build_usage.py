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
STRING_BYTES = 1 << 24
PROCESSES = 4
# Each subshell grows a 16 MiB string, then spins so that all of them hold theirs at once.
# `wait4` reports the largest single process; a cgroup counts them together.
PARALLEL = (
    'derivation { name = "usage-parallel"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "for n in 1 2 3 4; do (s=a; i=0; while [ $i -lt 24 ]; do s=$s$s; i=$((i+1)); done; '
    'j=0; while [ $j -lt 300000 ]; do j=$((j+1)); done) & done; wait; echo > $out" ]; }'
)


async def _build(tmp_path: Path, expr: str, name: str) -> tuple[object, ...]:
    """Build *expr* in pynixd's own sandbox; answer its observation's resource columns."""
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
            [NIX_BIN, "build", "--store", f"unix://{socket}?root={root}", "--no-link", "--impure", "--expr", expr]
        )

    with closing(sqlite3.connect(root / "nix/var/nix/db/db.sqlite")) as conn:
        rows = conn.execute(
            "SELECT cpuUserUs, cpuSystemUs, resourceSource, peakMemoryBytes, ioReadBytes, ioWriteBytes "
            f"FROM {BUILD_OBSERVATION_TABLE} WHERE name = ?",
            (name,),
        ).fetchall()
    assert len(rows) == 1
    return tuple(rows[0])


async def test_a_build_in_the_sandbox_records_its_cpu_time(tmp_path: Path) -> None:
    user_us, system_us, source, peak, read, written = await _build(tmp_path, SPIN, "usage-spin")
    assert isinstance(user_us, int)
    assert user_us > 50_000, "500000 shell iterations take well over 50 ms of user time"
    assert system_us is not None
    # The guest suite delegates a cgroup to the tests (`tests/guest/suite.py`); a host without one measures rusage.
    assert source in {"rusage", "cgroup"}
    assert isinstance(peak, int)
    assert peak > 1 << 20, "a shell holds more than 1 MiB"
    assert read is not None
    assert written is not None


async def test_a_build_cgroup_counts_every_process(tmp_path: Path) -> None:
    _user, _system, source, peak, _read, _written = await _build(tmp_path, PARALLEL, "usage-parallel")
    assert source == "cgroup", "the guest suite runs in a cgroup that the tester owns"
    assert isinstance(peak, int)
    assert peak > PROCESSES * STRING_BYTES, f"{peak} bytes: the subshells held their strings at once"
