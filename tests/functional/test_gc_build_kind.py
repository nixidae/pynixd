"""Decided builds freshen their closure; observations freshen what they name.

A build decision queues its derivation plus its declared inputs as build
seeds, and the flush expands them over the build closure into the same
access table runtime seeds reach. Planning queries stay silent. The
planner therefore judges every path by when something genuinely used
it -- served, ensured, or built -- and by nothing else. Issue #65.
"""

from __future__ import annotations

import time
from pathlib import Path

import aiosqlite

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.gc import Collector
from pynixd.store import LocalDBStore
from pynixd_store.db_migrations import PATH_ACCESS_TABLE
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

STALE_AGE = 7200
MAX_AGE = 3600


async def _add(store_path: Path, work: Path, name: str, text: str) -> str:
    source = work / name
    source.write_text(text)  # noqa: ASYNC240 -- test setup
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "store", "add-path", "--store", str(store_path), str(source)],
        verbose=False,
    )
    return stdout.strip()


async def _backdate(store_path: Path, paths: set[str], age: int) -> None:
    (db,) = list(store_path.rglob("db.sqlite"))
    async with aiosqlite.connect(db) as conn:
        await conn.executemany(
            f"INSERT OR REPLACE INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt) VALUES (?, ?)",
            [(path, int(time.time()) - age) for path in sorted(paths)],
        )
        await conn.commit()


async def test_build_seeds_freshen_the_closure_they_name(tmp_path: Path) -> None:
    """An old input queued as a build seed reads fresh afterwards."""
    store_path = tmp_path / "store"
    store_path.mkdir()
    compiler = await _add(store_path, tmp_path, "compiler.txt", "old, but a build names it\n")
    idle = await _add(store_path, tmp_path, "idle.txt", "old, and nothing names it\n")

    spec = make_test_spec(
        store_id="local",
        store_path=store_path,
        no_probe=True,
        gc_max_age=MAX_AGE,
        gc_allow_execute=True,
    )
    async with Server(
        stores={StoreId("local"): LocalDBStore(spec)},
        ssh_port=None,
        http_port=None,
    ) as server:
        await _backdate(store_path, {compiler, idle}, STALE_AGE)
        db = getattr(server.ctx.local_store, "db", None)
        assert db is not None
        db.mark_paths([compiler], kind="build")
        await db.flush_references()

        resp = await Collector(server.ctx).run(PynixdGCAction.DRY_RUN)

    assert {str(path) for path in resp.store_paths} == {idle}
