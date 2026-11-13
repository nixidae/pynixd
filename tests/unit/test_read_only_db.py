"""A read-only store's database on storage pynixd cannot write (nixidae/nixidae#1309).

A store closed cleanly and put on read-only storage keeps a WAL database with no `-wal` and no `-shm`.
`mode=ro` must create the `-shm` (`os_unix.c:5031` of SQLite 3.53) and cannot, so only `immutable=1` opens it.
`read-only-store.sh` does not reach this case: its earlier `mode=ro` readers leave a `-shm` behind.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
import stat
from typing import TYPE_CHECKING

import pytest

from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

PATH = "/nix/store/00000000000000000000000000000000-x"


@contextlib.contextmanager
def _closed_wal_store(root: Path) -> Iterator[StoreLayout]:
    """A WAL database whose last writer closed, in a directory and a file no one may write."""
    layout = StoreLayout.chroot(root)
    db_dir = layout.db_path.parent
    db_dir.mkdir(parents=True)
    with contextlib.closing(sqlite3.connect(layout.db_path)) as conn:
        conn.execute("PRAGMA journal_mode=wal")
        conn.execute("CREATE TABLE ValidPaths (id integer primary key, path text unique not null)")
        conn.execute("INSERT INTO ValidPaths (path) VALUES (?)", (PATH,))
        conn.commit()
    assert sorted(p.name for p in db_dir.iterdir()) == ["db.sqlite"]
    writable = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
    for p in (layout.db_path, db_dir):
        p.chmod(p.stat().st_mode & ~writable)
    try:
        yield layout
    finally:
        for p in (db_dir, layout.db_path):
            p.chmod(p.stat().st_mode | stat.S_IWUSR)


@pytest.mark.skipif(os.geteuid() == 0, reason="root writes a directory whatever its mode")
@pytest.mark.parametrize("immutable", [True, False])
async def test_only_an_immutable_open_reads_a_closed_wal_database_it_cannot_write(
    tmp_path: Path, immutable: bool
) -> None:
    with _closed_wal_store(tmp_path) as layout:
        db = await LocalStoreDB.open(layout.as_read_only(immutable=immutable))
        try:
            assert db.active is immutable
            if immutable:
                async with db.acquire_conn() as conn, conn.execute("SELECT path FROM ValidPaths") as cursor:
                    assert [row[0] for row in await cursor.fetchall()] == [PATH]
        finally:
            await db.close()
