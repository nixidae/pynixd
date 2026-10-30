"""Failed checkouts return clean, and flushes pop only flushed seeds.

On 2026-10-06 every pynixd write to the store database failed for
four hours with `database is locked` while fresh connections wrote
fine: pooled connections carry their state across checkouts, and a
checkout that failed mid-write held the file lock until restart.
A connection now returns to the pool only from a clean checkout with no
transaction open; any other is closed. The first tests pin that.

The flush used to drain its queues up front and merge seeds back on
failure — any exit that skipped the merge (cancellation above all)
lost references silently. It now snapshots the queues and subtracts
only committed work, one transaction per chunk; the other tests pin
both halves.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING, Any

import aiosqlite
import anyio
import pytest

from pynixd.db_migrations import PATH_ACCESS_TABLE, apply_migrations
from pynixd.local_store_db import LocalStoreDB

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.anyio
async def test_failed_checkout_returns_clean(tmp_path: Path) -> None:
    """A write without commit does not survive its checkout."""
    db_path = tmp_path / "db.sqlite"
    # The pool opens `mode=rw`, which never creates: the file must exist.
    db_path.touch()
    db = LocalStoreDB(
        db_path=db_path,
        store_path=tmp_path,
        read_only=False,
        reference_flush_interval=60.0,
    )
    try:
        async with db.acquire_conn() as conn:
            await conn.execute("CREATE TABLE t (x INTEGER)")
            await conn.commit()
            await conn.execute("INSERT INTO t VALUES (1)")
            # No commit: a failure between write and commit. (The CREATE
            # commits on its own — sqlite autocommits DDL outside a
            # transaction — so the assertion below reads rows, not tables.)
        async with db.acquire_conn() as second:
            cursor = await second.execute("SELECT COUNT(*) FROM t")
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == 0
            await second.execute("INSERT INTO t VALUES (2)")
            await second.commit()
    finally:
        await db.close_db_pool()


def _pool(tmp_path: Path) -> LocalStoreDB:
    db_path = tmp_path / "db.sqlite"
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute("CREATE TABLE t (x INTEGER)")
    return LocalStoreDB(db_path=db_path, store_path=tmp_path, read_only=False, reference_flush_interval=60.0)


@pytest.mark.anyio
async def test_a_clean_checkout_with_an_open_transaction_is_closed(tmp_path: Path) -> None:
    db = _pool(tmp_path)
    try:
        async with db.acquire_conn() as conn:
            await conn.execute("INSERT INTO t VALUES (1)")
        assert conn not in db._idle_conns
        assert conn not in db._all_conns
        # Closed, so the write lock is free: a plain connection writes at once.
        with closing(sqlite3.connect(tmp_path / "db.sqlite", timeout=0)) as other, other:
            other.execute("INSERT INTO t VALUES (2)")
    finally:
        await db.close_db_pool()


@pytest.mark.anyio
async def test_a_failed_checkout_never_returns_to_the_pool(tmp_path: Path) -> None:
    """Even with no transaction open: its state after an exception is unknown."""
    db = _pool(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="boom"):
            async with db.acquire_conn() as conn:
                await (await conn.execute("SELECT 1")).fetchall()
                raise RuntimeError("boom")
        assert conn not in db._idle_conns
        assert conn not in db._all_conns
    finally:
        await db.close_db_pool()


@pytest.mark.anyio
async def test_a_clean_idle_checkout_is_reused(tmp_path: Path) -> None:
    db = _pool(tmp_path)
    try:
        async with db.acquire_conn() as first:
            await first.execute("INSERT INTO t VALUES (1)")
            await first.commit()
        async with db.acquire_conn() as second:
            assert second is first
    finally:
        await db.close_db_pool()


@pytest.mark.anyio
async def test_a_cancelled_checkout_never_returns_to_the_pool(tmp_path: Path) -> None:
    """Cancelled mid-write: the transaction dies with the connection."""
    db = _pool(tmp_path)
    try:
        with anyio.move_on_after(0.1):
            async with db.acquire_conn() as conn:
                await conn.execute("INSERT INTO t VALUES (1)")
                await anyio.sleep_forever()
        assert conn not in db._all_conns
        with closing(sqlite3.connect(tmp_path / "db.sqlite", timeout=0)) as other, other:
            other.execute("INSERT INTO t VALUES (2)")
            assert other.execute("SELECT x FROM t").fetchall() == [(2,)]
    finally:
        await db.close_db_pool()


@pytest.mark.anyio
async def test_the_wal_checkpoint_truncates_the_file(tmp_path: Path) -> None:
    """Nix leaves its WAL at its largest size; the checkpoint takes it back to zero."""
    db = _pool(tmp_path)
    wal = tmp_path / "db.sqlite-wal"
    # The writer stays open, the way Nix's daemon keeps its connection:
    # SQLite deletes a WAL when its last connection closes.
    with closing(sqlite3.connect(tmp_path / "db.sqlite")) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        with writer:
            writer.executemany("INSERT INTO t VALUES (?)", [(n,) for n in range(20_000)])
        assert wal.stat().st_size > 0
        try:
            await db.checkpoint_wal()
        finally:
            await db.close_db_pool()
        assert wal.stat().st_size == 0
        assert writer.execute("SELECT count(*) FROM t").fetchone()[0] == 20_000


HELLO = "/nix/store/00000000000000000000000000000001-hello"
LIBC = "/nix/store/00000000000000000000000000000002-libc"


def _store_with_a_closure(tmp_path: Path) -> Path:
    """A store database where `hello` references `libc`, migrated."""
    db_path = tmp_path / "db.sqlite"
    db_path.touch()
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths ("
            "id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (1, ?, 0)", (HELLO,))
        conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (2, ?, 0)", (LIBC,))
        conn.execute("INSERT INTO Refs (referrer, reference) VALUES (1, 2)")
    return db_path


async def _flushing_db(tmp_path: Path) -> LocalStoreDB:
    """A `LocalStoreDB` whose schema is usable, for `flush_references`."""
    db_path = _store_with_a_closure(tmp_path)
    db = LocalStoreDB(
        db_path=db_path,
        store_path=tmp_path,
        read_only=False,
        reference_flush_interval=60.0,
    )
    db.schema = await apply_migrations(db_path, read_only=False)
    assert db.schema.usable
    return db


async def _touched(db: LocalStoreDB) -> set[str]:
    async with db.execute(f"SELECT path FROM {PATH_ACCESS_TABLE}") as cursor:
        return {str(row[0]) for row in await cursor.fetchall()}


@pytest.mark.anyio
async def test_flush_subtracts_flushed_seeds(tmp_path: Path) -> None:
    """Success empties the queues and touches the seed closure."""
    db = await _flushing_db(tmp_path)
    try:
        db.pending_references = {HELLO}
        await db.flush_references()

        assert db.pending_references == set()
        assert await _touched(db) == {HELLO, LIBC}
    finally:
        await db.close_db_pool()


@pytest.mark.anyio
async def test_failed_flush_keeps_seeds_queued(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Three failed attempts lose nothing: nothing was ever popped."""
    db = await _flushing_db(tmp_path)
    try:

        async def boom(*args: Any, **kwargs: Any) -> set[str]:
            raise aiosqlite.Error("database is locked")

        monkeypatch.setattr(db, "_expand_closure", boom)
        db.pending_references = {HELLO}
        await db.flush_references()

        assert db.pending_references == {HELLO}
        assert await _touched(db) == set()
    finally:
        await db.close_db_pool()
