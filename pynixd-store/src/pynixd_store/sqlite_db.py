"""The SQLite engine of the store database: Nix's own `db.sqlite`, with pynixd's tables beside Nix's.

`LocalStoreDB` pools the connections of this file. The sessions here are the others: the collector's long read, and
a worker thread's own connection.
"""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

import aiosqlite
import anyio
import structlog

from .database import DatabaseError, Params, Row
from .sql import SQLITE, Statements

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator
    from pathlib import Path

    from .database import Session, SyncSession

log = structlog.get_logger(__name__)

__all__ = ["SqliteDatabase", "database_uri", "translated"]

# aiosqlite's own errors for a connection that is closed (`core.py:73`, `:115` of 0.21): the pool
# closed under the block, as the store does when it shuts down.
_AIOSQLITE_CLOSED = frozenset({"no active connection", "Connection closed"})

_SNAPSHOT_BUSY_MS = 30_000
# A thread's write waits behind the daemon's writers instead of failing at once: a reconcile that gives up leaves
# the roots stale for that interval.
_THREAD_BUSY_MS = 5_000


@contextmanager
def translated() -> Iterator[None]:
    try:
        yield
    except sqlite3.Error as exc:
        raise DatabaseError(str(exc)) from exc
    except ValueError as exc:
        if str(exc) not in _AIOSQLITE_CLOSED:
            raise
        raise DatabaseError(str(exc)) from exc


def database_uri(db_path: Path, *, read_only: bool, immutable: bool = False) -> str:
    """The SQLite URI of *db_path*. *immutable* is storage that cannot change (`StoreLayout.immutable`).

    `immutable=1` takes no lock and reads no `-wal` or `-shm`, so it needs no write permission, and a live
    writer's frames are invisible to it.
    """
    if immutable:
        return f"file:{db_path}?mode=ro&immutable=1"
    return f"file:{db_path}?mode={'ro' if read_only else 'rw'}"


class SqliteSession:
    """A `Session` over one `aiosqlite` connection."""

    def __init__(self, conn: aiosqlite.Connection, sql: Statements) -> None:
        self._conn = conn
        self.sql = sql

    async def fetchone(self, sql: str, params: Params = ()) -> Row | None:
        # The cursor closes before the connection goes back: an open statement holds a shared lock on the file.
        with translated():
            async with self._conn.execute(sql, params) as cursor:
                row = await cursor.fetchone()
        return None if row is None else tuple(row)

    async def fetchall(self, sql: str, params: Params = ()) -> list[Row]:
        with translated():
            async with self._conn.execute(sql, params) as cursor:
                return [tuple(row) for row in await cursor.fetchall()]

    async def execute(self, sql: str, params: Params = ()) -> int:
        with translated():
            async with self._conn.execute(sql, params) as cursor:
                return cursor.rowcount

    async def executemany(self, sql: str, rows: Iterable[Params]) -> None:
        with translated():
            await self._conn.executemany(sql, rows)

    async def insert(self, sql: str, params: Params = ()) -> int:
        with translated():
            async with self._conn.execute(sql, params) as cursor:
                row_id = cursor.lastrowid
        if row_id is None:
            raise DatabaseError(f"the statement added no row: {sql.strip()}")
        return row_id

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """IMMEDIATE takes the write lock at once.

        A deferred transaction that upgrades in WAL mode can fail with
        SQLITE_BUSY_SNAPSHOT without waiting. Nix's connection enforces the
        foreign keys of `Refs` (`openDB`), and a pooled one does not by
        default: `invalidate_path` relies on their cascade.
        """
        await self.execute("PRAGMA foreign_keys = 1")
        await self.execute("BEGIN IMMEDIATE")
        try:
            yield
            with translated():
                await self._conn.commit()
        except BaseException:
            # A failed rollback must not replace the error that caused it. The connection does
            # not go back to the pool, so the transaction dies with it.
            with anyio.CancelScope(shield=True):
                try:
                    with translated():
                        await self._conn.rollback()
                except DatabaseError:
                    log.debug("db_rollback_failed", exc_info=True)
            raise


class SqliteSyncSession:
    """A `SyncSession` over one `sqlite3` connection in autocommit mode."""

    def __init__(self, conn: sqlite3.Connection, sql: Statements) -> None:
        self._conn = conn
        self.sql = sql

    def fetchone(self, sql: str, params: Params = ()) -> Row | None:
        with translated():
            row = self._conn.execute(sql, params).fetchone()
        return None if row is None else tuple(row)

    def fetchall(self, sql: str, params: Params = ()) -> list[Row]:
        with translated():
            return [tuple(row) for row in self._conn.execute(sql, params).fetchall()]

    def execute(self, sql: str, params: Params = ()) -> int:
        with translated():
            return self._conn.execute(sql, params).rowcount

    def executemany(self, sql: str, rows: Iterable[Params]) -> None:
        with translated():
            self._conn.executemany(sql, rows)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.execute("COMMIT")
        except BaseException:
            try:
                self.execute("ROLLBACK")
            except DatabaseError:
                log.debug("db_rollback_failed", exc_info=True)
            raise


class SqliteDatabase:
    """The `Database` of one `db.sqlite`."""

    def __init__(self, db_path: Path, sql: Statements = SQLITE, *, immutable: bool = False) -> None:
        self.db_path = db_path
        self.sql = sql
        self.immutable = immutable

    @asynccontextmanager
    async def snapshot(self) -> AsyncIterator[Session]:
        """`mode=ro`: TEMP tables take no lock Nix waits on, and a long read holds back only WAL checkpoints."""
        with translated():
            conn = await aiosqlite.connect(
                database_uri(self.db_path, read_only=True, immutable=self.immutable), uri=True, isolation_level=None
            )
        try:
            session = SqliteSession(conn, self.sql)
            await session.execute(f"PRAGMA busy_timeout = {_SNAPSHOT_BUSY_MS}")
            await session.execute("BEGIN")
            yield session
        finally:
            with anyio.CancelScope(shield=True):
                await conn.interrupt()
                await conn.close()

    @contextmanager
    def sync_session(self, *, write: bool = False) -> Iterator[SyncSession]:
        with translated():
            conn = sqlite3.connect(
                database_uri(self.db_path, read_only=not write, immutable=self.immutable),
                uri=True,
                isolation_level=None,
            )
        try:
            session = SqliteSyncSession(conn, self.sql)
            session.execute(f"PRAGMA busy_timeout = {_THREAD_BUSY_MS}")
            yield session
        finally:
            conn.close()
