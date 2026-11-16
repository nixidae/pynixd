"""The PostgreSQL engine of the store database (nixidae/nixidae#1209): psycopg 3 and its pool.

A daemonless pynixd serves a store whose database is a PostgreSQL database, so that several pynixd
instances can serve one store.

Every connection is in autocommit mode. A plain read then holds no transaction, and no connection
waits idle in one; a write opens its own with `BEGIN`. A statement's text always goes through
psycopg's placeholder parser, because the parameters default to `()` and not to `None`: a literal
`%` in a statement is `%%`.
"""

from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING

import anyio
import psycopg
import structlog
from psycopg_pool import AsyncConnectionPool

from . import postgres_schema
from .database import DatabaseError, Params, Row

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator

    from .database import Session, SyncSession
    from .db_migrations import SchemaState
    from .sql import Statements

log = structlog.get_logger(__name__)

__all__ = ["PostgresBackend", "PostgresDatabase", "translated"]

_POOL_CLOSE_SECONDS = 5.0


@contextmanager
def translated() -> Iterator[None]:
    """psycopg's errors as `DatabaseError`; the pool's own (`PoolTimeout`, `PoolClosed`) are psycopg's too."""
    try:
        yield
    except psycopg.Error as exc:
        raise DatabaseError(str(exc)) from exc


class PostgresSession:
    """A `Session` over one psycopg connection in autocommit mode."""

    def __init__(self, conn: psycopg.AsyncConnection, sql: Statements) -> None:
        self._conn = conn
        self.sql = sql

    async def fetchone(self, sql: str, params: Params = ()) -> Row | None:
        with translated():
            cursor = await self._conn.execute(sql, params)
            row = await cursor.fetchone()
        return None if row is None else tuple(row)

    async def fetchall(self, sql: str, params: Params = ()) -> list[Row]:
        with translated():
            cursor = await self._conn.execute(sql, params)
            return [tuple(row) for row in await cursor.fetchall()]

    async def execute(self, sql: str, params: Params = ()) -> int:
        with translated():
            cursor = await self._conn.execute(sql, params)
            return cursor.rowcount

    async def executemany(self, sql: str, rows: Iterable[Params]) -> None:
        with translated():
            async with self._conn.cursor() as cursor:
                await cursor.executemany(sql, rows)

    async def insert(self, sql: str, params: Params = ()) -> int:
        """*sql* ends in `RETURNING id`: PostgreSQL has no last row id."""
        row = await self.fetchone(sql, params)
        if row is None:
            raise DatabaseError(f"the statement added no row: {sql.strip()}")
        return int(row[0])

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        await self.execute("BEGIN")
        try:
            yield
            await self.execute("COMMIT")
        except BaseException:
            # A failed rollback must not replace the error that caused it.
            with anyio.CancelScope(shield=True):
                try:
                    await self.execute("ROLLBACK")
                except DatabaseError:
                    log.debug("db_rollback_failed", exc_info=True)
            raise


class PostgresSyncSession:
    """A `SyncSession` over one synchronous psycopg connection in autocommit mode."""

    def __init__(self, conn: psycopg.Connection, sql: Statements) -> None:
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
        with translated(), self._conn.cursor() as cursor:
            cursor.executemany(sql, rows)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.execute("BEGIN")
        try:
            yield
            self.execute("COMMIT")
        except BaseException:
            try:
                self.execute("ROLLBACK")
            except DatabaseError:
                log.debug("db_rollback_failed", exc_info=True)
            raise


class PostgresDatabase:
    """The `Database` of one PostgreSQL database: connections of their own, outside the pool."""

    def __init__(self, conninfo: str, sql: Statements) -> None:
        self.conninfo = conninfo
        self.sql = sql

    @asynccontextmanager
    async def snapshot(self) -> AsyncIterator[Session]:
        """`REPEATABLE READ`, and not `READ ONLY`: a read-only transaction refuses `CREATE TEMP TABLE`."""
        with translated():
            conn = await psycopg.AsyncConnection.connect(self.conninfo, autocommit=True)
        try:
            session = PostgresSession(conn, self.sql)
            await session.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            yield session
        finally:
            # Closing rolls the transaction back, and its temporary tables go with it.
            with anyio.CancelScope(shield=True):
                await conn.close()

    @contextmanager
    def sync_session(self, *, write: bool = False) -> Iterator[SyncSession]:
        options = "" if write else "-c default_transaction_read_only=on"
        with translated():
            conn = psycopg.connect(self.conninfo, autocommit=True, options=options)
        try:
            yield PostgresSyncSession(conn, self.sql)
        finally:
            conn.close()


class PostgresBackend:
    """The engine half of `LocalStoreDB` for a PostgreSQL database: a pool of autocommit connections.

    `open` fills the pool and fails when the server does not answer. A store whose database is
    PostgreSQL has no other database to fall back to, so it does not start.
    """

    def __init__(self, conninfo: str, sql: Statements, *, max_conns: int = 8, open_seconds: float = 30.0) -> None:
        self.conninfo = conninfo
        self.sql = sql
        self.open_seconds = open_seconds
        self.synchronous: str | None = None
        """Not read: PostgreSQL has no `pragma synchronous`, and the server's own `synchronous_commit` stands."""
        # The pool runs its workers as asyncio tasks of its own (`psycopg_pool/pool_async.py`):
        # no task group of pynixd owns them, and `close` ends them.
        self._pool = AsyncConnectionPool(
            conninfo,
            min_size=1,
            max_size=max_conns,
            open=False,
            kwargs={"autocommit": True},
            name="pynixd-store",
        )

    async def open(self) -> None:
        with translated():
            await self._pool.open(wait=True, timeout=self.open_seconds)

    async def close(self) -> None:
        with anyio.CancelScope(shield=True):
            await self._pool.close(timeout=_POOL_CLOSE_SECONDS)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Session]:
        """One pooled connection. `putconn` rolls back what a block leaves open, and drops a broken connection.

        Not `pool.connection()`: it returns the connection in an unshielded `finally`, and a cancellation there loses
        the connection.
        """
        with translated():
            conn = await self._pool.getconn()
        try:
            yield PostgresSession(conn, self.sql)
        finally:
            with anyio.CancelScope(shield=True), translated():
                await self._pool.putconn(conn)

    def database(self) -> PostgresDatabase:
        return PostgresDatabase(self.conninfo, self.sql)

    def sync_reader(self) -> None:
        """None: a session's reads go through the pool. `SyncReader` exists to skip SQLite's thread hop."""
        return

    async def probe(self) -> None:
        await self.open()

    async def migrate(self) -> SchemaState:
        async with self.connection() as session:
            return await postgres_schema.migrate(session)

    async def checkpoint_wal(self) -> bool:
        """False: the server checkpoints its own WAL."""
        return False
