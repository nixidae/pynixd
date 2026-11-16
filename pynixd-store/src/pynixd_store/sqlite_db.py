"""The SQLite engine of the store database: Nix's own `db.sqlite`, with pynixd's tables beside Nix's.

`SqliteBackend` is what `LocalStoreDB` holds for this file: the pool, the open probe, the migrations, the WAL
checkpoint and the session reader. `SqliteDatabase` gives the other sessions: the collector's long read, and a worker
thread's own connection.
"""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager, closing, contextmanager, suppress
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiosqlite
import anyio
import structlog

from .database import DatabaseError, Params, Row
from .db_migrations import SchemaState, apply_migrations
from .sql import SQLITE, Statements, nix as nix_sql

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator

    from .database import Session, SyncSession
    from .sql import NixSql

log = structlog.get_logger(__name__)

__all__ = ["SqliteBackend", "SqliteDatabase", "SyncReader", "database_uri", "translated"]

# aiosqlite's own errors for a connection that is closed (`core.py:73`, `:115` of 0.21): the pool
# closed under the block, as the store does when it shuts down.
_AIOSQLITE_CLOSED = frozenset({"no active connection", "Connection closed"})

_CHECKPOINT_BUSY_MS = 1000
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


def holds_valid_paths(db_path: Path) -> bool:
    """Whether the `db.sqlite` at *db_path* exists and registers a path."""
    if not db_path.exists():
        return False
    with closing(sqlite3.connect(database_uri(db_path, read_only=True), uri=True)) as file:
        if file.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ValidPaths'").fetchone() is None:
            return False
        return file.execute(nix_sql.sqlite(provenance=False).any_valid_path).fetchone() is not None


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


class SyncReader:
    """One synchronous read-only `sqlite3` connection for one client session.

    **The point is to keep a `SELECT` off a thread hop.** A read through the
    pool goes to `aiosqlite`, which runs every statement on a worker thread
    and wakes the event loop when it answers. Measured on this machine, that
    hop is 59 us for one `SELECT 1 FROM ValidPaths WHERE path = ?`, while the
    same query on a synchronous connection is 3.2 us (18x). A build sends one
    `IsValidPath` for every derivation of its closure, so the hop is the cost
    that the fast path was meant to remove.

    **It belongs to a session, not to the store.** A build is a burst of
    reads, and SQLite serialises statements on one connection, so a shared
    connection would let one client's slow query wait behind another's. One
    connection for each client keeps that wait out of the other sessions, and
    it is what makes running the query on the event loop safe in the first
    place: the only work it can block is the client that asked for it.

    The connection is read-only, and it reads a store database that holds a
    rollback journal or a WAL. In WAL a reader never blocks the writer; in a
    rollback journal a running `SELECT` takes a shared lock that a writer
    waits behind. A single statement over an indexed `path` is short, and the
    write path of `pynixd` is on the same event loop, so the lock is held for
    as long as a Python call and not for a thread hop.
    """

    def __init__(self, db_path: Path, sql: NixSql = nix_sql.SQLITE, *, immutable: bool = False) -> None:
        self.db_path = db_path
        self.sql = sql
        self.immutable = immutable
        self._conn: sqlite3.Connection | None = None
        self._unavailable = False

    def _connection(self) -> sqlite3.Connection | None:
        if self._unavailable:
            return None
        if self._conn is None:
            try:
                conn = sqlite3.connect(database_uri(self.db_path, read_only=True, immutable=self.immutable), uri=True)
                # A read must never be what stops a store from working. The
                # write path of the store is Nix's, and pynixd waits behind it
                # here rather than failing the query.
                conn.execute("PRAGMA busy_timeout = 5000")
            except sqlite3.Error as exc:
                self._unavailable = True
                log.warning(
                    "sync_reader_unavailable",
                    db_path=str(self.db_path),
                    error=str(exc),
                    detail="reads of this store go back to the pooled aiosqlite connection",
                )
                return None
            self._conn = conn
        return self._conn

    def is_valid_path(self, path: str) -> bool | None:
        """Whether `ValidPaths` holds `path`, or `None` when no read happened.

        `None` says that this connection could not answer, and the caller
        must use the pooled connection instead. It is the same answer over
        every failure, because the fast path is an optimization and a store
        that a client may still build from must never be reported invalid.
        """
        conn = self._connection()
        if conn is None:
            return None
        try:
            cursor = conn.execute(self.sql.is_valid_path, (path,))
        except sqlite3.Error:
            log.debug("sync_reader_query_failed", exc_info=True)
            return None
        try:
            return cursor.fetchone() is not None
        finally:
            cursor.close()

    def query_path_info(self, path: str) -> tuple[tuple[Any, ...] | None, list[str]] | None:
        """The `ValidPaths` row and reference paths for `path`.

        One statement carries both: the info columns repeat on every row
        and the last column holds one reference, so a path with no
        references reads back as a single row with a null reference. `None`
        when no read happened, and the caller must use the pooled connection
        instead -- the same contract as `is_valid_path`. A `(None, [])` pair
        means the read happened and the path is not valid, so the caller
        answers `valid=False`.
        """
        conn = self._connection()
        if conn is None:
            return None
        try:
            cursor = conn.execute(self.sql.path_info_with_refs, (path,))
        except sqlite3.Error:
            log.debug("sync_reader_query_failed", exc_info=True)
            return None
        try:
            rows = cursor.fetchall()
        finally:
            cursor.close()
        if not rows:
            return (None, [])
        return (tuple(rows[0][:9]), [r[9] for r in rows if r[9] is not None])

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class SqliteBackend:
    """The engine half of `LocalStoreDB` for one `db.sqlite`: its connection pool and everything that knows the file."""

    def __init__(self, db_path: Path, *, read_only: bool, immutable: bool = False, max_conns: int = 8) -> None:
        self.db_path = db_path
        self.read_only = read_only or immutable
        #: The database of storage that cannot change (`StoreLayout.immutable`), read with no lock.
        self.immutable = immutable

        self.sql = SQLITE
        """The statements of this database. `probe` checks `ValidPaths` for `provenance` once, and picks Nix's set."""

        self.synchronous: str | None = None
        """`pragma synchronous` for a writing connection: `NORMAL`, or `OFF` without `fsync-metadata`
        (`local-store.cc:536`). `None` until the engine reads its settings; SQLite's default stands until then."""
        self._synchronous_of: dict[aiosqlite.Connection, str] = {}

        self._all_conns: list[aiosqlite.Connection] = []
        self._idle_conns: list[aiosqlite.Connection] = []
        self._pool_lock = anyio.Lock()
        self._sem = anyio.Semaphore(max_conns)

    @asynccontextmanager
    async def acquire_conn(self) -> AsyncIterator[aiosqlite.Connection]:
        """Acquire a connection from the pool.

        A connection goes back to the pool only from a clean exit with no
        transaction open. Every other connection is interrupted and closed:
        this file is Nix's own database, and a pooled connection that kept a
        transaction held its lock for four hours on 2026-10-06 (#697).
        """
        await self._sem.acquire()
        conn: aiosqlite.Connection | None = None
        clean = False
        try:
            async with self._pool_lock:
                if self._idle_conns:
                    conn = self._idle_conns.pop()

            if conn is None:
                uri = database_uri(self.db_path, read_only=self.read_only, immutable=self.immutable)
                # Thirty seconds of busy wait, not five: this file is the
                # Nix daemon's own database, and under heavy builders a
                # flush that gives up early used to take the whole pool
                # down with it. Contention here is normal, not failure.
                conn = await aiosqlite.connect(uri, uri=True, timeout=30)
                async with self._pool_lock:
                    self._all_conns.append(conn)
            await self._apply_synchronous(conn)

            yield conn
            clean = True
        finally:
            # Shielded: a cancellation landing here must not strand the
            # connection or the semaphore with it.
            with anyio.CancelScope(shield=True):
                if conn is not None:
                    await self._checkin(conn, clean=clean)
                self._sem.release()

    async def _apply_synchronous(self, conn: aiosqlite.Connection) -> None:
        """The pragma is the connection's own, so each writing connection takes it once."""
        if self.read_only or self.synchronous is None or self._synchronous_of.get(conn) == self.synchronous:
            return
        await conn.execute(f"PRAGMA synchronous = {self.synchronous}")
        self._synchronous_of[conn] = self.synchronous

    async def _checkin(self, conn: aiosqlite.Connection, *, clean: bool) -> None:
        """Pool *conn* when it is provably idle; otherwise close it.

        A checkout that raised or was cancelled may still have a statement
        running on the connection's thread, so it never returns to the
        pool, whatever `in_transaction` says. A clean checkout that left a
        transaction open is a caller bug: it is logged, and the connection
        closes too, which rolls the transaction back.

        The interrupt stops a running statement at once, so the close that
        follows is quick. The close has no timeout on purpose: a connection
        given up on before it closed is the leak this exists to prevent.
        """
        try:
            idle = clean and not conn.in_transaction
        except Exception:
            idle = False
        if idle:
            async with self._pool_lock:
                self._idle_conns.append(conn)
            return
        if clean:
            log.warning("db_checkin_open_transaction")
        async with self._pool_lock:
            with suppress(ValueError):
                self._all_conns.remove(conn)
            self._synchronous_of.pop(conn, None)
        with suppress(Exception):
            await conn.interrupt()
        try:
            await conn.close()
        except Exception:
            log.warning("db_checkin_close_failed", exc_info=True)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Session]:
        # The session translates its own statements; this covers the connect and the pool's own pragma.
        with translated():
            async with self.acquire_conn() as conn:
                yield SqliteSession(conn, self.sql)

    def database(self) -> SqliteDatabase:
        return SqliteDatabase(self.db_path, self.sql, immutable=self.immutable)

    def sync_reader(self) -> SyncReader:
        return SyncReader(self.db_path, self.sql.nix, immutable=self.immutable)

    async def probe(self) -> None:
        """Read the file once, and pick Nix's statements for its `ValidPaths`. A database that refuses raises `DatabaseError`."""
        try:
            with translated():
                await self._probe()
        except OSError as exc:
            raise DatabaseError(str(exc)) from exc

    async def _probe(self) -> None:
        async with self.acquire_conn() as db:
            if not self.read_only:
                # The journal mode is Nix's to choose, and this used to set
                # WAL unconditionally. Nix picks it from `use-sqlite-wal`
                # (`settings.useSQLiteWAL ? "wal" : "truncate"`) and rewrites
                # the mode on every `LocalStore` open when it differs, so
                # forcing it here made the two flip the file back and forth.
                # The setting defaults to true, so nothing changes on a
                # normal machine; where it is false -- WSL1, or a store a
                # person put on a network filesystem -- it is false for a
                # reason and pynixd must not overrule it.
                async with db.execute("PRAGMA main.journal_mode") as cursor:
                    row = await cursor.fetchone()
                journal_mode = str(row[0]).lower() if row else "unknown"
                if journal_mode != "wal":
                    log.info(
                        "nix_db_journal_mode_not_wal",
                        db_path=self.db_path,
                        journal_mode=journal_mode,
                        detail="readers and writers of this database serialise; Nix chose the mode",
                    )
            # `async with`, and the row is read. A cursor that stops on
            # its first row holds a shared lock on the file until it is
            # closed, and this connection then goes back to the pool and
            # holds that lock for the life of the process. In a rollback
            # journal that blocks every writer, so `apply_migrations`
            # waited out its whole busy timeout and then reported the
            # database as locked. WAL hid it: a reader there does not
            # block a writer, and a Nix store is WAL unless
            # `use-sqlite-wal` is off.
            async with db.execute("SELECT 1 FROM ValidPaths LIMIT 1") as cursor:
                await cursor.fetchone()
            async with db.execute("SELECT name FROM pragma_table_info('ValidPaths')") as cursor:
                columns = {str(row[0]) for row in await cursor.fetchall()}
            self.sql = replace(SQLITE, nix=nix_sql.sqlite(provenance="provenance" in columns))

    async def migrate(self) -> SchemaState:
        return await apply_migrations(self.db_path, read_only=self.read_only, immutable=self.immutable)

    async def checkpoint_wal(self) -> bool:
        """Checkpoint the whole WAL into the database and truncate it to zero.

        `True` when a checkpoint ran, busy or not; `False` when the database is read-only.

        SQLite reuses a WAL file from its start after a checkpoint but never
        shrinks it, and Nix sets no `journal_size_limit`: on dynhetz the WAL
        stood at 1.8 GB beside a 1.6 GB database. A TRUNCATE checkpoint
        needs every reader on the newest snapshot and blocks new writers
        while it waits, so the wait is one second: a busy store skips this
        pass and the next one tries again. Its own connection, never a
        pooled one, so nothing it leaves can reach another caller.
        """
        if self.read_only:
            return False
        wal = Path(f"{self.db_path}-wal")
        before = await anyio.to_thread.run_sync(_size_or_none, wal)
        try:
            async with aiosqlite.connect(f"file:{self.db_path}?mode=rw", uri=True, isolation_level=None) as conn:
                await conn.execute(f"PRAGMA busy_timeout = {_CHECKPOINT_BUSY_MS}")
                if self.synchronous is not None:
                    await conn.execute(f"PRAGMA synchronous = {self.synchronous}")
                async with conn.execute("PRAGMA wal_checkpoint(TRUNCATE)") as cursor:
                    row = await cursor.fetchone()
        except aiosqlite.Error:
            log.warning("db_wal_checkpoint_failed", exc_info=True)
            return True
        if row is None:
            return True
        busy, frames, checkpointed = row
        log.info(
            "db_wal_checkpoint",
            busy=bool(busy),
            frames=frames,
            checkpointed=checkpointed,
            wal_bytes_before=before,
            wal_bytes_after=await anyio.to_thread.run_sync(_size_or_none, wal),
        )
        return True

    async def close(self) -> None:
        async with self._pool_lock:
            for db in self._all_conns:
                with suppress(Exception):
                    await db.close()
            self._all_conns.clear()
            self._idle_conns.clear()


def _size_or_none(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        return None
