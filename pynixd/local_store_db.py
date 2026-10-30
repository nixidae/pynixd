"""Direct SQLite access to the local Nix store database.

Provides a connection pool for direct SQL queries against the local store DB.
Operations that support fast-path SQL queries use ``store.db.acquire_conn()``
directly rather than going through a dispatcher.

Reference updates are batched writes into pynixd's own access table:
heads only, never closures. Marking one path used to touch its whole
transitive closure -- tens of thousands of rows in one write transaction
against the daemon's own database, every few seconds -- for paths nobody
observed. A row now means exactly "named in traffic"; anything untouched
resolves its age from `registrationTime` instead. Nix's own tables are
never written: not even `registrationTime`, which Nix writes once at
registration and `nix path-info` reports as the entry age.

If the database can't be opened (permissions, missing file, wrong schema),
logs a warning and becomes unavailable — callers fall back to the daemon.

pynixd also keeps tables of its own in this file. `db_migrations` owns their
schema and their version, and `open` migrates them. The two answers are
separate: `active` says that Nix's tables are readable, and `schema.usable`
says that pynixd's tables are at the version this pynixd knows. A store can
give the first and refuse the second, and then every fast path that reads
`ValidPaths` still works.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import statistics
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from itertools import batched
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Self

import aiosqlite
import anyio
import structlog

from .build_stats import BuildEstimateSource, ExpectedBytes, StoreLink
from .db_migrations import (
    BUILD_OBSERVATION_TABLE,
    DERIVATION_SHAPE_TABLE,
    PATH_ACCESS_TABLE,
    SchemaState,
    apply_migrations,
)
from .store.queries import IS_VALID_PATH, QUERY_PATH_INFO_WITH_REFS
from .store_layout import StoreLayout
from .store_path import StorePath

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from nix_daemon_protocol.aliases import StorePathSet

    from .build_stats import BuildObservation, DerivationIdentity

log = structlog.get_logger(__name__)


# ── SQL constants ─────────────────────────────────────────────────────

# The touched paths, and only them: the flush expands each queued seed
# over its closure first, so this writes the union. Unmarked paths
# resolve their age from `registrationTime`, so nothing needs the
# closure's testimony twice.
TOUCH_PATH_ACCESS = f"""
INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt)
SELECT path, unixepoch() FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
ON CONFLICT (path) DO UPDATE SET lastReferencedAt = excluded.lastReferencedAt
"""

# The runtime closure of a seed set: the seeds and everything reachable
# through references, never crossing a derivation boundary. A `.drv`
# file's references are its build inputs, so walking through one would
# refresh a build closure from a mere observation -- the pushed
# derivations stay heads. Store derivation paths always end in `.drv`,
# so the suffix is the boundary. The parameter is a JSON array of store
# paths. Issue #65.
_RUNTIME_CLOSURE_OF_SEEDS = """
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.reference FROM closure c
        JOIN Refs r ON c.id = r.referrer
        JOIN ValidPaths cvp ON cvp.id = c.id
        WHERE cvp.path NOT LIKE '%.drv'
    )
    SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
"""

# The build closure of a seed set: the seeds and everything reachable
# through references, derivations included. The scheduler seeds this
# with the derivation it decided to build plus the inputs the request
# names, so the walk covers the whole build closure whether or not the
# store has registered every layer. Same array parameter. Issue #65.
_BUILD_CLOSURE_OF_SEEDS = """
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer
    )
    SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
"""

# The referrers of each seed, transitively, seeds included: the mirror of
# `_CLOSURE_OF_SEEDS` walking `Refs` the other way. A delete set travels
# with its referrers (`gc.cc:653`), so the planner closes its set under
# this. The parameter is a JSON array of store paths, like above.
_REFERRERS_OF_SEEDS = """
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.referrer
        FROM closure c JOIN Refs r ON c.id = r.reference
    )
    SELECT id FROM closure
"""

QUERY_REFERRER_CLOSURE = f"""
SELECT path FROM ValidPaths WHERE id IN ({_REFERRERS_OF_SEEDS})
"""

QUERY_WEIGHTS = f"""
SELECT v.path, v.narSize, COALESCE(a.lastReferencedAt, v.registrationTime) FROM ValidPaths v
LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
WHERE v.path IN (SELECT value FROM json_each(?))
"""

QUERY_UNREFERENCED_SINCE = f"""
SELECT v.path FROM ValidPaths v
LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
WHERE COALESCE(a.lastReferencedAt, v.registrationTime) < ?
"""

# The join the tables of pynixd get by living in Nix's own database.
PRUNE_PATH_ACCESS = f"""
DELETE FROM {PATH_ACCESS_TABLE}
WHERE path NOT IN (SELECT path FROM ValidPaths)
"""

MarkKind = Literal["runtime", "build"]
"""Which seed set a mark joins, and so which closure the flush expands.

`runtime` is observed use: a path a client names. `build` is a decided
build: the derivation plus the inputs its request names. Both expand to
path lists at flush and upsert the same freshness column -- kind lives
only at gather time, in which CTE runs, never in what is stored. The
runtime walk stops at derivation boundaries, so pushing a derivation
around refreshes its head and never its build closure. Issue #65.
"""

# One root's closure over the same edges the liveness query walks:
# references, derivers, and deriver references. The parameter is a JSON
# array of seed paths, and the report runs it once per distinct seed set.
_ROOT_CLOSURE_OF_SEEDS = """
    WITH RECURSIVE closure(id) AS (
        SELECT vp.id FROM ValidPaths vp WHERE vp.path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer
        UNION
        SELECT deriver_vp.id FROM closure c
        JOIN ValidPaths current_vp ON c.id = current_vp.id
        JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path
        WHERE current_vp.deriver IS NOT NULL
        UNION
        SELECT r.reference FROM closure c
        JOIN ValidPaths current_vp ON c.id = current_vp.id
        JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path
        JOIN Refs r ON deriver_vp.id = r.referrer
        WHERE current_vp.deriver IS NOT NULL
    )
    SELECT id FROM closure
"""

# The build closure of a seed set: the seeds and everything reachable
# through references, derivations included. The scheduler seeds this
# with the derivation it decided to build plus the inputs the request
# names, so the walk covers the whole build closure whether or not the
# store has registered every layer. Same array parameter. Issue #65.
_BUILD_CLOSURE_OF_SEEDS = """
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer
    )
    SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
"""


@dataclass(frozen=True)
class RootAttribution:
    """One root's storage: full and exclusive path counts and bytes.

    Full counts everything the root keeps alive, shared or not;
    exclusive counts what no other root reaches. Both close over the
    same edges the liveness query walks, so the report reconciles with
    collection decisions instead of telling a second story.
    """

    label: str
    full_paths: int
    full_bytes: int
    exclusive_paths: int
    exclusive_bytes: int


OBSERVATIONS_KEPT = 20
"""Rows kept per identity and store. Enough for a median that one outlier
cannot move, and few enough that a store's history stays a few MB."""

INSERT_DERIVATION_SHAPE = f"""
INSERT OR IGNORE INTO {DERIVATION_SHAPE_TABLE} (shape, keys, features) VALUES (?, ?, ?)
"""

INSERT_BUILD_OBSERVATION = f"""
INSERT INTO {BUILD_OBSERVATION_TABLE}
(name, system, shape, storeId, status, queuedMs, inputsMs, inputsBytes, buildMs,
 cpuUserUs, cpuSystemUs, pullMs, pullBytes, finishedAt)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch())
"""

PRUNE_BUILD_OBSERVATIONS = f"""
DELETE FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND storeId = ? AND id NOT IN (
    SELECT id FROM {BUILD_OBSERVATION_TABLE}
    WHERE name = ? AND system = ? AND shape = ? AND storeId = ?
    ORDER BY id DESC LIMIT ?
)
"""

QUERY_BUILD_MS_OF_SHAPE = f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT ?
"""

QUERY_BUILD_MS_OF_SHAPE_ON_STORE = f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND storeId = ? AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT ?
"""

QUERY_BUILD_MS_OF_NAME = f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT ?
"""

LINK_ROWS = 50
"""A store's newest rows that its link rates read: recent enough to follow a
change of network, many enough that one stalled transfer does not decide."""

QUERY_STORE_LINK = f"""
SELECT status, inputsBytes, inputsMs, pullBytes, pullMs, buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE storeId = ?
ORDER BY id DESC LIMIT ?
"""

QUERY_INPUTS_BYTES = f"""
SELECT inputsBytes FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND storeId = ? AND inputsBytes IS NOT NULL
ORDER BY id DESC LIMIT ?
"""

QUERY_PULL_BYTES = f"""
SELECT pullBytes FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND pullBytes IS NOT NULL
ORDER BY id DESC LIMIT ?
"""

_DEFAULT_REFERENCE_FLUSH_INTERVAL = 5.0


def _bytes_per_s(transfers: list[tuple[int, int]]) -> float | None:
    """Total bytes over total time, for `(bytes, ms)` pairs."""
    total_ms = sum(ms for _, ms in transfers)
    return sum(size for size, _ in transfers) * 1000 / total_ms if total_ms else None


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

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._unavailable = False

    def _connection(self) -> sqlite3.Connection | None:
        if self._unavailable:
            return None
        if self._conn is None:
            try:
                conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
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
            cursor = conn.execute(IS_VALID_PATH, (path,))
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
            cursor = conn.execute(QUERY_PATH_INFO_WITH_REFS, (path,))
        except sqlite3.Error:
            log.debug("sync_reader_query_failed", exc_info=True)
            return None
        try:
            rows = cursor.fetchall()
        finally:
            cursor.close()
        if not rows:
            return (None, [])
        return (tuple(rows[0][:8]), [r[8] for r in rows if r[8] is not None])

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class LocalStoreDB:
    """Connection pool and dispatcher for Nix store SQLite.

    Operation types implement their own DB logic via ``execute_db(db)``.
    This class only manages connections and dispatches.

    Use the async factory ``await LocalStoreDB.open(layout)`` to create. A
    caller that holds it for one block writes ``async with await
    LocalStoreDB.open(layout) as db:``, so it is closed on every way out.
    One left open keeps an aiosqlite thread alive, and the interpreter
    then cannot exit.
    """

    def __init__(
        self,
        db_path: Path | None,
        store_path: Path | None,
        read_only: bool,
        reference_flush_interval: float,
        max_conns: int = 8,
    ) -> None:
        self.db_path = db_path
        self.store_path = store_path
        self.read_only: bool = read_only
        self.reference_flush_interval = reference_flush_interval
        self.flush_retry_seconds = 1.0
        """The first backoff of a flush the database refused; it doubles.

        A test sets 0. Patching `anyio.sleep` instead hung CI: the loop is
        shared, and a leaked `flush_loop` spun on the patched sleep without
        yielding. nixidae/nixidae#1019."""

        self.pending_references: set[str] = set()
        """Seeds for runtime-closure expansion: full store paths named in
        traffic since the last flush.

        Seeds, not paths: the flush expands each seed over the runtime
        closure -- references transitively, never crossing a derivation
        boundary -- and upserts the union. A burst of queries names
        hundreds of seeds, not hundreds of thousands of paths, so
        queueing stays an in-memory set insert no matter the burst size.

        Plain strings, because two `StorePath` classes reach this set and
        the SQL statement wants the text. `pynixd.store_path.StorePath`
        keeps the path without the `/nix/store/` prefix and adds it back
        in `__str__`, and the wire `StorePath` is a `str` of the whole path.
        """
        self.pending_build_references: set[str] = set()
        """Seeds for build-closure expansion: decided derivations plus the
        inputs their requests name. The flush expands them over the build
        closure into the same access table runtime seeds reach: kind lives
        in which closure runs, never in what is stored. See `MarkKind`.
        """

        self.flush_task: asyncio.Task[None] | None = None

        self.schema = SchemaState(version=0, usable=False, reason="the schema was never checked")
        """The state of the tables pynixd owns. `open` replaces it.

        It starts unusable, so a `LocalStoreDB` built by hand answers no query
        that needs one of those tables. `active` alone cannot say: it reports
        that Nix's own tables are readable, which is a different question.
        """

        self._all_conns: list[aiosqlite.Connection] = []
        self._idle_conns: list[aiosqlite.Connection] = []
        self._pool_lock = anyio.Lock()
        self._sem = anyio.Semaphore(max_conns)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    @classmethod
    def inactive(
        cls,
        layout: StoreLayout,
        *,
        reference_flush_interval: float = _DEFAULT_REFERENCE_FLUSH_INTERVAL,
    ) -> LocalStoreDB:
        """An instance that answers no query, for a store with no usable database.

        Three callers want this and each built it by hand: no database file,
        a database that would not open, and a store this class must not read
        at all (`LocalDBStore._refuses_a_database`).
        """
        return cls(
            db_path=None,
            store_path=layout.real_store_dir,
            read_only=True,
            reference_flush_interval=reference_flush_interval,
        )

    @property
    def active(self) -> bool:
        return self.db_path is not None

    def sync_reader(self) -> SyncReader | None:
        """A read-only connection for one client session, or `None` when none.

        The caller owns it and closes it when the session ends. A store with
        no readable database gets `None`, and every caller then keeps the
        pooled `aiosqlite` path. The connection is opened by the first query,
        and `SyncReader` refuses and reports `None` from there if the store
        will not give one, so a store that only `aiosqlite` can read still
        answers every query over the pool.
        """
        if not self.active or self.db_path is None:
            return None
        return SyncReader(self.db_path)

    @asynccontextmanager
    async def acquire_conn(self) -> AsyncIterator[aiosqlite.Connection]:
        """Acquire a connection from the pool.

        A connection goes back to the pool only from a clean exit with no
        transaction open. Every other connection is interrupted and closed:
        this file is Nix's own database, and a pooled connection that kept a
        transaction held its lock for four hours on 2026-10-06 (#697).
        """
        if not self.active:
            raise RuntimeError("Database not active")

        await self._sem.acquire()
        conn: aiosqlite.Connection | None = None
        clean = False
        try:
            async with self._pool_lock:
                if self._idle_conns:
                    conn = self._idle_conns.pop()

            if conn is None:
                mode = "ro" if self.read_only else "rw"
                uri = f"file:{self.db_path}?mode={mode}"
                # Thirty seconds of busy wait, not five: this file is the
                # Nix daemon's own database, and under heavy builders a
                # flush that gives up early used to take the whole pool
                # down with it. Contention here is normal, not failure.
                conn = await aiosqlite.connect(uri, uri=True, timeout=30)
                async with self._pool_lock:
                    self._all_conns.append(conn)

            yield conn
            clean = True
        finally:
            # Shielded: a cancellation landing here must not strand the
            # connection or the semaphore with it.
            with anyio.CancelScope(shield=True):
                if conn is not None:
                    await self._checkin(conn, clean=clean)
                self._sem.release()

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
        with suppress(Exception):
            await conn.interrupt()
        try:
            await conn.close()
        except Exception:
            log.warning("db_checkin_close_failed", exc_info=True)

    @asynccontextmanager
    async def execute(
        self,
        query: str,
        params: tuple[Any, ...] = (),
    ) -> AsyncIterator[aiosqlite.Cursor]:
        """Execute a query and return the cursor.

        Convenience method that acquires a connection, runs the query,
        and returns the cursor. Usage::

            async with db.execute("SELECT * FROM Foo WHERE id = ?", (id,)) as cursor:
                row = await cursor.fetchone()

        For multiple queries or complex flows, use acquire_conn() directly.

        The cursor is closed when the block ends, and it has to be. A caller
        that reads one row of a query that could return more leaves the
        statement open, and an open statement holds a shared lock on the
        database file. The connection then goes back to the pool with that
        lock, and in a rollback journal every writer waits behind it. Almost
        every caller here calls `fetchone`, so almost every call left one.
        """
        async with self.acquire_conn() as conn:
            cursor = await conn.execute(query, params)
            try:
                yield cursor
            finally:
                with suppress(ValueError, aiosqlite.Error):
                    # ValueError: aiosqlite raises it for a connection
                    # that went away underneath the read.
                    await cursor.close()

    @classmethod
    async def open(
        cls,
        layout: StoreLayout,
        reference_flush_interval: float = _DEFAULT_REFERENCE_FLUSH_INTERVAL,
    ) -> LocalStoreDB:
        """Open the Nix store database. Returns an instance (possibly with no DB).

        This never raises. A store whose database pynixd cannot read gets an
        inactive instance, every fast path of `LocalDBStore` declines, and
        `DaemonStore.execute` uses the wire. That is what lets `use_db` default
        to true.
        """
        db_path = resolve_db_path(layout)
        if db_path is None:
            return cls.inactive(layout, reference_flush_interval=reference_flush_interval)

        db_dir = db_path.parent
        can_write = os.access(db_dir, os.W_OK)
        read_only = not can_write

        instance = cls(
            db_path=db_path,
            store_path=layout.real_store_dir,
            read_only=read_only,
            reference_flush_interval=reference_flush_interval,
        )
        try:
            async with instance.acquire_conn() as db:
                if not read_only:
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
                            db_path=db_path,
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

        except (aiosqlite.Error, OSError) as e:
            log.warning(
                "nix_db_open_failed",
                db_path=db_path,
                error=e,
            )
            # The probe above already opened pool connections, and each one
            # holds a worker thread: returning with them open leaks threads
            # that keep short-lived processes from exiting, the same class
            # of failure as issue #61 one level up. Close what the probe
            # opened, then report the store as having no database.
            with suppress(Exception):
                await instance.close_db_pool()
            return cls.inactive(layout, reference_flush_interval=reference_flush_interval)

        instance.schema = await apply_migrations(db_path, read_only=read_only)
        if not instance.schema.usable:
            log.warning(
                "pynixd_tables_unavailable",
                db_path=db_path,
                version=instance.schema.version,
                reason=instance.schema.reason,
                detail="the fast paths that read Nix's own tables are not affected",
            )
        log.info(
            "local_store_db_active",
            db_path=db_path,
            mode="read-write" if not read_only else "read-only",
            pynixd_schema_version=instance.schema.version,
            pynixd_tables=instance.schema.usable,
        )
        return instance

    # ── Internal utility queries ──────────────────────────────────────
    # These are not operation dispatches but internal helpers used by
    # non-operation code (GC, build planner, http cache).

    async def query_paths_not_referenced_since(self, max_age_seconds: int) -> StorePathSet | None:
        """The paths nothing has referenced for `max_age_seconds`, for an LRU collector.

        This replaces `query_stale_paths`, which asked the same question of
        `ValidPaths.registrationTime`. Nothing called it, and the column it
        read answers two questions at once. `PynixdPathAccess` answers one,
        and `registrationTime` answers for the paths the tracker never saw:
        the effective time is the access row when one exists, else the
        registration time.

        The fallback changes who is exposed. A path pynixd never saw, with an
        old registration time, is now reported when dead -- before, no row
        meant never eligible. That is the whole point (an unwatched buildup
        must age out), and the cost is stated plainly: for such a path the
        only evidence of age is Nix's column, and the only evidence of death
        is the mirror.
        """
        if not self.active or not self.schema.usable:
            return None
        try:
            cutoff = int(time.time()) - max_age_seconds
            async with self.execute(QUERY_UNREFERENCED_SINCE, (cutoff,)) as cursor:
                rows = await cursor.fetchall()
            return {StorePath(r[0]) for r in rows}
        except aiosqlite.Error:
            log.debug("query_paths_not_referenced_since_failed", exc_info=True)
            return None

    async def query_roots_report(self, labeled: list[tuple[str, list[str]]]) -> list[RootAttribution] | None:
        """Full and exclusive storage per labeled root, exact.

        Labels sharing a seed set compute one closure; a path reachable
        from exactly one label counts exclusive to it. The counting runs
        through one integer counter per valid path in a temp table --
        pairs would work but store an order of magnitude more -- and the
        table drops with the connection. Read-only throughout: the temp
        schema is the only thing written, and it never leaves the session.
        `None` when the database cannot answer.
        """
        if not self.active:
            return None
        groups: dict[tuple[str, ...], list[str]] = {}
        for label, seed_paths in labeled:
            key = tuple(sorted(set(seed_paths)))
            if key:
                groups.setdefault(key, []).append(label)
        if not groups:
            return []
        try:
            async with self.acquire_conn() as db:
                try:
                    await db.execute(
                        "CREATE TEMP TABLE _pynixd_root_hits(id INTEGER PRIMARY KEY, n INTEGER NOT NULL DEFAULT 0)"
                    )
                    await db.execute("INSERT INTO _pynixd_root_hits(id) SELECT id FROM ValidPaths")
                    full: dict[tuple[str, ...], tuple[int, int]] = {}
                    for seeds in groups:
                        params = (json.dumps(list(seeds)),)
                        async with db.execute(
                            f"SELECT COUNT(*), COALESCE(SUM(narSize), 0) FROM ValidPaths "
                            f"WHERE id IN ({_ROOT_CLOSURE_OF_SEEDS})",
                            params,
                        ) as cursor:
                            counts = await cursor.fetchone()
                        full[seeds] = (int(counts[0]), int(counts[1])) if counts else (0, 0)
                        await db.execute(
                            f"UPDATE _pynixd_root_hits SET n = n + ? WHERE id IN ({_ROOT_CLOSURE_OF_SEEDS})",
                            (len(groups[seeds]), json.dumps(list(seeds))),
                        )
                    report: list[RootAttribution] = []
                    for seeds, labels in groups.items():
                        async with db.execute(
                            f"SELECT COUNT(*), COALESCE(SUM(v.narSize), 0) FROM ValidPaths v "
                            f"JOIN _pynixd_root_hits h ON h.id = v.id "
                            f"WHERE h.n = 1 AND v.id IN ({_ROOT_CLOSURE_OF_SEEDS})",
                            (json.dumps(list(seeds)),),
                        ) as cursor:
                            counts = await cursor.fetchone()
                        exclusive = (int(counts[0]), int(counts[1])) if counts else (0, 0)
                        for label in labels:
                            report.append(
                                RootAttribution(
                                    label=label,
                                    full_paths=full[seeds][0],
                                    full_bytes=full[seeds][1],
                                    exclusive_paths=exclusive[0],
                                    exclusive_bytes=exclusive[1],
                                )
                            )
                    return report
                finally:
                    with suppress(Exception):
                        await db.execute("DROP TABLE _pynixd_root_hits")
        except aiosqlite.Error:
            log.debug("query_roots_report_failed", exc_info=True)
            return None

    async def query_referrer_closure(self, paths: Iterable[str]) -> set[str] | None:
        """The paths that reference `paths`, transitively, seeds included.

        The planner closes its delete set under this, because Nix refuses a
        named path whose referrer is not named in the same request
        (`gc.cc:653`). A referrer of a dead path is itself dead: a live
        referrer would keep its references alive. `None` when the database
        cannot answer, and the planner plans nothing on that.
        """
        if not self.active or not self.schema.usable:
            return None
        try:
            paths_json = json.dumps(sorted(set(paths)))
            async with self.execute(QUERY_REFERRER_CLOSURE, (paths_json,)) as cursor:
                rows = await cursor.fetchall()
            return {r[0] for r in rows}
        except aiosqlite.Error:
            log.debug("query_referrer_closure_failed", exc_info=True)
            return None

    async def query_weights(self, paths: Iterable[str]) -> dict[str, tuple[int, int | None]] | None:
        """`(nar_size, last_referenced)` for each of `paths` the store holds.

        The planner weighs its candidates by size and age, and this is the
        one read of both per pass: indexed, and bounded by the candidate set.
        Not path infos: those carry every reference, and a 105k-path plan
        loaded 9.2M reference rows to read one column (nixidae#1133).
        The date is the access row when one exists, else the registration
        time -- the same resolution the staleness query uses, so the planner
        and the weigher never disagree about a path's age. A path neither
        table dates carries `None`, which the caller treats as age zero
        rather than as old. Paths outside `ValidPaths` are absent. `None`
        when the database cannot answer.
        """
        if not self.active or not self.schema.usable:
            return None
        try:
            paths_json = json.dumps(sorted(set(paths)))
            async with self.execute(QUERY_WEIGHTS, (paths_json,)) as cursor:
                rows = await cursor.fetchall()
            return {r[0]: (r[1] or 0, None if r[2] is None else int(r[2])) for r in rows}
        except (aiosqlite.Error, ValueError):
            log.debug("query_weights_failed", exc_info=True)
            return None

    async def prune_path_access(self) -> int:
        """Drop the rows for paths the store no longer holds. Returns the count.

        This is the join that keeping pynixd's tables inside Nix's database
        buys: one statement compares `PynixdPathAccess` against `ValidPaths`.
        Without it the table grows for ever, because a path that the garbage
        collector deletes leaves its access time behind.
        """
        if not self.active or self.read_only or not self.schema.usable:
            return 0
        try:
            async with self.acquire_conn() as db:
                cursor = await db.execute(PRUNE_PATH_ACCESS)
                removed = cursor.rowcount
                await db.commit()
        except aiosqlite.Error:
            log.warning("prune_path_access_failed", exc_info=True)
            return 0
        if removed > 0:
            log.debug("path_access_pruned", removed=removed)
        return max(removed, 0)

    # ── Last reference times ──────────────────────────────────────────

    def mark_path(self, path: StorePath | str, kind: MarkKind = "runtime") -> None:
        """Note that something referenced `path` just now."""
        self.mark_paths((path,), kind=kind)

    def mark_paths(self, paths: Iterable[StorePath | str], kind: MarkKind = "runtime") -> None:
        """Queue each of `paths` as a seed for the next flush.

        Seeds only: the flush expands each seed over its closure -- the
        runtime closure for `runtime` seeds, the build closure for `build`
        seeds -- and upserts the union. A burst of queries names hundreds
        of seeds, not hundreds of thousands of paths, so queueing stays
        an in-memory set insert no matter the burst size.

        `kind` picks the seed set. Serving a path queues it for runtime
        expansion; the scheduler queues a decided build's derivation plus
        its declared inputs for build expansion. The default keeps every
        existing caller on the runtime queue.

        The write happens later. `flush_loop` drains the queues every few
        seconds, so a burst of queries costs two closure reads and
        small chunked writes, not one write per path.
        """
        if self.active and not self.read_only:
            queue = self.pending_build_references if kind == "build" else self.pending_references
            queue.update(str(path) for path in paths)

    async def record_build_observation(self, identity: DerivationIdentity, observation: BuildObservation) -> int | None:
        """Store one build's cost, and keep the newest `OBSERVATIONS_KEPT` of its identity and store.

        Answers the row id, or `None` when nothing was written.
        """
        if not self.active or self.read_only or not self.schema.usable:
            return None
        key = (identity.name, identity.system, identity.shape, observation.store_id)
        try:
            async with self.acquire_conn() as db:
                await db.execute(
                    INSERT_DERIVATION_SHAPE,
                    (identity.shape, json.dumps(identity.keys), json.dumps(identity.features)),
                )
                cursor = await db.execute(
                    INSERT_BUILD_OBSERVATION,
                    (
                        *key,
                        observation.status,
                        observation.queued_ms,
                        observation.inputs_ms,
                        observation.inputs_bytes,
                        observation.build_ms,
                        observation.cpu_user_us,
                        observation.cpu_system_us,
                        observation.pull_ms,
                        observation.pull_bytes,
                    ),
                )
                row_id = cursor.lastrowid
                await db.execute(PRUNE_BUILD_OBSERVATIONS, (*key, *key, OBSERVATIONS_KEPT))
                await db.commit()
        except aiosqlite.Error:
            log.warning("record_build_observation_failed", name=identity.name, exc_info=True)
            return None
        return row_id

    async def expected_build_ms(self, identity: DerivationIdentity) -> int | None:
        """The median time of the identity's recent `BUILT` builds, on any store.

        Falls back to the name and system under any shape: a derivation that
        gained an attribute is still mostly the same build.
        """
        if not self.active or not self.schema.usable:
            return None
        try:
            for query, params in (
                (QUERY_BUILD_MS_OF_SHAPE, (identity.name, identity.system, identity.shape, OBSERVATIONS_KEPT)),
                (QUERY_BUILD_MS_OF_NAME, (identity.name, identity.system, OBSERVATIONS_KEPT)),
            ):
                async with self.execute(query, params) as cursor:
                    rows = await cursor.fetchall()
                if rows:
                    return int(statistics.median(int(row[0]) for row in rows))
        except aiosqlite.Error:
            log.debug("expected_build_ms_failed", name=identity.name, exc_info=True)
        return None

    async def expected_build_ms_on(
        self, identity: DerivationIdentity, store_id: str
    ) -> tuple[int | None, BuildEstimateSource | None]:
        """The median time of the identity's recent `BUILT` builds on *store_id*, and which history answered.

        Falls back to the shape on any store, then to the name and system.
        """
        if not self.active or not self.schema.usable:
            return None, None
        shape = (identity.name, identity.system, identity.shape)
        try:
            for source, query, params in (
                (BuildEstimateSource.SHAPE_ON_STORE, QUERY_BUILD_MS_OF_SHAPE_ON_STORE, (*shape, store_id)),
                (BuildEstimateSource.SHAPE, QUERY_BUILD_MS_OF_SHAPE, shape),
                (BuildEstimateSource.NAME, QUERY_BUILD_MS_OF_NAME, (identity.name, identity.system)),
            ):
                async with self.execute(query, (*params, OBSERVATIONS_KEPT)) as cursor:
                    rows = await cursor.fetchall()
                if rows:
                    return int(statistics.median(int(row[0]) for row in rows)), source
        except aiosqlite.Error:
            log.debug("expected_build_ms_on_failed", name=identity.name, exc_info=True)
        return None, None

    async def store_link(self, store_id: str) -> StoreLink:
        """The store's send and receive rates and dispatch round trip, from its newest rows.

        A rate is total bytes over total time, so a large transfer weighs
        what it moved. The round trip is the median build time of the
        successes that built nothing: their whole time is delegation.
        """
        if not self.active or not self.schema.usable:
            return StoreLink()
        try:
            async with self.execute(QUERY_STORE_LINK, (store_id, LINK_ROWS)) as cursor:
                rows = await cursor.fetchall()
        except aiosqlite.Error:
            log.debug("store_link_failed", store_id=store_id, exc_info=True)
            return StoreLink()
        sent = [(int(r[1]), int(r[2])) for r in rows if r[1] and r[2]]
        pulled = [(int(r[3]), int(r[4])) for r in rows if r[3] and r[4]]
        round_trips = [int(r[5]) for r in rows if r[0] != 0 and r[5] is not None]
        return StoreLink(
            send_bytes_per_s=_bytes_per_s(sent),
            receive_bytes_per_s=_bytes_per_s(pulled),
            dispatch_ms=int(statistics.median(round_trips)) if round_trips else None,
        )

    async def expected_bytes(self, identity: DerivationIdentity, store_id: str) -> ExpectedBytes:
        """The median bytes the identity sent to this store, and pulled from any store."""
        if not self.active or not self.schema.usable:
            return ExpectedBytes()
        shape = (identity.name, identity.system, identity.shape)
        try:
            async with self.execute(QUERY_INPUTS_BYTES, (*shape, store_id, OBSERVATIONS_KEPT)) as cursor:
                inputs = [int(r[0]) for r in await cursor.fetchall()]
            async with self.execute(QUERY_PULL_BYTES, (*shape, OBSERVATIONS_KEPT)) as cursor:
                pulls = [int(r[0]) for r in await cursor.fetchall()]
        except aiosqlite.Error:
            log.debug("expected_bytes_failed", name=identity.name, exc_info=True)
            return ExpectedBytes()
        return ExpectedBytes(
            inputs=int(statistics.median(inputs)) if inputs else None,
            pull=int(statistics.median(pulls)) if pulls else None,
        )

    async def flush_references(self) -> None:
        """Expand the pending seeds over their closures and touch the union.

        The queues hold seeds, not paths: runtime seeds expand over the
        runtime closure, build seeds over the build closure, both through
        read-only walks, and one table takes the union. `ValidPaths`
        `registrationTime` is read, never written: Nix writes it once at
        registration, stock `nix-collect-garbage --delete-older-than`
        reads it as entry age, and pynixd needs no code for either.
        `PynixdPathAccess` is the column that says what pynixd saw, and
        it is the only column pynixd writes. Issue
        nixidae/nixidae#205 has the whole argument; issue #65 picks
        the closure by context.
        """
        if not self.active or self.read_only:
            return
        # Snapshot the queues instead of draining them: only committed
        # work leaves, so a failure — or a cancellation, which skips any
        # merge-back entirely — loses no reference. Seeds that arrive
        # mid-flush stay queued for the next one.
        runtime_seeds = set(self.pending_references)
        build_seeds = set(self.pending_build_references)
        if not runtime_seeds and not build_seeds:
            return

        for attempt in range(3):
            try:
                t0 = time.monotonic()
                async with self.acquire_conn() as db:
                    touched: set[str] = set()
                    # The walks read; only the chunks below write, one
                    # transaction each: a failed chunk rolls back to the
                    # last commit instead of leaking into the pool, and a
                    # short write holds the lock briefly enough that the
                    # daemon's own writers get in between chunks.
                    if self.schema.usable:
                        if runtime_seeds:
                            touched.update(await self._expand_closure(db, _RUNTIME_CLOSURE_OF_SEEDS, runtime_seeds))
                        if build_seeds:
                            touched.update(await self._expand_closure(db, _BUILD_CLOSURE_OF_SEEDS, build_seeds))
                        for chunk in batched(sorted(touched), 1000, strict=False):
                            try:
                                await db.execute(TOUCH_PATH_ACCESS, (json.dumps(chunk),))
                                await db.commit()
                            except aiosqlite.Error:
                                with suppress(Exception):
                                    await db.rollback()
                                log.debug("db_flush_chunk_rolled_back", paths=len(chunk))
                                raise
                # Only flushed work leaves the queues. Rework on retry is
                # harmless: touching is an idempotent upsert.
                self.pending_references -= runtime_seeds
                self.pending_build_references -= build_seeds
                elapsed = time.monotonic() - t0
                log.debug(
                    "db_flush_complete",
                    runtime_seeds=len(runtime_seeds),
                    build_seeds=len(build_seeds),
                    touched=len(touched),
                    path_access=self.schema.usable,
                    elapsed_ms=elapsed * 1000,
                )
                break
            except aiosqlite.Error:
                # Lock contention with the daemon or another writer: retry
                # with backoff. A failed flush used to close the whole
                # pool, which left every fast path erroring until a
                # restart; contention is transient, the pool is not the
                # problem, and the next interval retries anyway. Nothing
                # was popped, so no reference is lost, only delayed.
                if attempt >= 2:
                    log.exception("db_flush_failed")
                    break
                await anyio.sleep(self.flush_retry_seconds * (1 << attempt))

    async def _expand_closure(self, db: aiosqlite.Connection, query: str, seeds: set[str]) -> set[str]:
        """The closure of `seeds` under `query`, as path strings."""
        async with db.execute(query, (json.dumps(sorted(seeds)),)) as cursor:
            return {str(row[0]) for row in await cursor.fetchall()}

    def start(self) -> None:
        """Start background regtime flush task. Call from async context."""
        if not self.active or self.flush_task is not None or self.read_only:
            return
        self.flush_task = asyncio.create_task(self.flush_loop())

    async def flush_loop(self) -> None:
        try:
            while True:
                await anyio.sleep(self.reference_flush_interval)
                try:
                    await self.flush_references()
                except aiosqlite.Error:
                    log.exception("db_flush_loop_iteration_failed")
        except anyio.get_cancelled_exc_class():
            with suppress(Exception):
                await self.flush_references()
        except Exception:
            log.exception("db_flush_loop_crashed")

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def close(self) -> None:
        """Stop flush task, flush pending writes, close database."""
        if self.flush_task is not None:
            self.flush_task.cancel()
            with suppress(BaseException):
                await self.flush_task
            self.flush_task = None
        await self.flush_references()
        await self.close_db_pool()

    async def close_db_pool(self) -> None:
        async with self._pool_lock:
            for db in self._all_conns:
                with suppress(Exception):
                    await db.close()
            self._all_conns.clear()
            self._idle_conns.clear()
            self.db_path = None


def resolve_db_path(layout: StoreLayout) -> Path | None:
    """The `db.sqlite` of a store, or `None` when there is none to use.

    Returns `None` rather than raising. `LocalStoreDB.open` is what decides
    whether the SQLite fast paths are available, and `use_db` defaults to
    true, so a store pynixd cannot read has to degrade and not stop the
    daemon.

    This used to `mkdir(parents=True)` the database directory whenever the
    file was missing, and let the `OSError` out. Two things went wrong with
    that. A store root pynixd may not write -- a read-only file system, or a
    store owned by another user -- raised straight out of `open` and took the
    daemon's startup with it. And a path that holds no Nix store at all got
    `nix/var/nix/db/` created inside it by a function named "resolve".

    The directory is still created, because a managed daemon writes its
    database there and `LocalDBStore.start` calls `ensure_daemon` first, so
    the usual case is a directory that already exists. A failure to create it
    now means the fast paths are off, and nothing more.
    """
    # `StoreLayout` answers this for a chroot store and for a relocated one.
    # This used to build `<root>/nix/var/nix/db/db.sqlite` itself, which named
    # the wrong file for a store that `NIX_STORE_DIR` moved. Issue nixidae/nixidae#215.
    db_path = layout.db_path

    if db_path.exists():
        return db_path

    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        log.warning(
            "nix_db_directory_unavailable",
            db_path=str(db_path),
            error=str(exc),
            detail="the SQLite fast paths are off for this store; every query uses the wire",
        )
        return None
    return db_path
