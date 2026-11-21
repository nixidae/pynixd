"""pynixd's queries of the local Nix store database.

A caller reads with `fetchone` and `fetchall`, writes in `transaction`, and
holds a `Session` (`database.py`) for anything longer. The backend
(`sqlite_db.SqliteBackend`) holds the connections; no driver is imported here.

Reference updates are batched writes into pynixd's own access table:
heads only, never closures. Marking one path used to touch its whole
transitive closure -- tens of thousands of rows in one write transaction
against the daemon's own database, every few seconds -- for paths nobody
observed. A row now means exactly "named in traffic"; anything untouched
resolves its age from `registrationTime` instead. The access tracker never
writes Nix's own tables: not even `registrationTime`, which Nix writes once at
registration and `nix path-info` reports as the entry age. The one writer of
Nix's tables is `store/register.py`, behind `pynixd_writes`.

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

import os
import statistics
import time
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from itertools import batched
from typing import TYPE_CHECKING, Literal, Self

import anyio
import anyio.to_thread
import structlog

from .build_stats import BuildEstimateSource, ExpectedBytes, StoreLink
from .database import Backend, DatabaseError, Params, Row, Session
from .db_migrations import SchemaState
from .held_by_tables import HeldByTables
from .memory_model import MemoryPoint, fit
from .nix_tables import NixTables
from .placement_tables import PlacementTables
from .provisioning_tables import ProvisioningTables
from .sql import SQLITE
from .sqlite_db import SqliteBackend, SyncReader as SyncReader
from .store_layout import StoreLayout
from .store_path import StorePath

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager
    from pathlib import Path

    from nix_daemon_protocol.aliases import StorePathSet

    from .background import Background
    from .build_stats import BuildObservation, DerivationIdentity
    from .database import SyncSession
    from .memory_model import MemoryModel
    from .sql import Statements

log = structlog.get_logger(__name__)

__all__ = ["DatabaseError", "LocalStoreDB", "RootAttribution", "Session", "SyncReader"]


MarkKind = Literal["runtime", "build"]
"""Which seed set a mark joins, and so which closure the flush expands.

`runtime` is observed use: a path a client names. `build` is a decided
build: the derivation plus the inputs its request names. Both expand to
path lists at flush and upsert the same freshness column -- kind lives
only at gather time, in which CTE runs, never in what is stored. The
runtime walk stops at derivation boundaries, so pushing a derivation
around refreshes its head and never its build closure. Issue #65.
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

LINK_ROWS = 50
"""A store's newest rows that its link rates read: recent enough to follow a
change of network, many enough that one stalled transfer does not decide."""

_DEFAULT_REFERENCE_FLUSH_INTERVAL = 5.0


def _bytes_per_s(transfers: list[tuple[int, int]]) -> float | None:
    """Total bytes over total time, for `(bytes, ms)` pairs."""
    total_ms = sum(ms for _, ms in transfers)
    return sum(size for size, _ in transfers) * 1000 / total_ms if total_ms else None


class LocalStoreDB:
    """The store database: pynixd's queries over the connections of its backend.

    Operation types implement their own DB logic via ``execute_db(db)``.
    `backend` holds the connections and everything that knows the engine.

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
        *,
        immutable: bool = False,
        backend: Backend | None = None,
    ) -> None:
        """*backend* holds a database that is not a file; without it, *db_path* names the SQLite file."""
        self.store_path = store_path
        self.read_only: bool = read_only or immutable
        self._backend: Backend | None = (
            backend
            if backend is not None or db_path is None
            else SqliteBackend(db_path, read_only=self.read_only, immutable=immutable, max_conns=max_conns)
        )
        self._synchronous: str | None = None
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
        the SQL statement wants the text. `pynixd_store.store_path.StorePath`
        keeps the path without the `/nix/store/` prefix and adds it back
        in `__str__`, and the wire `StorePath` is a `str` of the whole path.
        """
        self.pending_build_references: set[str] = set()
        """Seeds for build-closure expansion: decided derivations plus the
        inputs their requests name. The flush expands them over the build
        closure into the same access table runtime seeds reach: kind lives
        in which closure runs, never in what is stored. See `MarkKind`.
        """

        self.flush_scope: anyio.CancelScope | None = None
        self._flush_ended: anyio.Event | None = None

        self.schema = SchemaState(version=0, usable=False, reason="the schema was never checked")
        """The state of the tables pynixd owns. `open` replaces it.

        It starts unusable, so a `LocalStoreDB` built by hand answers no query
        that needs one of those tables. `active` alone cannot say: it reports
        that Nix's own tables are readable, which is a different question.
        """

        self.nix = NixTables(self)
        """The queries and writes of Nix's own tables."""

        self.placements = PlacementTables(self)
        """The scheduler's placement trace tables."""

        self.provisioned = ProvisioningTables(self)
        """The builder machines the provisioner started."""

        self.held_by = HeldByTables(self)
        """The paths a cold tier or a stable substituter holds."""

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
    def backend(self) -> Backend:
        """The engine of this database; `DatabaseError` when the store has none."""
        if self._backend is None:
            raise DatabaseError("the store has no database")
        return self._backend

    @property
    def active(self) -> bool:
        return self._backend is not None

    @property
    def db_path(self) -> Path | None:
        """The SQLite file, or `None` for a database that is not one."""
        return self._backend.db_path if isinstance(self._backend, SqliteBackend) else None

    @property
    def sql(self) -> Statements:
        """The statements of this database, which the backend picks when it opens."""
        return SQLITE if self._backend is None else self._backend.sql

    @property
    def synchronous(self) -> str | None:
        """`pragma synchronous` for a writing connection: `NORMAL`, or `OFF` without `fsync-metadata`
        (`local-store.cc:536`). `None` until the engine reads its settings; SQLite's default stands until then."""
        return self._synchronous

    @synchronous.setter
    def synchronous(self, mode: str | None) -> None:
        self._synchronous = mode
        if self._backend is not None:
            self._backend.synchronous = mode

    def sync_reader(self) -> SyncReader | None:
        """A read-only connection for one client session, or `None` when none.

        The caller owns it and closes it when the session ends. A store with
        no readable database gets `None`, and every caller then keeps the
        pooled `aiosqlite` path. The connection is opened by the first query,
        and `SyncReader` refuses and reports `None` from there if the store
        will not give one, so a store that only `aiosqlite` can read still
        answers every query over the pool.
        """
        return None if self._backend is None else self._backend.sync_reader()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Session]:
        """A pooled connection for one block; a refusal of the database raises `DatabaseError`.

        A write in the block goes in `session.transaction()`, or `transaction` here for a block that only writes.
        """
        async with self.backend.connection() as session:
            yield session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Session]:
        """One write transaction on a pooled connection: committed when the block ends, rolled back when it raises."""
        async with self.connection() as session, session.transaction():
            yield session

    def snapshot(self) -> AbstractAsyncContextManager[Session]:
        """One read transaction on a connection of its own (`Database.snapshot`)."""
        return self.backend.database().snapshot()

    def sync_session(self, *, write: bool = False) -> AbstractContextManager[SyncSession]:
        """A worker thread's own connection (`Database.sync_session`)."""
        return self.backend.database().sync_session(write=write)

    async def fetchone(self, sql: str, params: Params = ()) -> Row | None:
        async with self.connection() as session:
            return await session.fetchone(sql, params)

    async def fetchall(self, sql: str, params: Params = ()) -> list[Row]:
        async with self.connection() as session:
            return await session.fetchall(sql, params)

    @classmethod
    async def open(
        cls,
        layout: StoreLayout,
        reference_flush_interval: float = _DEFAULT_REFERENCE_FLUSH_INTERVAL,
        *,
        read_only: bool = False,
    ) -> LocalStoreDB:
        """Open the Nix store database. Returns an instance (possibly with no DB).

        This never raises. A store whose database pynixd cannot read gets an
        inactive instance, every fast path of `LocalDBStore` declines, and
        `DaemonStore.execute` uses the wire. That is what lets `use_db` default
        to true.

        *read_only* opens a database pynixd must not change even where it
        can, such as the lower store of an overlay: no migration writes to it.
        """
        db_path = await anyio.to_thread.run_sync(resolve_db_path, layout)
        if db_path is None:
            return cls.inactive(layout, reference_flush_interval=reference_flush_interval)

        read_only = (
            read_only or layout.read_only or not await anyio.to_thread.run_sync(os.access, db_path.parent, os.W_OK)
        )

        instance = cls(
            db_path=db_path,
            store_path=layout.real_store_dir,
            read_only=read_only,
            reference_flush_interval=reference_flush_interval,
            immutable=layout.immutable,
        )
        try:
            await instance.backend.probe()
        except DatabaseError as e:
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

        instance.schema = await instance.backend.migrate()
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
            provenance=instance.sql.nix.provenance,
        )
        return instance

    @classmethod
    async def open_backend(
        cls,
        backend: Backend,
        layout: StoreLayout,
        reference_flush_interval: float = _DEFAULT_REFERENCE_FLUSH_INTERVAL,
    ) -> LocalStoreDB:
        """Open the store database that *backend* holds, or raise `DatabaseError`.

        Not `open`, which never raises: a store whose database is a server has no daemon to fall back to,
        so a server that refuses stops the start. The backend is closed before the error goes out.
        """
        instance = cls(
            db_path=None,
            store_path=layout.real_store_dir,
            read_only=False,
            reference_flush_interval=reference_flush_interval,
            backend=backend,
        )
        try:
            await backend.probe()
            instance.schema = await backend.migrate()
        except BaseException:
            await instance.close_db_pool()
            raise
        if not instance.schema.usable:
            log.warning(
                "pynixd_tables_unavailable",
                engine=type(backend).__name__,
                version=instance.schema.version,
                reason=instance.schema.reason,
            )
        log.info(
            "local_store_db_active",
            engine=type(backend).__name__,
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
            rows = await self.fetchall(self.sql.path_access.unreferenced_since, (cutoff,))
            return {StorePath(r[0]) for r in rows}
        except DatabaseError:
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
            async with self.connection() as db:
                try:
                    await db.execute(self.sql.liveness.create_root_hits)
                    await db.execute(self.sql.liveness.fill_root_hits)
                    full: dict[tuple[str, ...], tuple[int, int]] = {}
                    for seeds in groups:
                        params = (self.sql.codec.path_set(seeds),)
                        counts = await db.fetchone(self.sql.liveness.root_full_counts, params)
                        full[seeds] = (int(counts[0]), int(counts[1])) if counts is not None else (0, 0)
                        await db.execute(
                            self.sql.liveness.mark_root_hits, (len(groups[seeds]), self.sql.codec.path_set(seeds))
                        )
                    report: list[RootAttribution] = []
                    for seeds, labels in groups.items():
                        counts = await db.fetchone(
                            self.sql.liveness.root_exclusive_counts, (self.sql.codec.path_set(seeds),)
                        )
                        exclusive = (int(counts[0]), int(counts[1])) if counts is not None else (0, 0)
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
                        await db.execute(self.sql.liveness.drop_root_hits)
        except DatabaseError:
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
            path_set = self.sql.codec.path_set(sorted(set(paths)))
            rows = await self.fetchall(self.sql.path_access.referrer_closure, (path_set,))
            return {r[0] for r in rows}
        except DatabaseError:
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
            path_set = self.sql.codec.path_set(sorted(set(paths)))
            rows = await self.fetchall(self.sql.path_access.weights, (path_set,))
            return {r[0]: (r[1] or 0, None if r[2] is None else int(r[2])) for r in rows}
        except (DatabaseError, ValueError):
            log.debug("query_weights_failed", exc_info=True)
            return None

    async def checkpoint_wal(self) -> bool:
        """Checkpoint the WAL (`SqliteBackend.checkpoint_wal`). `False` when this store has no database to checkpoint."""
        if self._backend is None or self.read_only:
            return False
        return await self._backend.checkpoint_wal()

    async def prune_path_access(self) -> int:
        """Drop the rows for paths the store no longer holds. Returns the count.

        This is the join that keeping pynixd's tables inside Nix's database
        buys: one statement compares `PynixdPathAccess` against `ValidPaths`.
        Without it the table grows for ever, because a path that the garbage
        collector deletes leaves its access time behind.
        """
        prune = self.sql.path_access.prune
        if prune is None or not self.active or self.read_only or not self.schema.usable:
            return 0
        try:
            async with self.transaction() as db:
                removed = await db.execute(prune)
        except DatabaseError:
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
        """Store one build's cost, and keep the newest `OBSERVATIONS_KEPT` of its identity and store, and its newest `BUILT`.

        Answers the row id, or `None` when nothing was written.
        """
        if not self.active or self.read_only or not self.schema.usable:
            return None
        key = (identity.name, identity.system, identity.shape, observation.store_id)
        try:
            async with self.transaction() as db:
                await db.execute(
                    self.sql.observations.insert_derivation_shape,
                    (
                        identity.shape,
                        self.sql.codec.string_list(identity.keys),
                        self.sql.codec.string_list(identity.features),
                    ),
                )
                row_id = await db.insert(
                    self.sql.observations.insert_build_observation,
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
                        observation.resource_source,
                        observation.peak_memory_bytes,
                        observation.io_read_bytes,
                        observation.io_write_bytes,
                        observation.cpu_stall_us,
                        observation.memory_stall_us,
                        observation.io_stall_us,
                        observation.host_resource_source,
                        observation.host_cpu_user_us,
                        observation.host_cpu_system_us,
                        observation.host_peak_memory_bytes,
                        observation.cores,
                        observation.oom_kills,
                    ),
                )
                await db.executemany(
                    self.sql.observations.insert_build_observation_sample,
                    [
                        (row_id, s.at_ms, s.cpu_us, s.memory_bytes, s.io_read_bytes, s.io_write_bytes)
                        for s in observation.samples
                    ],
                )
                # The samples first: the subquery finds the rows the next statement removes.
                pruned = dict(zip(("name", "system", "shape", "store"), key, strict=True), kept=OBSERVATIONS_KEPT)
                await db.execute(self.sql.observations.prune_build_observation_samples, pruned)
                await db.execute(self.sql.observations.prune_build_observations, pruned)
        except DatabaseError:
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
                (
                    self.sql.observations.build_ms_of_shape,
                    (identity.name, identity.system, identity.shape, OBSERVATIONS_KEPT),
                ),
                (self.sql.observations.build_ms_of_name, (identity.name, identity.system, OBSERVATIONS_KEPT)),
            ):
                rows = await self.fetchall(query, params)
                if rows:
                    return int(statistics.median(int(row[0]) for row in rows))
        except DatabaseError:
            log.debug("expected_build_ms_failed", name=identity.name, exc_info=True)
        return None

    async def memory_model(self, identity: DerivationIdentity) -> MemoryModel | None:
        """The peak memory of the identity's recent `BUILT` and killed attempts, on any store, as a function of cores.

        A VM build counts its host peak: the guest's RAM is what the host's available memory loses. Falls back to the
        name and system under any shape, as `expected_build_ms` does.
        """
        if not self.active or not self.schema.usable:
            return None
        try:
            for query, params in (
                (
                    self.sql.observations.memory_points_of_shape,
                    (identity.name, identity.system, identity.shape, OBSERVATIONS_KEPT),
                ),
                (self.sql.observations.memory_points_of_name, (identity.name, identity.system, OBSERVATIONS_KEPT)),
            ):
                rows = await self.fetchall(query, params)
                if rows:
                    return fit(
                        MemoryPoint(cores=int(cores or 0), peak_bytes=int(peak), killed=bool(kills))
                        for cores, peak, kills in rows
                    )
        except DatabaseError:
            log.debug("memory_model_failed", name=identity.name, exc_info=True)
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
                (
                    BuildEstimateSource.SHAPE_ON_STORE,
                    self.sql.observations.build_ms_of_shape_on_store,
                    (*shape, store_id),
                ),
                (BuildEstimateSource.SHAPE, self.sql.observations.build_ms_of_shape, shape),
                (BuildEstimateSource.NAME, self.sql.observations.build_ms_of_name, (identity.name, identity.system)),
            ):
                rows = await self.fetchall(query, (*params, OBSERVATIONS_KEPT))
                if rows:
                    return int(statistics.median(int(row[0]) for row in rows)), source
        except DatabaseError:
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
            rows = await self.fetchall(self.sql.observations.store_link, (store_id, LINK_ROWS))
        except DatabaseError:
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

    async def expected_bytes(self, identity: DerivationIdentity, store_id: str, *, local_id: str) -> ExpectedBytes:
        """The median bytes the identity sent to this store, and pulled from any store but *local_id*."""
        if not self.active or not self.schema.usable:
            return ExpectedBytes()
        shape = (identity.name, identity.system, identity.shape)
        try:
            inputs = [
                int(r[0])
                for r in await self.fetchall(self.sql.observations.inputs_bytes, (*shape, store_id, OBSERVATIONS_KEPT))
            ]
            pulls = [
                int(r[0])
                for r in await self.fetchall(self.sql.observations.pull_bytes, (*shape, local_id, OBSERVATIONS_KEPT))
            ]
        except DatabaseError:
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
                async with self.connection() as db:
                    touched: set[str] = set()
                    # The walks read; only the chunks below write, one
                    # transaction each: a failed chunk rolls back to the
                    # last commit instead of leaking into the pool, and a
                    # short write holds the lock briefly enough that the
                    # daemon's own writers get in between chunks.
                    if self.schema.usable:
                        if runtime_seeds:
                            touched.update(
                                await self._expand_closure(db, self.sql.path_access.runtime_closure, runtime_seeds)
                            )
                        if build_seeds:
                            touched.update(
                                await self._expand_closure(db, self.sql.path_access.build_closure, build_seeds)
                            )
                        for chunk in batched(sorted(touched), 1000, strict=False):
                            try:
                                async with db.transaction():
                                    await db.execute(self.sql.path_access.touch, (self.sql.codec.path_set(chunk),))
                            except DatabaseError:
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
            except DatabaseError:
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

    async def _expand_closure(self, db: Session, query: str, seeds: set[str]) -> set[str]:
        """The closure of `seeds` under `query`, as path strings."""
        return {str(row[0]) for row in await db.fetchall(query, (self.sql.codec.path_set(sorted(seeds)),))}

    def start(self, background: Background) -> None:
        """Flush the references in *background*, the loops of the store that owns this database."""
        if not self.active or self.flush_scope is not None or self.read_only:
            return
        self.flush_scope = background.spawn(self.flush_loop, anyio.Event())

    async def flush_loop(self, ended: anyio.Event) -> None:
        """Flush every interval. `close` flushes what is left."""
        # Set here, not in `start`: a task cancelled before its first step never runs, and never sets *ended*.
        self._flush_ended = ended
        try:
            while True:
                await anyio.sleep(self.reference_flush_interval)
                try:
                    await self.flush_references()
                except DatabaseError:
                    log.exception("db_flush_loop_iteration_failed")
        finally:
            ended.set()

    # ── Lifecycle ─────────────────────────────────────────────────────

    async def close(self) -> None:
        """Stop flush task, flush pending writes, close database."""
        if self.flush_scope is not None:
            self.flush_scope.cancel()
            self.flush_scope = None
        # The loop may be inside a flush, on a connection the pool closes below.
        if self._flush_ended is not None:
            await self._flush_ended.wait()
            self._flush_ended = None
        await self.flush_references()
        await self.close_db_pool()

    async def close_db_pool(self) -> None:
        if self._backend is not None:
            await self._backend.close()
            self._backend = None


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
