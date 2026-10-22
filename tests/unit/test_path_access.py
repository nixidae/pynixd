"""pynixd records when each store path was last referenced.

The user's design: `registrationTime` is an otherwise unused column, so
refreshing it on every reference turns stock `nix-collect-garbage
--delete-older-than` into an LRU collector for free.

None of it ran. `mark_path` and `mark_paths` had no caller in any project of
this repository, so `pending_references` was always empty,
`flush_references` returned at its first line, and the background task woke
every five seconds to do nothing at all. `LocalDBStore.execute` is the caller
that was missing.

`PynixdPathAccess` is the second half. `registrationTime` says "when this
path entered the store", and `nix path-info --json` reports it as that, so
one number cannot answer both questions afterwards. Issue nixidae/nixidae#205.
`registrationTime` is therefore read, never written, and the flush expands
each queued seed over its closure: the runtime closure for served paths,
the build closure for decided builds, upserting the union. Unmarked paths
resolve their age from `registrationTime` instead. Issue #65 picks the
closure by context.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, closing
from typing import TYPE_CHECKING

import aiosqlite
import pytest
from pydantic import BaseModel, ConfigDict

from pynixd.db_migrations import PATH_ACCESS_TABLE
from pynixd.local_store_db import LocalStoreDB
from pynixd.serde import (
    IsValidPathRequest,
    QueryAllValidPathsRequest,
    QueryValidPathsRequest,
    StorePath,
)
from pynixd.store.local_db import _path_field_names, referenced_paths
from pynixd.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path

HELLO = "/nix/store/00000000000000000000000000000001-hello"
LIBC = "/nix/store/00000000000000000000000000000002-libc"
GONE = "/nix/store/00000000000000000000000000000003-gone"
DRV = "/nix/store/00000000000000000000000000000004-x.drv"


def _store_with_a_closure(tmp_path: Path) -> Path:
    """A store database where `hello` references `libc`."""
    db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
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


async def _access_times(db: LocalStoreDB) -> dict[str, int]:
    async with db.execute(f"SELECT path, lastReferencedAt FROM {PATH_ACCESS_TABLE}") as cursor:
        return {str(row[0]): int(row[1]) for row in await cursor.fetchall()}


def _store_with_a_drv(tmp_path: Path) -> None:
    """A store database where a derivation references `hello`, which references `libc`."""
    db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths ("
            "id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (1, ?, 0)", (DRV,))
        conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (2, ?, 0)", (HELLO,))
        conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (3, ?, 0)", (LIBC,))
        conn.execute("INSERT INTO Refs (referrer, reference) VALUES (1, 2)")
        conn.execute("INSERT INTO Refs (referrer, reference) VALUES (2, 3)")


async def _registration_times(db: LocalStoreDB) -> dict[str, int]:
    async with db.execute("SELECT path, registrationTime FROM ValidPaths") as cursor:
        return {str(row[0]): int(row[1]) for row in await cursor.fetchall()}


class TestReadingThePathsOfARequest:
    """`referenced_paths` reads the declared fields, not a list of operations."""

    def test_a_single_path_field(self) -> None:
        request = IsValidPathRequest(path=StorePath(path=HELLO))
        assert referenced_paths(request) == {HELLO}

    def test_a_set_of_paths(self) -> None:
        request = QueryValidPathsRequest(
            paths={StorePath(path=HELLO), StorePath(path=LIBC)},
        )
        assert referenced_paths(request) == {HELLO, LIBC}

    def test_a_request_that_names_no_path(self) -> None:
        assert referenced_paths(QueryAllValidPathsRequest()) == set()

    def test_an_empty_path_is_not_a_reference(self) -> None:
        """The wire spells "no path" as the empty string, and that is not one."""
        assert referenced_paths(IsValidPathRequest(path=StorePath(path=""))) == set()

    def test_something_that_is_not_a_request(self) -> None:
        assert referenced_paths(object()) == set()


class _Nested(BaseModel):
    """A model the scan never unwraps, so its paths stay uncounted."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    path: StorePath


class _Shapes(BaseModel):
    """One field per annotation shape the filter decides on."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    path: StorePath
    maybe: StorePath | None = None
    many: list[StorePath] = []
    mapping: dict[str, str] = {}
    paths_by_name: dict[str, StorePath] = {}
    nested: _Nested | None = None
    count: int = 0
    name: str = ""


def _full_scan(request: object) -> set[str]:
    """The scan before the per-class filter: every field, every time."""
    found: set[str] = set()
    for name in type(request).model_fields:  # type: ignore[attr-defined]
        value = getattr(request, name, None)
        if isinstance(value, StorePath):
            found.add(str(value))
        elif isinstance(value, (set, frozenset, list, tuple)):
            found.update(str(item) for item in value if isinstance(item, StorePath))
    found.discard("")
    return found


class TestTheFieldFilter:
    """The per-class filter skips fields, never paths."""

    def test_only_path_shaped_fields_are_scanned(self) -> None:
        assert set(_path_field_names(_Shapes)) == {"path", "maybe", "many"}

    def test_the_filter_matches_the_full_scan(self) -> None:
        full = _Shapes(
            path=StorePath(path=HELLO),
            maybe=StorePath(path=LIBC),
            many=[StorePath(path=HELLO)],
            mapping={"path": HELLO},
            paths_by_name={"hello": StorePath(path=HELLO)},
            nested=_Nested(path=StorePath(path=LIBC)),
            count=3,
            name=HELLO,
        )
        assert referenced_paths(full) == _full_scan(full) == {HELLO, LIBC}

    def test_a_mapping_of_paths_stays_uncounted_by_both(self) -> None:
        """The runtime never unwraps a mapping, and neither does the filter.

        A mapping that holds paths is uncounted twice over, on purpose and
        in both places. If either side ever learns to scan mappings, this
        fails, and the other side and the contract in `local_db.py` change
        with it.
        """
        only_a_map = _Shapes(path=StorePath(path=""), paths_by_name={"hello": StorePath(path=HELLO)})
        assert _path_field_names(_Shapes) == ("path", "maybe", "many")
        assert referenced_paths(only_a_map) == _full_scan(only_a_map) == set()

    def test_the_filter_matches_the_full_scan_when_empty(self) -> None:
        assert (
            referenced_paths(_Shapes(path=StorePath(path=""))) == _full_scan(_Shapes(path=StorePath(path=""))) == set()
        )


@pytest.mark.anyio
class TestFlushingTheReferences:
    async def test_a_mark_expands_the_runtime_closure(self, tmp_path: Path) -> None:
        """Serving `hello` freshens `hello` and the `libc` it references.

        The queue holds seeds and the flush expands them: one closure
        read plus small chunked writes per flush, no matter the burst
        size. Unmarked paths resolve their age from `registrationTime`
        instead, so the closure's testimony is missed nowhere that
        matters.

        Perturbation: touch heads only and `libc` rots while `hello`
        stays fresh, although no client can use one without the other.
        """
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_path(HELLO)
            await db.flush_references()

            assert set(await _access_times(db)) == {HELLO, LIBC}

    async def test_the_registration_time_is_never_written(self, tmp_path: Path) -> None:
        """Nix's column is read, never written: entry age stays honest.

        `nix path-info --json` reports `registrationTime` as the entry
        age, and the staleness fallback reads it as exactly that. Bumping
        it on every reference made both lie; the access table carries
        witnessed time now, and this column keeps creation time.
        """
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_path(HELLO)
            await db.flush_references()

            assert all(t == 0 for t in (await _registration_times(db)).values())

    async def test_marking_nothing_writes_nothing(self, tmp_path: Path) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            await db.flush_references()
            assert await _access_times(db) == {}

    async def test_a_second_reference_moves_the_time_forward(self, tmp_path: Path) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_path(HELLO)
            await db.flush_references()
            async with db.acquire_conn() as conn:
                await conn.execute(f"UPDATE {PATH_ACCESS_TABLE} SET lastReferencedAt = 1")
                await conn.commit()

            db.mark_path(HELLO)
            await db.flush_references()

            assert all(t > 1 for t in (await _access_times(db)).values())

    async def test_the_pending_set_is_emptied(self, tmp_path: Path) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_paths([HELLO, LIBC])
            await db.flush_references()
            assert db.pending_references == set()

    async def test_a_derivation_seed_stops_at_the_boundary(self, tmp_path: Path) -> None:
        """A pushed derivation refreshes its head, never its build closure.

        The runtime walk does not traverse out of `.drv` nodes: a
        derivation's references are its build inputs, and walking through
        one would refresh a build closure from a mere observation. Only a
        build decision -- which uses the build walk -- names those paths.

        Perturbation: walk through derivations and every planning query
        that names a `.drv` freshens its whole build closure.
        """
        drv = "/nix/store/00000000000000000000000000000001-x.drv"
        dep = "/nix/store/00000000000000000000000000000002-dep"
        db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
        db_path.parent.mkdir(parents=True)
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.execute(
                "CREATE TABLE ValidPaths ("
                "id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
                "deriver TEXT, registrationTime INTEGER)",
            )
            conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
            conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (1, ?, 0)", (drv,))
            conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (2, ?, 0)", (dep,))
            conn.execute("INSERT INTO Refs (referrer, reference) VALUES (1, 2)")
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_path(drv)
            await db.flush_references()

            assert set(await _access_times(db)) == {drv}


@pytest.mark.anyio
class TestExpandingClosuresByKind:
    """Kind picks the walk, never the table. Issue #65."""

    async def test_a_build_seed_expands_through_derivations(self, tmp_path: Path) -> None:
        """A decided build freshens its whole build closure.

        The build walk follows references everywhere, derivations
        included: the decision is the use-event for the closure, and
        merely pushing the derivation around never records beyond the
        head. One table takes the union either way.
        """
        _store_with_a_drv(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_paths([DRV], kind="build")
            await db.flush_references()

            assert set(await _access_times(db)) == {DRV, HELLO, LIBC}

    async def test_a_runtime_seed_stops_at_derivations(self, tmp_path: Path) -> None:
        _store_with_a_drv(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_paths([DRV])
            await db.flush_references()

            assert set(await _access_times(db)) == {DRV}

    async def test_both_kinds_agree_off_derivations(self, tmp_path: Path) -> None:
        """Below a derivation the two walks are the same walk."""
        _store_with_a_drv(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_paths([HELLO])
            await db.flush_references()
            runtime = set(await _access_times(db))
            async with db.acquire_conn() as conn:
                await conn.execute(f"DELETE FROM {PATH_ACCESS_TABLE}")
                await conn.commit()
            db.mark_paths([HELLO], kind="build")
            await db.flush_references()
            build = set(await _access_times(db))

        assert runtime == build == {HELLO, LIBC}

    async def test_an_unknown_seed_records_nothing(self, tmp_path: Path) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_paths([GONE], kind="build")
            await db.flush_references()

            assert await _access_times(db) == {}

    async def test_both_queues_drain_together(self, tmp_path: Path) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_paths([HELLO])
            db.mark_paths([LIBC], kind="build")
            await db.flush_references()

            assert db.pending_references == set()
            assert db.pending_build_references == set()
            assert set(await _access_times(db)) == {HELLO, LIBC}

    async def test_a_collected_path_leaves_the_table(self, tmp_path: Path) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_paths([HELLO])
            await db.flush_references()
            async with db.acquire_conn() as conn:
                await conn.execute("DELETE FROM ValidPaths WHERE path = ?", (HELLO,))
                await conn.execute("DELETE FROM ValidPaths WHERE path = ?", (LIBC,))
                await conn.commit()

            assert await db.prune_path_access() == 2
            assert await _access_times(db) == {}


@pytest.mark.anyio
class TestFlushingUnderContention:
    """A locked database delays the flush; it never kills the pool.

    Production taught this: a flush that hit `database is locked` closed
    the whole pool, and every fast path errored until a restart. Contention
    with the daemon is normal on a busy store, so the flush re-queues and
    retries, and the pool outlives any single flush.
    """

    async def test_a_locked_flush_retries_and_keeps_the_pool(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            calls = 0
            real = LocalStoreDB.acquire_conn

            @asynccontextmanager
            async def flaky(self: LocalStoreDB) -> AsyncIterator[aiosqlite.Connection]:
                nonlocal calls
                calls += 1
                if calls <= 2:
                    raise sqlite3.OperationalError("database is locked")
                async with real(self) as conn:
                    yield conn

            monkeypatch.setattr(LocalStoreDB, "acquire_conn", flaky)
            db.flush_retry_seconds = 0.0
            db.mark_path(HELLO)
            await db.flush_references()

            assert calls == 3
            assert db.active
            assert set(await _access_times(db)) == {HELLO, LIBC}

    async def test_a_hopeless_flush_requeues_and_surrenders_loudly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Three strikes: the marks go back, the pool stays, the error logs."""
        db_path = _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            calls = 0

            @asynccontextmanager
            async def always_locked(self: LocalStoreDB) -> AsyncIterator[aiosqlite.Connection]:
                nonlocal calls
                calls += 1
                raise sqlite3.OperationalError("database is locked")
                yield

            monkeypatch.setattr(LocalStoreDB, "acquire_conn", always_locked)
            db.flush_retry_seconds = 0.0
            db.mark_path(HELLO)
            await db.flush_references()

            assert calls == 3
            assert db.active
            assert db.pending_references == {HELLO}
            # The mock still fails every acquire, so read past it: nothing
            # was written in three attempts.
            with closing(sqlite3.connect(db_path)) as conn:
                assert conn.execute(f"SELECT * FROM {PATH_ACCESS_TABLE}").fetchall() == []


@pytest.mark.anyio
class TestAskingTheTable:
    async def test_an_old_path_is_reported_and_a_fresh_one_is_not(self, tmp_path: Path) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_path(HELLO)
            await db.flush_references()
            async with db.acquire_conn() as conn:
                await conn.execute(
                    f"UPDATE {PATH_ACCESS_TABLE} SET lastReferencedAt = ? WHERE path = ?",
                    (int(time.time()) - 90_000, LIBC),
                )
                await conn.commit()

            stale = await db.query_paths_not_referenced_since(86_400)

            assert stale is not None
            assert {str(p) for p in stale} == {LIBC}

    async def test_a_path_the_store_no_longer_holds_is_pruned(self, tmp_path: Path) -> None:
        """The join that keeping the table inside Nix's database buys."""
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            async with db.acquire_conn() as conn:
                await conn.execute(
                    f"INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt) VALUES (?, 1)",
                    (GONE,),
                )
                await conn.commit()

            removed = await db.prune_path_access()

            assert removed == 1
            assert GONE not in await _access_times(db)

    async def test_pruning_an_untouched_table_removes_nothing(self, tmp_path: Path) -> None:
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            db.mark_path(HELLO)
            await db.flush_references()
            assert await db.prune_path_access() == 0

    async def test_a_path_without_a_row_falls_back_to_registration_time(self, tmp_path: Path) -> None:
        """Never seen by the tracker, old by Nix's column: stale, by the fallback.

        This is the unwatched buildup: dead paths that predate tracking
        have no access row, and `registrationTime` is their only date.
        The fixture registers both paths at time zero and writes no rows.

        Perturbation: read the access table alone and the set comes back empty.
        """
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            stale = await db.query_paths_not_referenced_since(86_400)

            assert stale is not None
            assert {str(p) for p in stale} == {HELLO, LIBC}

    async def test_a_fresh_registration_time_spares_the_unseen(self, tmp_path: Path) -> None:
        """No row, but Nix just registered it: not stale.

        Newly built outputs land here: they exist in no access row yet,
        and their fresh registration time is what keeps the collector off
        them until the tracker sees them live.
        """
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            async with db.acquire_conn() as conn:
                await conn.execute("UPDATE ValidPaths SET registrationTime = ?", (int(time.time()),))
                await conn.commit()

            stale = await db.query_paths_not_referenced_since(86_400)

            assert stale is not None
            assert {str(p) for p in stale} == set()

    async def test_a_row_wins_over_registration_time_either_way(self, tmp_path: Path) -> None:
        """The access row is the witnessed date; Nix's column loses on conflict."""
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            now = int(time.time())
            async with db.acquire_conn() as conn:
                await conn.execute(
                    f"INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt) VALUES (?, ?)",
                    (HELLO, now),
                )
                await conn.execute(
                    f"INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt) VALUES (?, ?)",
                    (LIBC, now - 90_000),
                )
                await conn.execute("UPDATE ValidPaths SET registrationTime = ? WHERE path = ?", (now, LIBC))
                await conn.commit()

            stale = await db.query_paths_not_referenced_since(86_400)

            assert stale is not None
            # HELLO: seen just now, registered at time zero -- the row spares it.
            # LIBC: seen long ago, registered just now -- the row condemns it.
            assert {str(p) for p in stale} == {LIBC}

    async def test_access_times_fall_back_to_registration_time(self, tmp_path: Path) -> None:
        """The weigher resolves age the same way the planner does."""
        _store_with_a_closure(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
            now = int(time.time())
            async with db.acquire_conn() as conn:
                await conn.execute(
                    f"INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt) VALUES (?, ?)",
                    (HELLO, now),
                )
                await conn.commit()

            times = await db.query_access_times([HELLO, LIBC, GONE])

            assert times is not None
            # HELLO carries its witnessed date, LIBC Nix's, and GONE --
            # in no `ValidPaths` row -- stays unknown instead of leaking in
            # through a dangling access row.
            assert times == {HELLO: now, LIBC: 0}
