"""The SQL plan answers what the Python plan answered, on the same store, on each engine.

`SqlPlan` moved the age rule's plan into SQLite (#1134). Each test here
builds a random store -- a reference DAG with sizes, registration times,
access rows, a live set and a veto -- and checks the SQL answer against
the set algebra the collector did in Python: the plan, the weight order,
the bound, the re-closed batch and the delete order.
"""

from __future__ import annotations

import random
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING

import psycopg
import pytest

from pynixd_store.database import DatabaseError
from pynixd_store.db_migrations import PATH_ACCESS_TABLE, apply_migrations
from pynixd_store.gc import by_weight, take_until_below_target
from pynixd_store.gc_sql import Bound, SqlPlan, referrer_first
from pynixd_store.postgres_db import PostgresBackend, PostgresDatabase
from pynixd_store.sql import POSTGRES, SQLITE
from pynixd_store.sqlite_db import SqliteDatabase

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd_store.database import Database

NOW = 1_800_000_000
MAX_AGE = 86_400


@dataclass
class Store:
    database: Database
    paths: list[str]
    refs: dict[str, set[str]]
    size: dict[str, int]
    seen: dict[str, int]
    live: set[str]
    vetoed: set[str]


@pytest.fixture(params=["sqlite", "postgres"])
def conninfo(request: pytest.FixtureRequest) -> str | None:
    """`None` for SQLite; the conninfo of an empty database for PostgreSQL."""
    return None if request.param == "sqlite" else str(request.getfixturevalue("postgres_conninfo"))


def _down(refs: dict[str, set[str]], seeds: set[str]) -> set[str]:
    """*seeds* and everything they reference, transitively."""
    closed = set(seeds)
    queue = list(seeds)
    while queue:
        for reference in refs.get(queue.pop(), set()):
            if reference not in closed:
                closed.add(reference)
                queue.append(reference)
    return closed


def _up(refs: dict[str, set[str]], seeds: set[str]) -> set[str]:
    """*seeds* and everything that references them, transitively."""
    referrers: dict[str, set[str]] = {}
    for referrer, references in refs.items():
        for reference in references:
            referrers.setdefault(reference, set()).add(referrer)
    return _down(referrers, seeds)


async def _sqlite(
    db: Path, paths: list[tuple[int, str, int, int]], refs: list[tuple[int, int]], accessed: list[tuple[str, int]]
) -> Database:
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, deriver TEXT, "
            "registrationTime INTEGER, narSize INTEGER)"
        )
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        conn.executemany("INSERT INTO ValidPaths (id, path, registrationTime, narSize) VALUES (?, ?, ?, ?)", paths)
        conn.executemany("INSERT INTO Refs VALUES (?, ?)", refs)
    assert (await apply_migrations(db, read_only=False)).usable
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.executemany(f"INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt) VALUES (?, ?)", accessed)
    return SqliteDatabase(db)


async def _postgres(
    conninfo: str, paths: list[tuple[int, str, int, int]], refs: list[tuple[int, int]], accessed: list[tuple[str, int]]
) -> Database:
    backend = PostgresBackend(conninfo, POSTGRES)
    try:
        await backend.probe()
        assert (await backend.migrate()).usable
        async with backend.connection() as session, session.transaction():
            await session.executemany(
                "INSERT INTO ValidPaths (id, path, hash, registrationTime, narSize) VALUES (%s, %s, decode(repeat('00', 32), 'hex'), %s, %s)",
                paths,
            )
            await session.executemany("INSERT INTO Refs VALUES (%s, %s)", refs)
            await session.executemany(
                f"INSERT INTO {PATH_ACCESS_TABLE} (pathId, lastReferencedAt) "
                "SELECT id, %s FROM ValidPaths WHERE path = %s",
                [(stamp, path) for path, stamp in accessed],
            )
    finally:
        await backend.close()
    return PostgresDatabase(conninfo, POSTGRES)


async def _store(tmp_path: Path, seed: int, conninfo: str | None, count: int = 120) -> Store:
    rng = random.Random(seed)
    paths = [f"/nix/store/{index:032d}-p{index}" for index in range(count)]
    # A DAG: each path refers only to paths before it, and some to themselves.
    refs = {
        path: {paths[j] for j in rng.sample(range(index), k=min(index, rng.randint(0, 4)))}
        | ({path} if rng.random() < 0.1 else set())
        for index, path in enumerate(paths)
    }
    size = {path: rng.choice([0, rng.randint(1, 10**9)]) for path in paths}
    registered = {path: NOW - rng.randint(0, 3 * MAX_AGE) for path in paths}
    accessed = {path: NOW - rng.randint(0, 2 * MAX_AGE) for path in rng.sample(paths, k=count // 3)}
    live = _down(refs, set(rng.sample(paths[count // 2 :], k=4)))
    vetoed = _down(refs, set(rng.sample(paths, k=3)))
    ids = {path: index + 1 for index, path in enumerate(paths)}
    path_rows = [(ids[path], path, registered[path], size[path]) for path in paths]
    ref_rows = [(ids[referrer], ids[reference]) for referrer, references in refs.items() for reference in references]
    if conninfo is None:
        database = await _sqlite(tmp_path / f"db-{seed}.sqlite", path_rows, ref_rows, list(accessed.items()))
    else:
        database = await _postgres(conninfo, path_rows, ref_rows, list(accessed.items()))
    seen = registered | accessed
    return Store(database, paths, refs, size, seen, live, vetoed)


def _python_plan(store: Store, cutoff: int | None) -> set[str]:
    dead = set(store.paths) - store.live
    stale = dead if cutoff is None else {path for path in dead if store.seen[path] < cutoff}
    return _up(store.refs, stale) & dead


def _python_close(store: Store, batch: list[str], planned: set[str]) -> list[str]:
    kept: dict[str, None] = {}
    for seed in batch:
        closed = _up(store.refs, {seed})
        if closed - planned:
            continue
        for path in closed:
            kept.setdefault(path)
    order = {path: index for index, path in enumerate(batch)}
    return sorted(kept, key=lambda path: (0, order[path]) if path in order else (1, path))


SEEDS = range(12)


@pytest.mark.anyio
@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("pressed", [False, True])
async def test_the_plan_and_its_order_match_python(
    tmp_path: Path, conninfo: str | None, seed: int, pressed: bool
) -> None:
    store = await _store(tmp_path, seed, conninfo)
    cutoff = None if pressed else NOW - MAX_AGE
    planned = _python_plan(store, cutoff)
    weights = {path: (store.size[path], max(0, NOW - store.seen[path])) for path in planned - store.vetoed}
    pressure = random.Random(seed).random()

    async with SqlPlan.open(store.database) as plan:
        counts = await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=cutoff)
        rows = await plan.weighed(pressure=pressure, now=NOW, bound=None, limit=None)

    assert counts.planned == len(planned)
    assert counts.spared == len(planned & store.vetoed)
    # Every dead path the veto leaves, whatever the age rule: what a full collection could free.
    assert counts.reclaimable_bytes == sum(store.size[path] for path in set(store.paths) - store.live - store.vetoed)
    assert [path for path, _, _ in rows] == by_weight(weights, pressure)
    assert {path: (size, age) for path, size, age in rows} == weights


@pytest.mark.anyio
@pytest.mark.parametrize("ratio", [1.0, 0.7])
@pytest.mark.parametrize("seed", SEEDS)
async def test_the_bound_takes_what_python_took(tmp_path: Path, conninfo: str | None, seed: int, ratio: float) -> None:
    """At a ratio under one each row frees less, so the same target takes a longer prefix."""
    store = await _store(tmp_path, seed, conninfo)
    planned = _python_plan(store, None)
    weights = {path: (store.size[path], max(0, NOW - store.seen[path])) for path in planned - store.vetoed}
    ordered = by_weight(weights, 0.8)
    total = 10**11
    freeable = sum(size for size, _ in weights.values())
    bound = Bound(used=int(total * 0.8), total=total, target=0.8 - freeable / total / 2, ratio=ratio)

    async with SqlPlan.open(store.database) as plan:
        await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=None)
        rows = await plan.weighed(pressure=0.8, now=NOW, bound=bound, limit=None)
        limited = await plan.weighed(pressure=0.8, now=NOW, bound=bound, limit=3)

    expected = take_until_below_target(
        [(path, weights[path][0]) for path in ordered], bound.used, bound.total, bound.target, bound.ratio
    )
    assert [path for path, _, _ in rows] == expected
    assert [path for path, _, _ in limited] == expected[:3]


@pytest.mark.anyio
@pytest.mark.parametrize("seed", SEEDS)
async def test_a_cut_batch_recloses_and_orders_as_python_did(tmp_path: Path, conninfo: str | None, seed: int) -> None:
    store = await _store(tmp_path, seed, conninfo)
    planned = _python_plan(store, None)
    weights = {path: (store.size[path], max(0, NOW - store.seen[path])) for path in planned - store.vetoed}
    head = by_weight(weights, 0.5)[: max(1, len(weights) // 3)]

    async with SqlPlan.open(store.database) as plan:
        await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=None)
        closed = await plan.close_batch(head)
        ordered = await plan.order_batch(closed)

    expected = _python_close(store, head, planned)
    assert closed == expected
    assert ordered == referrer_first(expected, store.refs)


def _delete_refs(store: Store, conninfo: str | None) -> None:
    """Delete every reference on a connection of its own, as a writer beside the plan."""
    if conninfo is None:
        assert isinstance(store.database, SqliteDatabase)
        with closing(sqlite3.connect(store.database.db_path, timeout=0, isolation_level=None)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("DELETE FROM Refs")
            writer.execute("COMMIT")
    else:
        with psycopg.connect(conninfo, autocommit=True) as server:
            server.execute("DELETE FROM Refs")


@pytest.mark.anyio
async def test_an_open_plan_does_not_block_a_writer(tmp_path: Path, conninfo: str | None) -> None:
    """The plan reads one snapshot and writes only TEMP tables: Nix's write lock stays free."""
    store = await _store(tmp_path, 0, conninfo)
    if isinstance(store.database, SqliteDatabase):
        with closing(sqlite3.connect(store.database.db_path)) as conn:
            conn.execute("PRAGMA journal_mode=WAL")

    async with SqlPlan.open(store.database) as plan:
        await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=None)
        _delete_refs(store, conninfo)
        batch = sorted(_python_plan(store, None) - store.vetoed)
        # The snapshot still holds the references the plan was built on.
        assert await plan.order_batch(batch) == referrer_first(batch, store.refs)


@pytest.mark.anyio
async def test_a_refused_plan_raises_database_error(tmp_path: Path, conninfo: str | None) -> None:
    """The collector fails closed on `DatabaseError` (`pynixd/gc.py`), so the plan raises that and not the driver's."""
    if conninfo is None:
        db = tmp_path / "empty.sqlite"
        sqlite3.connect(db).close()
        database: Database = SqliteDatabase(db)
        refusal = "no such table"
    else:
        database = PostgresDatabase(conninfo, POSTGRES)
        refusal = "does not exist"
    with pytest.raises(DatabaseError, match=refusal):
        async with SqlPlan.open(database) as plan:
            await plan.plan(live=set(), vetoed=set(), cutoff=None)


COLD = "https://cold.example/bucket"


async def _record(store: Store, conninfo: str | None, key: str, paths: set[str]) -> None:
    """Rows of `PynixdHeldBy`, written by the statement pynixd writes them with."""
    if conninfo is None:
        assert isinstance(store.database, SqliteDatabase)
        with closing(sqlite3.connect(store.database.db_path)) as conn, conn:
            params = {"store": key, "paths": SQLITE.codec.path_set(sorted(paths)), "at": NOW}
            conn.execute(SQLITE.held_by.record, params)
        return
    backend = PostgresBackend(conninfo, POSTGRES)
    try:
        await backend.probe()
        async with backend.connection() as session, session.transaction():
            params = {"store": key, "paths": POSTGRES.codec.path_set(sorted(paths)), "at": NOW}
            await session.execute(POSTGRES.held_by.record, params)
    finally:
        await backend.close()


@pytest.mark.anyio
@pytest.mark.parametrize("seed", SEEDS)
async def test_the_cover_keeps_each_uncovered_path_and_its_closure(
    tmp_path: Path, conninfo: str | None, seed: int
) -> None:
    """nixidae/nixidae#1417: what a cold tier covers goes, and the rest stays whole. Rows of another store do not count."""
    store = await _store(tmp_path, seed, conninfo)
    rng = random.Random(seed)
    planned = _python_plan(store, None)
    covered = set(rng.sample(sorted(planned), k=len(planned) * 2 // 3))
    await _record(store, conninfo, COLD, covered)
    await _record(store, conninfo, "https://elsewhere.example", planned - covered)
    expected = planned - _down(store.refs, planned - covered)

    async with SqlPlan.open(store.database) as plan:
        await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=None)
        dropped = await plan.cover({COLD})
        rows = await plan.weighed(pressure=0.5, now=NOW, bound=None, limit=None)

    assert {path for path, _, _ in rows} == expected - store.vetoed
    assert dropped == len(planned - expected)
    assert _up(store.refs, expected) == expected, "closed under referrers"
    assert (plan.size, plan.spared) == (len(expected), len(expected & store.vetoed))
