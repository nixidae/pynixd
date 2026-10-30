"""The SQL plan answers what the Python plan answered, on the same store.

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

import pytest

from pynixd.db_migrations import PATH_ACCESS_TABLE, apply_migrations
from pynixd.gc import _by_weight, _take_until_below_target
from pynixd.gc_sql import Bound, SqlPlan, referrer_first

if TYPE_CHECKING:
    from pathlib import Path

NOW = 1_800_000_000
MAX_AGE = 86_400


@dataclass
class Store:
    db: Path
    paths: list[str]
    refs: dict[str, set[str]]
    size: dict[str, int]
    seen: dict[str, int]
    live: set[str]
    vetoed: set[str]


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


async def _store(tmp_path: Path, seed: int, count: int = 120) -> Store:
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
    db = tmp_path / f"db-{seed}.sqlite"
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, deriver TEXT, "
            "registrationTime INTEGER, narSize INTEGER)"
        )
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        ids = {path: index + 1 for index, path in enumerate(paths)}
        conn.executemany(
            "INSERT INTO ValidPaths (id, path, registrationTime, narSize) VALUES (?, ?, ?, ?)",
            [(ids[path], path, registered[path], size[path]) for path in paths],
        )
        conn.executemany(
            "INSERT INTO Refs VALUES (?, ?)",
            [(ids[referrer], ids[reference]) for referrer, references in refs.items() for reference in references],
        )
    assert (await apply_migrations(db, read_only=False)).usable
    with closing(sqlite3.connect(db)) as conn, conn:
        conn.executemany(
            f"INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt) VALUES (?, ?)", list(accessed.items())
        )
    seen = registered | accessed
    return Store(db, paths, refs, size, seen, live, vetoed)


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
async def test_the_plan_and_its_order_match_python(tmp_path: Path, seed: int, pressed: bool) -> None:
    store = await _store(tmp_path, seed)
    cutoff = None if pressed else NOW - MAX_AGE
    planned = _python_plan(store, cutoff)
    weights = {path: (store.size[path], max(0, NOW - store.seen[path])) for path in planned - store.vetoed}
    pressure = random.Random(seed).random()

    async with SqlPlan.open(store.db) as plan:
        counts = await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=cutoff)
        rows = await plan.weighed(pressure=pressure, now=NOW, bound=None, limit=None)

    assert counts.planned == len(planned)
    assert counts.spared == len(planned & store.vetoed)
    assert [path for path, _, _ in rows] == _by_weight(weights, pressure)
    assert {path: (size, age) for path, size, age in rows} == weights


@pytest.mark.anyio
@pytest.mark.parametrize("seed", SEEDS)
async def test_the_bound_takes_what_python_took(tmp_path: Path, seed: int) -> None:
    store = await _store(tmp_path, seed)
    planned = _python_plan(store, None)
    weights = {path: (store.size[path], max(0, NOW - store.seen[path])) for path in planned - store.vetoed}
    ordered = _by_weight(weights, 0.8)
    total = 10**11
    freeable = sum(size for size, _ in weights.values())
    bound = Bound(used=int(total * 0.8), total=total, target=0.8 - freeable / total / 2)

    async with SqlPlan.open(store.db) as plan:
        await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=None)
        rows = await plan.weighed(pressure=0.8, now=NOW, bound=bound, limit=None)
        limited = await plan.weighed(pressure=0.8, now=NOW, bound=bound, limit=3)

    expected = _take_until_below_target(
        [(path, weights[path][0]) for path in ordered], bound.used, bound.total, bound.target
    )
    assert [path for path, _, _ in rows] == expected
    assert [path for path, _, _ in limited] == expected[:3]


@pytest.mark.anyio
@pytest.mark.parametrize("seed", SEEDS)
async def test_a_cut_batch_recloses_and_orders_as_python_did(tmp_path: Path, seed: int) -> None:
    store = await _store(tmp_path, seed)
    planned = _python_plan(store, None)
    weights = {path: (store.size[path], max(0, NOW - store.seen[path])) for path in planned - store.vetoed}
    head = _by_weight(weights, 0.5)[: max(1, len(weights) // 3)]

    async with SqlPlan.open(store.db) as plan:
        await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=None)
        closed = await plan.close_batch(head)
        ordered = await plan.order_batch(closed)

    expected = _python_close(store, head, planned)
    assert closed == expected
    assert ordered == referrer_first(expected, store.refs)


@pytest.mark.anyio
async def test_an_open_plan_does_not_block_a_writer(tmp_path: Path) -> None:
    """The plan reads one snapshot and writes only TEMP tables: Nix's write lock stays free."""
    store = await _store(tmp_path, 0)
    with closing(sqlite3.connect(store.db)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")

    async with SqlPlan.open(store.db) as plan:
        await plan.plan(live=store.live, vetoed=store.vetoed, cutoff=None)
        with closing(sqlite3.connect(store.db, timeout=0, isolation_level=None)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("DELETE FROM Refs")
            writer.execute("COMMIT")
        batch = sorted(_python_plan(store, None) - store.vetoed)
        # The snapshot still holds the references the plan was built on.
        assert await plan.order_batch(batch) == referrer_first(batch, store.refs)
