"""The collector's age-rule plan, done where the data is: in SQLite.

The plan is set algebra over Nix's own tables: valid minus live, dated by
`PynixdPathAccess`, closed under referrers through `Refs`, weighed and cut
by disk pressure. Done in Python, it loaded the whole store and every
reference: 5 to 25 GB per pass on dynhetz (#1133). Done here, it costs
12 MB and 27 s for 152k planned paths (#1134).

One connection per pass, `mode=ro`, one read transaction: every step
reads the same snapshot of Nix's tables, and everything written lives in
TEMP tables, which takes no lock Nix waits on. Nix writes on while the
pass reads; a long read only holds back WAL checkpoints for its length.
Closing the connection ends the transaction and drops the TEMP tables, so
the collector closes it before it deletes anything.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from heapq import heappop, heappush
from typing import TYPE_CHECKING

import aiosqlite
import anyio
import structlog

from .db_migrations import PATH_ACCESS_TABLE

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping, Sequence
    from pathlib import Path

log = structlog.get_logger(__name__)

_BUSY_TIMEOUT_MS = 30_000

_SCHEMA = (
    "CREATE TEMP TABLE gc_live(id INTEGER PRIMARY KEY)",
    "CREATE TEMP TABLE gc_veto(id INTEGER PRIMARY KEY)",
    "CREATE TEMP TABLE gc_dead(id INTEGER PRIMARY KEY, path TEXT NOT NULL, size INTEGER NOT NULL, seen INTEGER)",
    "CREATE TEMP TABLE gc_planned(id INTEGER PRIMARY KEY)",
    "CREATE TEMP TABLE gc_batch(id INTEGER PRIMARY KEY)",
)

_LOAD_IDS = "INSERT OR IGNORE INTO {table} SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))"

_LOAD_DEAD = f"""
INSERT INTO gc_dead
SELECT v.id, v.path, COALESCE(v.narSize, 0), COALESCE(a.lastReferencedAt, v.registrationTime)
FROM ValidPaths v LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
WHERE v.id NOT IN (SELECT id FROM gc_live)
"""

# The stale dead seeds and everything that refers to them, kept to the dead.
# `?1` NULL takes every dead path (a pressed disk). A referrer of a dead path
# is dead unless the mirror is wrong, so the filter only guards that case.
_PLAN = """
INSERT INTO gc_planned
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_dead WHERE ?1 IS NULL OR seen < ?1
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
)
SELECT id FROM closure WHERE id IN (SELECT id FROM gc_dead)
"""

_COUNTS = f"""
SELECT
    (SELECT count(*) FROM ValidPaths),
    (SELECT count(*) FROM gc_live),
    (SELECT count(*) FROM ValidPaths v LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
        WHERE ?1 IS NOT NULL AND COALESCE(a.lastReferencedAt, v.registrationTime) < ?1),
    (SELECT count(*) FROM gc_planned),
    (SELECT count(*) FROM gc_planned WHERE id IN (SELECT id FROM gc_veto))
"""

# The weight `_by_weight` computes, in the same operations and the same
# order, so the two agree to the bit: size and age each normalised against
# the candidates, blended by pressure, path string breaking ties. `before`
# is what the rows ahead of this one free, which is what
# `_take_until_below_target` tests before it takes a row.
_WEIGHED = """
WITH w AS (
    SELECT d.path, d.size, CASE WHEN d.seen IS NULL THEN 0 ELSE max(0, :now - d.seen) END AS age
    FROM gc_planned p JOIN gc_dead d USING (id)
    WHERE p.id NOT IN (SELECT id FROM gc_veto)
), n AS (
    SELECT max(size) AS big, max(age) AS old FROM w
), scored AS (
    SELECT w.path, w.size, w.age,
        :pressure * (CASE WHEN n.big THEN 1.0 * w.size / n.big ELSE 0.0 END)
        + (1.0 - :pressure) * (CASE WHEN n.old THEN 1.0 * w.age / n.old ELSE 0.0 END) AS score
    FROM w, n
), ordered AS (
    SELECT path, size, age, score,
        COALESCE(sum(size) OVER (ORDER BY score DESC, path ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
            AS before
    FROM scored
)
SELECT path, size, age FROM ordered
WHERE :total IS NULL OR 1.0 * (:used - before) / :total > :target
ORDER BY score DESC, path
LIMIT :limit
"""

# A seed escapes the plan when anything outside the plan refers to it,
# transitively. Walking down from the batch's outside referrers marks every
# seed one of them reaches.
_BATCH_REFERRERS = """
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_batch
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
)
SELECT v.path, c.id IN (SELECT id FROM gc_planned) FROM closure c JOIN ValidPaths v ON v.id = c.id
"""

_ESCAPED_SEEDS = """
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_batch
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
), tainted(id) AS (
    SELECT id FROM closure WHERE id NOT IN (SELECT id FROM gc_planned)
    UNION
    SELECT r.reference FROM tainted t JOIN Refs r ON r.referrer = t.id WHERE r.reference IN (SELECT id FROM closure)
)
SELECT v.path FROM tainted t JOIN gc_batch b ON b.id = t.id JOIN ValidPaths v ON v.id = t.id
"""

_WEIGHTS_OF = """
SELECT path, size, CASE WHEN seen IS NULL THEN 0 ELSE max(0, ?1 - seen) END FROM gc_dead
WHERE path IN (SELECT value FROM json_each(?2))
"""

_BATCH_EDGES = """
SELECT a.path, b.path FROM Refs r
JOIN gc_batch ra ON ra.id = r.referrer JOIN gc_batch rb ON rb.id = r.reference
JOIN ValidPaths a ON a.id = r.referrer JOIN ValidPaths b ON b.id = r.reference
WHERE r.referrer != r.reference
"""


def referrer_first(ordered: Sequence[str], references: Mapping[str, set[str]]) -> list[str]:
    """Order referrers before references, with weight order breaking ties."""
    rank = {path: index for index, path in enumerate(dict.fromkeys(ordered))}
    paths = list(rank)
    children = {path: references.get(path, set()) & rank.keys() - {path} for path in paths}
    waiting = dict.fromkeys(paths, 0)
    for refs in children.values():
        for ref in refs:
            waiting[ref] += 1
    ready: list[int] = []
    for path, count in waiting.items():
        if count == 0:
            heappush(ready, rank[path])
    result = []
    while ready:
        path = paths[heappop(ready)]
        result.append(path)
        for ref in children[path]:
            waiting[ref] -= 1
            if waiting[ref] == 0:
                heappush(ready, rank[ref])
    if len(result) != len(paths):
        log.warning("gc_delete_order_blocked", paths=len(paths) - len(result))
    return result


@dataclass(frozen=True, slots=True)
class Bound:
    """Delete in weight order until `used - freed` over `total` is at most `target`."""

    used: int
    total: int
    target: float


@dataclass(frozen=True, slots=True)
class PlanCounts:
    valid: int
    live: int
    stale: int
    planned: int
    spared: int


class SqlPlan:
    """One pass's plan, held as TEMP tables on one read snapshot."""

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn
        self.size = 0
        self.spared = 0

    @classmethod
    @asynccontextmanager
    async def open(cls, db_path: Path) -> AsyncIterator[SqlPlan]:
        """A plan session on its own connection, closed whatever happens.

        Not a pooled connection: the pass holds its read transaction for
        tens of seconds, and the pool returns a connection only with no
        transaction open.
        """
        conn = await aiosqlite.connect(f"file:{db_path}?mode=ro", uri=True, isolation_level=None)
        try:
            await conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
            await conn.execute("BEGIN")
            for statement in _SCHEMA:
                await conn.execute(statement)
            yield cls(conn)
        finally:
            with anyio.CancelScope(shield=True):
                await conn.interrupt()
                await conn.close()

    async def _load(self, table: str, paths: Iterable[str]) -> None:
        await self._conn.execute(_LOAD_IDS.format(table=table), (json.dumps(sorted(paths)),))

    async def plan(self, *, live: set[str], vetoed: set[str], cutoff: int | None) -> PlanCounts:
        """Plan the dead paths unreferenced since `cutoff` (all dead when `None`), closed under referrers."""
        await self._load("gc_live", live)
        await self._load("gc_veto", vetoed)
        await self._conn.execute(_LOAD_DEAD)
        await self._conn.execute(_PLAN, (cutoff,))
        async with self._conn.execute(_COUNTS, (cutoff,)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("the plan counts returned no row")
        counts = PlanCounts(valid=row[0], live=row[1], stale=row[2], planned=row[3], spared=row[4])
        self.size = counts.planned
        self.spared = counts.spared
        return counts

    def whole(self, batch: list[str]) -> bool:
        """Whether *batch* is every planned path the veto left.

        That set needs no re-closing: the veto is a reference closure, so
        a referrer of an unvetoed path is never vetoed, and the plan is
        closed under referrers. Re-closing it would walk the whole plan
        again for nothing.
        """
        return len(batch) == self.size - self.spared

    async def weighed(
        self, *, pressure: float, now: int, bound: Bound | None, limit: int | None
    ) -> list[tuple[str, int, int]]:
        """`(path, nar_size, age_seconds)` of the planned, unvetoed paths, heaviest first, cut by *bound*."""
        params = {
            "pressure": pressure,
            "now": now,
            "used": bound.used if bound else None,
            "total": bound.total if bound else None,
            "target": bound.target if bound else None,
            "limit": -1 if limit is None else limit,
        }
        async with self._conn.execute(_WEIGHED, params) as cursor:
            return [(path, size, age) for path, size, age in await cursor.fetchall()]

    async def _set_batch(self, batch: Iterable[str]) -> None:
        await self._conn.execute("DELETE FROM gc_batch")
        await self._load("gc_batch", batch)

    async def close_batch(self, batch: list[str]) -> list[str]:
        """The batch closed under referrers again, against the plan.

        `Collector._close_batch` asks one recursive query per seed. Here a
        seed escapes when an outside referrer reaches it, found in one walk
        down from the escaped referrers, and the survivors close in one walk
        up. Seeds first in batch order, joined referrers after, by path.
        """
        await self._set_batch(batch)
        async with self._conn.execute(_ESCAPED_SEEDS) as cursor:
            escaped = {row[0] for row in await cursor.fetchall()}
        for seed in sorted(escaped):
            log.info("gc_batch_seed_dropped", seed=seed)
        survivors = [path for path in batch if path not in escaped]
        await self._set_batch(survivors)
        async with self._conn.execute(_BATCH_REFERRERS) as cursor:
            closed = {path for path, planned in await cursor.fetchall() if planned}
        order = {path: index for index, path in enumerate(survivors)}
        return sorted(closed, key=lambda path: (0, order[path]) if path in order else (1, path))

    async def weights_of(self, paths: list[str], now: int) -> dict[str, tuple[int, int]]:
        """`(nar_size, age_seconds)` of planned paths the weighed rows did not carry."""
        if not paths:
            return {}
        async with self._conn.execute(_WEIGHTS_OF, (now, json.dumps(sorted(paths)))) as cursor:
            return {path: (size, age) for path, size, age in await cursor.fetchall()}

    async def order_batch(self, batch: list[str]) -> list[str]:
        """Referrers before references, weight order breaking ties; empty when a batch path is not valid.

        Only the references between batch paths order a delete: a reference
        that leaves the batch stays in the store whatever the order.
        """
        await self._set_batch(batch)
        async with self._conn.execute("SELECT count(*) FROM gc_batch") as cursor:
            row = await cursor.fetchone()
        if row is None or row[0] != len(set(batch)):
            log.warning("gc_delete_order_incomplete", asked=len(set(batch)), answered=0 if row is None else row[0])
            return []
        edges: dict[str, set[str]] = {}
        async with self._conn.execute(_BATCH_EDGES) as cursor:
            for referrer, reference in await cursor.fetchall():
                edges.setdefault(referrer, set()).add(reference)
        return referrer_first(batch, edges)
