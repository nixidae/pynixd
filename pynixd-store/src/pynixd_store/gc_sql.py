"""The collector's age-rule plan, done where the data is: in SQLite.

The plan is set algebra over Nix's own tables: valid minus live, dated by
`PynixdPathAccess`, closed under referrers through `Refs`, weighed and cut
by disk pressure. Done in Python, it loaded the whole store and every
reference: 5 to 25 GB per pass on dynhetz (#1133). Done here, it costs
12 MB and 27 s for 152k planned paths (#1134).

One connection per pass, one read transaction (`Database.snapshot`): every
step reads the same snapshot of Nix's tables, and everything written lives
in TEMP tables, which takes no lock Nix waits on. Nix writes on while the
pass reads. Closing the connection ends the transaction and drops the TEMP
tables, so the collector closes it before it deletes anything.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from heapq import heappop, heappush
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping, Sequence

    from .database import Database, Session
    from .gc import DeleteOrder

log = structlog.get_logger(__name__)


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
    reclaimable_bytes: int
    """The NAR bytes of every dead path the veto leaves: what a pass with no age rule could free at most.

    Logical bytes: a compressing or deduplicating filesystem frees less (sg1's btrfs, zstd).
    """


class SqlPlan:
    """One pass's plan, held as TEMP tables on one read snapshot."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._sql = session.sql.gc
        self.size = 0
        self.spared = 0

    @classmethod
    @asynccontextmanager
    async def open(cls, db: Database) -> AsyncIterator[SqlPlan]:
        """A plan on a snapshot of *db*, closed whatever happens.

        A refusal of the database in the block raises `DatabaseError`, on
        which the collector fails closed.
        """
        async with db.snapshot() as session:
            for statement in session.sql.gc.schema:
                await session.execute(statement)
            yield cls(session)

    async def _load(self, table: str, paths: Iterable[str]) -> None:
        await self._session.execute(
            self._sql.load_ids.format(table=table), (self._session.sql.codec.path_set(sorted(paths)),)
        )

    async def plan(self, *, live: set[str], vetoed: set[str], cutoff: int | None) -> PlanCounts:
        """Plan the dead paths unreferenced since `cutoff` (all dead when `None`), closed under referrers."""
        await self._load("gc_live", live)
        await self._load("gc_veto", vetoed)
        await self._session.execute(self._sql.load_dead)
        await self._session.execute(self._sql.plan, {"cutoff": cutoff})
        row = await self._session.fetchone(self._sql.counts, {"cutoff": cutoff})
        if row is None:
            raise RuntimeError("the plan counts returned no row")
        counts = PlanCounts(
            valid=row[0], live=row[1], stale=row[2], planned=row[3], spared=row[4], reclaimable_bytes=int(row[5])
        )
        self.size = counts.planned
        self.spared = counts.spared
        return counts

    async def cover(self, stores: Iterable[str]) -> int:
        """Keep only what `PynixdHeldBy` shows *stores* hold, closed as the plan was; the count dropped."""
        before = self.size
        await self._session.execute(self._sql.load_covered, (self._session.sql.codec.path_set(sorted(stores)),))
        await self._session.execute(self._sql.drop_uncovered)
        row = await self._session.fetchone(self._sql.planned_counts)
        if row is None:
            raise RuntimeError("the plan counts returned no row")
        self.size, self.spared = int(row[0]), int(row[1])
        return before - self.size

    def whole(self, batch: list[str]) -> bool:
        """Whether *batch* is every planned path the veto left.

        That set needs no re-closing: the veto is a reference closure, so
        a referrer of an unvetoed path is never vetoed, and the plan is
        closed under referrers. Re-closing it would walk the whole plan
        again for nothing.
        """
        return len(batch) == self.size - self.spared

    async def weighed(
        self, *, order: DeleteOrder, now: int, bound: Bound | None, limit: int | None
    ) -> list[tuple[str, int, int]]:
        """`(path, nar_size, age_seconds)` of the planned, unvetoed paths in *order*, cut by *bound*."""
        params = {
            "now": now,
            "used": bound.used if bound else None,
            "total": bound.total if bound else None,
            "target": bound.target if bound else None,
            "limit": -1 if limit is None else limit,
        }
        statement = self._sql.weighed.format(score=self._sql.score(order))
        return [(path, size, age) for path, size, age in await self._session.fetchall(statement, params)]

    async def _set_batch(self, batch: Iterable[str]) -> None:
        await self._session.execute(self._sql.clear_batch)
        await self._load("gc_batch", batch)

    async def close_batch(self, batch: list[str]) -> list[str]:
        """The batch closed under referrers again, against the plan.

        `GarbagePlanner.close_batch` asks one recursive query per seed. Here a
        seed escapes when an outside referrer reaches it, found in one walk
        down from the escaped referrers, and the survivors close in one walk
        up. Seeds first in batch order, joined referrers after, by path.
        """
        await self._set_batch(batch)
        escaped = {row[0] for row in await self._session.fetchall(self._sql.escaped_seeds)}
        for seed in sorted(escaped):
            log.info("gc_batch_seed_dropped", seed=seed)
        survivors = [path for path in batch if path not in escaped]
        await self._set_batch(survivors)
        closed = {path for path, planned in await self._session.fetchall(self._sql.batch_referrers) if planned}
        order = {path: index for index, path in enumerate(survivors)}
        return sorted(closed, key=lambda path: (0, order[path]) if path in order else (1, path))

    async def weights_of(self, paths: list[str], now: int) -> dict[str, tuple[int, int]]:
        """`(nar_size, age_seconds)` of planned paths the weighed rows did not carry."""
        if not paths:
            return {}
        rows = await self._session.fetchall(
            self._sql.weights_of, (now, self._session.sql.codec.path_set(sorted(paths)))
        )
        return {path: (size, age) for path, size, age in rows}

    async def order_batch(self, batch: list[str]) -> list[str]:
        """Referrers before references, weight order breaking ties; empty when a batch path is not valid.

        Only the references between batch paths order a delete: a reference
        that leaves the batch stays in the store whatever the order.
        """
        await self._set_batch(batch)
        row = await self._session.fetchone(self._sql.batch_count)
        if row is None or row[0] != len(set(batch)):
            log.warning("gc_delete_order_incomplete", asked=len(set(batch)), answered=0 if row is None else row[0])
            return []
        edges: dict[str, set[str]] = {}
        for referrer, reference in await self._session.fetchall(self._sql.batch_edges):
            edges.setdefault(referrer, set()).add(reference)
        return referrer_first(batch, edges)
