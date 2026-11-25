"""The collector behind `PynixdCollectGarbage` (op 101).

One rule today: a path may leave the local store when a substituter that
`gc_defer` names already holds it, and when Nix agrees that it is not alive.

**The rule is not liveness alone.** The store pynixd serves is a cache, and
almost nothing in it is rooted, so `GCAction.DELETE_DEAD` would empty it. This
collector names every path that it deletes.

Age is the second rule, and it is here: `gc_max_age` on the local store
plans the dead paths nothing referenced for that many seconds, with no
substituter needed. Age yields to pressure: over `gc_target_usage`, every
dead path is planned, the way `nix store gc --max-freed` plans. Size is
the third: every pass deletes in weight order, `(nar_size, age)` blended
by disk pressure, so an empty disk collects
oldest first and a full disk biggest first. `gc_target_usage` bounds a
pass by pressure instead of emptying the plan at once.
"""

from __future__ import annotations

import time
from collections import deque
from contextlib import asynccontextmanager, nullcontext
from itertools import batched
from typing import TYPE_CHECKING

import anyio
import structlog
from anyio.to_thread import run_sync

from nix_daemon_protocol import (
    CollectGarbageRequest,
    GCAction,
    LogNext,
    QueryAllValidPathsRequest,
    QueryValidPathsRequest,
    StorePath,
)
from nix_daemon_protocol.query_path_infos import QueryPathInfosRequest
from pynixd_store.gc import (
    GarbagePlanner,
    by_weight,
    over_target,
    physical_batch_total,
    take_until_below_target,
)
from pynixd_store.gc_sql import Bound, SqlPlan, referrer_first
from pynixd_store.local_store_db import DatabaseError

from . import metrics
from .cold_tier import ColdTier
from .daemon_extensions import (
    PynixdCollectGarbageResponse,
    PynixdGCAction,
    QueryClosureRequest,
)
from .exceptions import BackendError, CacheFault, GCNotPermittedError, PynixdError
from .psi import ReclaimSample
from .store.local_db import LocalDBStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from contextlib import AbstractAsyncContextManager

    from nix_daemon_protocol.collect_garbage import CollectGarbageResponse

    from .connection import ClientConn, Connection
    from .context import PynixdContext
    from .store.base import Store
    from .store.local_daemon import LocalStore

log = structlog.get_logger(__name__)

_MAX_FREED = 2**63 - 1

_DELETE_BATCH_SIZE = 100
_DELETE_TIMEOUT_S = 60
"""Bound each delete and recovery query; cancelled connections are discarded."""


def _wire(conn: Connection | None) -> Connection:
    """The connection of a pass that the daemon collects: only an engine pass runs with none."""
    if conn is None:
        raise RuntimeError("a pass that the daemon collects runs on a connection")
    return conn


def _request(action: GCAction, paths: set[StorePath]) -> CollectGarbageRequest:
    return CollectGarbageRequest(
        action=action,
        paths_to_delete=paths,
        ignore_liveness=0,
        max_freed=_MAX_FREED,
        obsolete1=0,
        obsolete2=0,
        obsolete3=0,
    )


class _MemoryPlan:
    """A plan held as Python sets: the substituter rule, and stores without pynixd's tables.

    The same questions `SqlPlan` answers, through the collector's own
    helpers; the age rule on a store with pynixd's tables plans in SQLite.
    """

    def __init__(self, collector: Collector, planned: set[str], vetoed: set[str]) -> None:
        self._collector = collector
        self._planned = planned
        self._vetoed = vetoed
        self._weights: dict[str, tuple[int, int]] = {}
        self.size = len(planned)
        self.spared = len(planned & vetoed)

    async def weighed(
        self, *, pressure: float, now: int, bound: Bound | None, limit: int | None
    ) -> list[tuple[str, int, int]] | None:
        weights = await self._collector.planner().weigh(self._planned - self._vetoed)
        if weights is None:
            return None
        self._weights = weights
        ordered = by_weight(weights, pressure)
        if bound is not None:
            ordered = take_until_below_target(
                [(path, weights[path][0]) for path in ordered], bound.used, bound.total, bound.target
            )
        if limit is not None:
            ordered = ordered[:limit]
        return [(path, *weights[path]) for path in ordered]

    def whole(self, batch: list[str]) -> bool:
        return set(batch) == self._planned

    async def weights_of(self, paths: list[str], now: int) -> dict[str, tuple[int, int]]:
        return {path: self._weights[path] for path in paths if path in self._weights}

    async def close_batch(self, batch: list[str]) -> list[str]:
        return await self._collector.planner().close_batch(batch, self._planned)

    async def order_batch(self, batch: list[str]) -> list[str]:
        return await self._collector._order_batch(self._collector.ctx.local_store, batch)


class Collector:
    """Chooses what leaves the local store, and asks Nix to delete it."""

    def __init__(self, ctx: PynixdContext) -> None:
        self.ctx = ctx

    def planner(self) -> GarbagePlanner:
        """The reads of the local store's roots, database and disk."""
        return _planner_of(self.ctx.local_store)

    async def run(
        self,
        action: PynixdGCAction,
        client: ClientConn | None = None,
        limit: int | None = None,
        target_usage: float | None = None,
    ) -> PynixdCollectGarbageResponse:
        """Plan a pass, and run it when *action* says to.

        A plan refreshes Python liveness before selecting paths.
        Only an unavailable Python resolver falls back to a daemon trace.

        Before planning, the pass re-walks the volatile roots fresh -- the
        living half no watch covers -- and vetoes them and their reference
        closure from the plan. A process that started after the last check
        must not lose its libraries to this pass. The veto is fail-closed:
        a closure the daemon cannot answer falls back to the seeds.

        The pass deletes in weight order, so a bounded pass frees the most
        with the fewest deletes, and a dry-run lists what goes first. The
        pressure bound narrows execute passes only: a dry-run answers the
        whole plan, so an under-target disk does not read as clean (#73).
        The weight blends size and age by disk pressure: an empty disk collects
        oldest first, a full disk biggest first. `limit` takes the head of
        that order, and `target_usage` overrides the store's bound for one
        pass; both narrow only, and neither is plannable below zero.

        EXECUTE stays refused until the operator permits it. Planning asks
        Nix nothing it cannot already ask, but a delete is irreversible, and
        the planner's liveness answer is still unproven against the mirror:
        `gc_allow_execute` on the local store is the signature, off by
        default, and EXECUTE without it raises `GCNotPermittedError`.

        Progress travels on the wire when *client* is set: one `deleting`
        line per path, in the words `gc.cc:574` uses, plus the veto line
        and the outcome. The same lines buffer into the response, which is
        what a client that reads at the end prints.
        """
        if limit is not None and limit < 0:
            raise ValueError(f"limit deletes at most N paths, and {limit} is not a count")
        if target_usage is not None and target_usage <= 0:
            raise ValueError(f"target_usage bounds by a fraction, and {target_usage} is not one")
        if action == PynixdGCAction.EXECUTE and not getattr(self.ctx.local_store, "gc_allow_execute", False):
            log.warning("gc_execute_refused")
            raise GCNotPermittedError(
                "collector EXECUTE is not permitted: set gc_allow_execute "
                "after the liveness mirror shows sustained zero-divergence"
            )
        lines: list[LogNext] = []
        local = self.ctx.local_store
        usage = await run_sync(self._disk_usage, local)
        target = target_usage if target_usage is not None else local.gc_target_usage
        if (
            action == PynixdGCAction.EXECUTE
            and target is not None
            and usage is not None
            and not over_target(usage, target)
        ):
            # The bound takes nothing from a disk already at its target, so
            # planning first is all cost: on dynhetz, a 150k-path plan every
            # hour for no delete (#1133).
            log.info("gc_under_target", target=target, used=usage[0], total=usage[1])
            line = LogNext(
                text=f"usage {usage[0] / usage[1]:.3f} is at or under the target {target}: nothing to delete"
            )
            lines.append(line)
            if client is not None:
                await client.send(line)
            return self._answered(PynixdCollectGarbageResponse(store_paths=set(), bytes=0), lines)
        tier = ColdTier.of(self.ctx)
        response = await self._pass(action, client, limit, target, usage, lines, tier)
        if tier is None or action != PynixdGCAction.EXECUTE or target is None or local.gc_max_age is None:
            return response
        usage = await run_sync(self._disk_usage, local)
        if usage is None or not over_target(usage, target):
            return response
        # Over target with every covered path gone: stop short. Deleting a path the tier does not
        # hold is the rebuild the tier exists to prevent (#1417); the upload-ahead loop covers more
        # each round, and the next pass takes what it covered.
        metrics.GC_STOPPED_SHORT.inc()
        log.warning("gc_stopped_short", target=target, used=usage[0], total=usage[1])
        line = LogNext(
            text=f"usage {usage[0] / usage[1]:.3f} is over the target {target}, "
            "but the cold tier holds no more of the dead paths: stopping short"
        )
        lines.append(line)
        if client is not None:
            await client.send(line)
        return self._answered(response, lines)

    async def _pass(
        self,
        action: PynixdGCAction,
        client: ClientConn | None,
        limit: int | None,
        target: float | None,
        usage: tuple[int, int] | None,
        lines: list[LogNext],
        tier: ColdTier | None,
    ) -> PynixdCollectGarbageResponse:
        """One plan and its deletes. With *tier*, a planned path goes only when the tier covers its closure."""
        volatile = await run_sync(self.planner().volatile_roots)
        vetoed = await self._veto_closure(volatile)
        if usage is None:
            # Unknown reads as half full: with no information neither axis
            # earns the lead. The bound still defaults to unbounded on top
            # of this, so a missing reading changes the order only, never
            # the set.
            log.warning("gc_no_disk_usage")
            pressure = 0.5
        else:
            used, total = usage
            pressure = used / total if total else 0.5
        # A dry-run answers the plan, not the pressure: the store's
        # `gc_target_usage` bounds what an execute pass deletes, but a
        # dry-run reports what is droppable, so an under-target disk does
        # not read as clean. Issue #73. An execute pass reaching here is
        # over its target, so the bound has a nonzero total.
        bound = (
            Bound(used=usage[0], total=usage[1], target=target)
            if action == PynixdGCAction.EXECUTE and target is not None and usage is not None
            else None
        )
        empty = PynixdCollectGarbageResponse(store_paths=set(), bytes=0)
        try:
            async with self._open_plan(pressed=over_target(usage, target), vetoed=vetoed, tier=tier) as plan:
                veto = LogNext(
                    text=f"volatile veto: rechecked {len(volatile)} living roots, spared {plan.spared} planned paths"
                )
                lines.append(veto)
                if client is not None:
                    await client.send(veto)
                log.info("gc_volatile_veto", fresh=len(volatile), spared=plan.spared)
                now = int(time.time())
                rows = await plan.weighed(pressure=pressure, now=now, bound=bound, limit=limit)
                if rows is None:
                    return self._answered(empty, lines)
                if bound is not None:
                    log.info(
                        "gc_bounded_pass",
                        target=bound.target,
                        usage=bound.used / bound.total,
                        batch=len(rows),
                        planned=plan.size,
                    )
                weights = {path: (size, age) for path, size, age in rows}
                batch = [path for path, _, _ in rows]
                if not batch:
                    return self._answered(empty, lines)
                if not plan.whole(batch):
                    # Slicing breaks the referrer closure the planners promise, and
                    # gc.cc refuses a batch whose referrer rides below the slice.
                    # Re-close: referrers in the plan join, seeds that escape it drop.
                    # Issues #69 and #70.
                    batch = await plan.close_batch(batch)
                    weights |= await plan.weights_of([path for path in batch if path not in weights], now)
                if action != PynixdGCAction.EXECUTE:
                    self._log_top(batch, weights, pressure)
                    return self._answered(
                        PynixdCollectGarbageResponse(
                            store_paths={StorePath(path) for path in batch},
                            bytes=sum(weights[path][0] for path in batch),
                        ),
                        lines,
                    )
                batch = await plan.order_batch(batch)
        except DatabaseError:
            # Fail closed: a plan the database could not finish deletes nothing.
            log.warning("gc_plan_failed", exc_info=True)
            return self._answered(empty, lines)
        # The plan's connection is closed by here: the deletes below must not
        # run under its read transaction.
        if tier is not None and not await self._cold_still_holds(tier, batch, lines, client):
            return self._answered(empty, lines)
        return await self._delete(batch, weights, lines, client)

    async def _cold_still_holds(
        self, tier: ColdTier, batch: list[str], lines: list[LogNext], client: ClientConn | None
    ) -> bool:
        """Whether the bucket still holds every path of *batch* its record covers.

        One missing path stops the whole pass: dropping it from the batch would leave its referrers in the
        batch, and the next plan, without its row, keeps it and them.
        """
        try:
            gone = await tier.missing(batch)
        except CacheFault:
            log.warning("gc_cold_verify_failed", store_id=tier.cold.store_id, exc_info=True)
            text = "the cold tier could not answer for its paths: this pass deletes nothing"
        else:
            if not gone:
                return True
            log.warning("gc_cold_lost_paths", store_id=tier.cold.store_id, paths=len(gone), first=sorted(gone)[:3])
            text = f"the cold tier no longer holds {len(gone)} recorded paths: this pass deletes nothing"
        line = LogNext(text=text)
        lines.append(line)
        if client is not None:
            await client.send(line)
        return False

    async def cold_window(self, tier: ColdTier, budget: int, pressure: float) -> list[str]:
        """The uncovered paths a pressed pass would delete first, up to *budget* bytes: what to upload first.

        The plan is the pressed one, every dead path in weight order at *pressure*: the pass the
        watermark starts. Covered paths count toward the budget and cost nothing. The window then
        takes its planned referrers, whatever their weight: a pass keeps a covered path while an
        uncovered referrer stays.
        """
        local = self.ctx.local_store
        db = self.planner().plan_database()
        if db is None:
            return []
        volatile = await run_sync(self.planner().volatile_roots)
        vetoed = await self._veto_closure(volatile)
        live = await self._live_paths(local)
        async with SqlPlan.open(db) as plan:
            await plan.plan(live=live, vetoed=vetoed, cutoff=None)
            rows = await plan.weighed(pressure=pressure, now=int(time.time()), bound=None, limit=None)
            window: list[str] = []
            total = 0
            for path, size, _ in rows:
                if total >= budget:
                    break
                total += size
                window.append(path)
            closed = await plan.close_batch(window) if window else []
        covered = await tier.covered(closed)
        head = [path for path in closed if path not in covered]
        log.info(
            "gc_cold_ahead",
            uncovered=len(head),
            budget=budget,
            planned=len(rows),
            window=len(window),
            closed=len(closed),
            covered=len(covered),
        )
        return head

    @asynccontextmanager
    async def _open_plan(
        self, *, pressed: bool, vetoed: set[str], tier: ColdTier | None = None
    ) -> AsyncIterator[SqlPlan | _MemoryPlan]:
        """The pass's plan: in SQLite for the age rule on a store with pynixd's tables, else in memory.

        The substituter rule asks the caches about every path, which is not
        a database question, and a store without pynixd's tables has no
        access dates to plan by.
        """
        local = self.ctx.local_store
        max_age = local.gc_max_age
        db = self.planner().plan_database()
        if max_age is None or db is None:
            yield _MemoryPlan(self, {str(path) for path in await self.plan(pressed=pressed, tier=tier)}, vetoed)
            return
        live = await self._live_paths(local)
        async with SqlPlan.open(db) as plan:
            counts = await plan.plan(live=live, vetoed=vetoed, cutoff=None if pressed else int(time.time()) - max_age)
            usage = await run_sync(self._disk_usage, local)
            if usage is not None:
                used, total = usage
                local.daemon_client.disk_reclaim = ReclaimSample(
                    free_bytes=total - used, reclaimable_bytes=counts.reclaimable_bytes, at=time.monotonic()
                )
            log.info(
                "gc_plan_lru",
                valid=counts.valid,
                live=counts.live,
                stale=counts.stale,
                pressed=pressed,
                droppable=counts.planned,
                reclaimable_bytes=counts.reclaimable_bytes,
                planner="sqlite",
            )
            if tier is not None:
                kept = await plan.cover(tier.keys)
                log.info("gc_plan_cold", store_id=tier.cold.store_id, kept_uncovered=kept, droppable=plan.size)
            yield plan

    async def _live_paths(self, local: Store) -> set[str]:
        """The mirror's live set, refreshed just now, or Nix's when there is no mirror."""
        mirror = await self.planner().live()
        if mirror is not None:
            return mirror
        return {str(path) for path in (await local.call(_request(GCAction.RETURN_LIVE, set()))).paths_deleted}

    @staticmethod
    def _answered(response: PynixdCollectGarbageResponse, lines: list[LogNext]) -> PynixdCollectGarbageResponse:
        """*response* carrying the pass's wire lines in its log buffer."""
        for line in lines:
            response.logs.add(line)
        return response

    async def _veto_closure(self, volatile: set[str]) -> set[str]:
        """*volatile* plus what it references: the paths no pass may name.

        The daemon answers the closure without taking the collector lock,
        and a path the veto spares keeps whatever still names it, which is
        what keeps the delete set closed under referrers (`gc.cc:653`). A
        closure the daemon cannot answer falls back to the seeds alone:
        sparing less than the full closure, but never nothing.
        """
        local = self.ctx.local_store
        if not volatile:
            return set()
        try:
            closed = {
                str(path)
                for path in (
                    await local.execute(QueryClosureRequest(paths={StorePath(path) for path in volatile}))
                ).paths
            }
        except Exception:
            log.warning("gc_veto_closure_failed", exc_info=True)
            return volatile
        # A seed missing from the answer is not valid yet -- an in-flight
        # build's output under a temp root -- and has no closure to add. The
        # rest of the answer still holds, so the seeds join it rather than
        # replace it: on sg1 every pass had such a seed, and the fallback to
        # the seeds alone dropped 404 of 705 paths from the veto. #1098.
        return closed | volatile

    def _log_top(
        self, ordered: list[str], weights: dict[str, tuple[int, int]], pressure: float, count: int = 10
    ) -> None:
        """The first planned paths, for the dry-run to show first.

        The response carries a set, which has no order, so the ranking
        travels in the logs instead: the journal keeps it, and the CLI
        prints what the daemon logged.
        """
        log.info(
            "gc_weights_top",
            pressure=round(pressure, 3),
            top=[{"path": path, "bytes": weights[path][0], "age_s": weights[path][1]} for path in ordered[:count]],
        )

    async def plan(self, *, pressed: bool = False, tier: ColdTier | None = None) -> set[StorePath]:
        """The paths that may leave the store.

        Two rules, picked by `gc_max_age` on the local store. `None` keeps
        the substituter rule below; a number of seconds picks the age rule,
        which needs no substituter: dead and unreferenced for that long goes,
        whether or not any cache holds it. Setting the number is the operator
        taking ownership of the store's old paths, and the default keeps the
        cache behaviour.

        *pressed* lifts the age from the age rule: every dead path goes. A
        node that builds all day refreshes its build inputs all day, so on
        sg1 18000 paths were dead, fewer than 300 of them a day unreferenced,
        and a disk over its watermark found nothing to free. #1098. The substituter
        rule ignores it: there the cache is what makes a delete safe.

        Both rules refresh Python liveness, falling back to Nix only when unavailable.

        *tier* adds to the substituter rule and narrows the age rule: a path its
        record covers counts as held, and an aged path it does not cover stays.
        """
        local = self.ctx.local_store
        max_age = local.gc_max_age
        if max_age is None:
            return await self._plan_deferred(local, tier)
        planned = await self._plan_lru(local, None if pressed else max_age)
        if tier is None:
            return planned
        return await self._covered_part(local, planned, tier)

    async def _covered_part(self, local: Store, planned: set[StorePath], tier: ColdTier) -> set[StorePath]:
        """*planned* minus each uncovered path and its closure: what may go and keep the set closed under referrers."""
        covered = await tier.covered({str(path) for path in planned})
        uncovered = {path for path in planned if str(path) not in covered}
        if not uncovered:
            return planned
        execute = getattr(local, "execute")  # noqa: B009 -- `mark` is LocalDBStore's, not Store's
        keep: set[StorePath] = (await execute(QueryClosureRequest(paths=uncovered), mark=False)).paths
        if not uncovered <= keep:
            log.error("gc_closure_incomplete", asked=len(uncovered), answered=len(keep))
            return set()
        return planned - keep

    async def _plan_deferred(self, local: Store, tier: ColdTier | None = None) -> set[StorePath]:
        """The paths that may leave the store.

        A path stays when no substituter confirms it, and so does everything
        that path references. The drop set is therefore the complement of the
        reference closure of the unconfirmed paths, which makes it closed under
        referrers: a path unique to this store keeps the cached paths under it.

        Closed under referrers is also what makes the delete work at all.
        `gc.cc:653` refuses a named path whose referrer is not named in the
        same request, so the set travels together.

        Python resolves roots and their reference closure before selecting paths.
        """
        stores = [store for store in self.ctx.stores.values() if store.gc_defer]
        if not stores and tier is None:
            return set()

        all_paths: set[StorePath] = (await local.execute(QueryAllValidPathsRequest())).paths
        if not all_paths:
            return set()

        held: set[StorePath] = set()
        if tier is not None:
            held = {StorePath(path) for path in await tier.covered({str(path) for path in all_paths})}
        for store in stores:
            held |= await self._held_by(store, all_paths)

        unheld = all_paths - held
        # Unmarked, like the weighing: these are candidates under judgment,
        # and naming them must not freshen them.
        execute = getattr(local, "execute")  # noqa: B009 -- `mark` is LocalDBStore's, not Store's
        keep: set[StorePath] = (await execute(QueryClosureRequest(paths=unheld), mark=False)).paths
        if not unheld <= keep:
            # `DaemonStore.query_closure` answers an empty set for a store that
            # does not carry the feature, and the difference below would then
            # be the whole store.
            log.error("gc_closure_incomplete", asked=len(unheld), answered=len(keep))
            return set()

        mirror = await self.planner().live()
        if mirror is None:
            live: set[StorePath] = (await local.call(_request(GCAction.RETURN_LIVE, set()))).paths_deleted
        else:
            live = {StorePath(path) for path in mirror}
        droppable = all_paths - keep - live
        log.info("gc_plan", valid=len(all_paths), held=len(held), live=len(live), droppable=len(droppable))
        return droppable

    async def _plan_lru(self, local: Store, max_age: int | None) -> set[StorePath]:
        """The dead paths nothing referenced for `max_age` seconds; `None` takes every dead path.

        `PynixdPathAccess` dates each path, and a path with no access row
        dates from its registration. Python resolves the live set. The set is
        closed under referrers so Nix accepts it (`gc.cc:653`), which pulls
        in dead referrers even when they are fresh — deleting a path takes
        what still names it. A failure anywhere plans nothing: an LRU pass
        that cannot see the whole state deletes nothing.
        """
        all_paths: set[StorePath] = (await local.execute(QueryAllValidPathsRequest())).paths
        if not all_paths:
            return set()

        live_paths = await self.planner().live()
        if live_paths is None:
            live = {str(path) for path in (await local.call(_request(GCAction.RETURN_LIVE, set()))).paths_deleted}
        else:
            live = live_paths
        dead = {str(path) for path in all_paths} - live

        if max_age is None:
            stale = dead
        else:
            found = await self.planner().stale_since(max_age)
            if found is None:
                return set()
            stale = found
        seeds = dead & stale

        closed = await self.planner().referrer_closure(seeds)
        if closed is None:
            return set()
        droppable = {path for path in closed if path in dead}
        log.info(
            "gc_plan_lru",
            valid=len(all_paths),
            live=len(live),
            stale=len(stale),
            pressed=max_age is None,
            droppable=len(droppable),
        )
        return {StorePath(path) for path in droppable}

    async def _order_batch(self, local: Store, batch: list[str]) -> list[str]:
        """Resolve the whole graph in Python before dividing it into delete groups."""
        if not batch:
            return []
        try:
            execute = getattr(local, "execute")  # noqa: B009 -- `mark` is LocalDBStore's, not Store's
            response = await execute(QueryPathInfosRequest(paths={StorePath(path) for path in batch}), mark=False)
        except Exception:
            log.warning("gc_delete_order_failed", exc_info=True)
            return []
        references = {str(info.path): {str(ref) for ref in info.info.references} for info in response.infos}
        if not set(batch) <= references.keys():
            log.warning("gc_delete_order_incomplete", asked=len(batch), answered=len(references))
            return []
        return referrer_first(batch, references)

    async def _held_by(self, store: Store, paths: set[StorePath]) -> set[StorePath]:
        """The paths of *paths* that *store* confirms it has.

        A store that cannot answer confirms nothing, so an unreachable
        substituter keeps every path it would have released.

        **`WantMassQuery` does not stop the sweep.** That flag asks a client
        that nobody configured to leave the cache alone, and `gc_defer` is an
        operator naming this store. `nix copy --to file://` writes `0` into
        every cache it makes, so honouring it here would make the flag useless
        for a lab cache. `BinaryCacheStore.query_valid_paths` asks for one
        narinfo at a time, so the sweep cannot flood a cache either way.
        """
        if not store.want_mass_query:
            log.debug("gc_store_asked_despite_wantmassquery", store_id=str(store.store_id))
        try:
            valid: set[StorePath] = (await store.execute(QueryValidPathsRequest(paths=paths))).paths
            return valid
        except Exception:
            log.warning("gc_store_query_failed", store_id=str(store.store_id), exc_info=True)
            return set()

    async def _delete(
        self, batch: list[str], weights: dict[str, tuple[int, int]], lines: list[LogNext], client: ClientConn | None
    ) -> PynixdCollectGarbageResponse:
        """Delete groups of 100, isolating refusals without stopping the queue.

        Errors carry no partial results. Recheck validity before splitting a
        failed group, so deleted paths never enter another delete request.
        Singleton refusals get one final lap for cross-group referrers.
        Silent skips and timed-out groups stay out of further deletes this run.
        """
        local = self.ctx.local_store
        # Idle daemon workers retain temporary roots for paths they served.
        await local.retire_idle_connections()
        if not batch:
            return self._answered(PynixdCollectGarbageResponse(store_paths=set(), bytes=0), lines)

        directory = self.planner().real_store_dir
        physical = await run_sync(physical_batch_total, batch, directory)
        deleted: set[str] = set()
        recovered: set[str] = set()
        spared: set[str] = set()
        uncertain: set[str] = set()
        timed_out: set[str] = set()
        freed = 0

        async def say(text: str) -> None:
            line = LogNext(text=text)
            lines.append(line)
            if client is not None:
                await client.send(line)

        async def recheck(conn: Connection | None, paths: Sequence[str]) -> list[str]:
            # Bypass the caches: a failed delete can already invalidate paths.
            with anyio.fail_after(_DELETE_TIMEOUT_S):
                if collecting is not None:
                    valid = {str(path) for path in await collecting.engine.query_valid_paths(paths)}
                else:
                    response = await _wire(conn).call(
                        QueryValidPathsRequest(paths={StorePath(path) for path in paths}, substitute=0),
                        raise_on_error=True,
                    )
                    valid = {str(path) for path in response.paths}
            gone = set(paths) - valid
            deleted.update(gone)
            recovered.update(gone)
            for path in gone:
                local.forget_path_info(path)
            return [path for path in paths if path in valid]

        await local.probe()
        collecting = local if isinstance(local, LocalDBStore) and local.router.engine_collects() else None

        def connection() -> AbstractAsyncContextManager[Connection | None]:
            # The engine deletes and rechecks with no connection, and a store with no daemon has none to give.
            return nullcontext() if collecting is not None else local.transfer_conn()

        async def delete(conn: Connection | None, group: list[str]) -> CollectGarbageResponse:
            request = _request(GCAction.DELETE_SPECIFIC, {StorePath(path) for path in group})
            with anyio.fail_after(_DELETE_TIMEOUT_S):
                if collecting is not None:
                    return await collecting.router.collect_garbage(request)
                response: CollectGarbageResponse = await _wire(conn).call(request, raise_on_error=True)
                return response

        pending = list(dict.fromkeys(batch))
        for lap in (1, 2):
            if not pending:
                break
            if lap == 2:
                log.info("gc_delete_retry", paths=len(pending))
            queue = deque(list(group) for group in batched(pending, _DELETE_BATCH_SIZE, strict=False))
            retry: list[str] = []
            while queue:
                async with connection() as conn:
                    while queue:
                        group = queue.popleft()
                        await say(f"deleting '{group[0]}'" if len(group) == 1 else f"deleting {len(group)} paths")
                        try:
                            response = await delete(conn, group)
                        except PynixdError as exc:
                            # The daemon refuses with `BackendError`, the engine with `PathInUse`.
                            log.warning("gc_chunk_refused", paths=len(group), reason=str(exc))
                            try:
                                remaining = await recheck(conn, group)
                            except (BackendError, TimeoutError, ConnectionError, EOFError):
                                log.warning("gc_chunk_recheck_failed", paths=len(group), exc_info=True)
                                uncertain.update(group)
                                if conn is not None:
                                    conn.dirty = True
                                break
                            if len(group) == 1:
                                retry.extend(remaining)
                            elif remaining:
                                middle = max(1, len(remaining) // 2)
                                if remaining[middle:]:
                                    queue.appendleft(remaining[middle:])
                                queue.appendleft(remaining[:middle])
                            continue
                        except (TimeoutError, ConnectionError, EOFError) as exc:
                            log.warning("gc_chunk_interrupted", paths=len(group), reason=str(exc))
                            uncertain.update(group)
                            if isinstance(exc, TimeoutError):
                                timed_out.update(group)
                            if conn is not None:
                                conn.dirty = True
                            break
                        finally:
                            for path in group:
                                local.forget_path_info(path)
                        got = {str(path) for path in response.paths_deleted}
                        deleted.update(got)
                        freed += response.bytes_freed
                        spared.update(set(group) - got)
                        # The paths, so a later "path ... is not valid" can be
                        # matched against what a pass took. nixidae/nixidae#16.
                        log.info(
                            "gc_chunk_deleted",
                            requested=len(group),
                            deleted=len(got),
                            total=len(deleted),
                            paths=sorted(got),
                        )
                        await say(f"deleted {len(got)} paths, {len(deleted)} total")
            pending = [path for path in retry if path not in deleted]

        for group in (list(members) for members in batched(sorted(uncertain), _DELETE_BATCH_SIZE, strict=False)):
            async with connection() as conn:
                try:
                    await recheck(conn, group)
                except (BackendError, TimeoutError, ConnectionError, EOFError):
                    log.warning("gc_chunk_recheck_failed", paths=len(group), exc_info=True)
                    if conn is not None:
                        conn.dirty = True

        refused = len(set(pending) - deleted)
        if physical is not None and deleted == set(batch):
            freed = physical
        elif physical is not None and recovered:
            remaining_bytes = await run_sync(physical_batch_total, set(batch) - deleted, directory)
            if remaining_bytes is not None:
                freed = max(0, physical - remaining_bytes)
            else:
                await say("freed byte count excludes deletions from failed requests")
        elif recovered:
            await say("freed byte count excludes deletions from failed requests")
        log.info(
            "gc_pass_done",
            deleted=len(deleted),
            refused=refused,
            spared=len(spared),
            timed_out=len(timed_out - deleted),
            unconfirmed=len(uncertain - deleted),
            bytes=freed,
        )
        if refused or spared or uncertain:
            await say(
                f"delete refused for {refused} paths, {len(spared)} spared, "
                f"{len(uncertain - deleted)} interrupted, deleted the rest"
            )
        await say(f"deleted {len(deleted)} paths, {freed} bytes freed")
        return self._answered(
            PynixdCollectGarbageResponse(store_paths={StorePath(path) for path in deleted}, bytes=freed),
            lines,
        )

    @staticmethod
    def _disk_usage(local: LocalStore | LocalDBStore) -> tuple[int, int] | None:
        """`(used, total)` bytes of pressure on the filesystem holding the store. A `statvfs`: call it in a thread."""
        return _planner_of(local).disk_usage()


def _planner_of(local: LocalStore | LocalDBStore) -> GarbagePlanner:
    """The engine's view of *local*: its layout, and its database when it has one."""
    return GarbagePlanner(local.layout, local.db)
