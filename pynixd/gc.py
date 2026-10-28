"""The collector behind `PynixdCollectGarbage` (op 101).

One rule today: a path may leave the local store when a substituter that
`gc_defer` names already holds it, and when Nix agrees that it is not alive.

**The rule is not liveness alone.** The store pynixd serves is a cache, and
almost nothing in it is rooted, so `GCAction.DELETE_DEAD` would empty it. This
collector names every path that it deletes.

Age is the second rule, and it is here: `gc_max_age` on the local store
plans the dead paths nothing referenced for that many seconds, with no
substituter needed. Size is the third: every pass deletes in weight order,
`(nar_size, age)` blended by disk pressure, so an empty disk collects
oldest first and a full disk biggest first. `gc_target_usage` bounds a
pass by pressure instead of emptying the plan at once.
"""

from __future__ import annotations

import os
import stat
import time
from collections import deque
from heapq import heappop, heappush
from itertools import batched
from pathlib import Path
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

from .daemon_extensions import (
    PynixdCollectGarbageResponse,
    PynixdGCAction,
    QueryClosureRequest,
    QueryPathInfosRequest,
)
from .exceptions import BackendError, GCNotPermittedError
from .liveness import RootsTracker, walk_volatile
from .local_store_db import resolve_db_path
from .store import is_http_binary_cache

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from .connection import ClientConn, Connection
    from .context import PynixdContext
    from .store.base import Store

log = structlog.get_logger(__name__)

_MAX_FREED = 2**63 - 1

_DELETE_BATCH_SIZE = 100
_DELETE_TIMEOUT_S = 60
"""Bound each delete and recovery query; cancelled connections are discarded."""


def _physical_batch_total(top_paths: Iterable[str], store_dir: Path | None) -> int | None:
    """Block bytes the batch holds, hardlinks counted once, or `None`.

    The daemon counts `st.st_size` per deleted file (`deletePath` in
    `file-system.cc`), so sparse files count their holes and two-link
    files count twice -- the number `nix store gc` prints has the same
    fiction. An operator deciding by `df` needs blocks, so this walks
    the batch with `lstat` (never following symlinks), dedupes by
    `(st_dev, st_ino)`, and sums `st_blocks * 512`. `None` on anything
    unmeasurable -- no layout, a path that vanished, an unreadable
    directory -- and the caller falls back to the daemon number.
    """
    if store_dir is None:
        return None
    total = 0
    seen: set[tuple[int, int]] = set()
    queue: deque[Path] = deque()
    for top in top_paths:
        if store_dir != Path("/nix/store") and top.startswith("/nix/store/"):
            queue.append(store_dir / Path(top).relative_to("/nix/store"))
        else:
            queue.append(Path(top))
    try:
        while queue:
            current = queue.popleft()
            try:
                info = current.lstat()
            except OSError:
                return None
            key = (info.st_dev, info.st_ino)
            if key in seen:
                continue
            seen.add(key)
            total += info.st_blocks * 512
            if stat.S_ISDIR(info.st_mode):
                try:
                    queue.extend(current.iterdir())
                except OSError:
                    return None
    except OSError:
        return None
    return total


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


def _by_weight(weights: Mapping[str, tuple[int, int]], pressure: float) -> list[str]:
    """The paths in delete order: size matters more the fuller the disk is.

    Pure, so the order a pass deletes in is asserted without a daemon.
    `weights` maps the path string to `(nar_size, age_seconds)`; `pressure`
    is the disk usage fraction. Both axes normalise against the candidate
    set, and the score blends them by pressure:

        score = pressure * size_norm + (1 - pressure) * age_norm

    An empty disk collects oldest first, a full disk biggest first, and
    every pressure between moves continuously: no threshold flips the order
    between two passes. Descending score, path string breaking ties so the
    order is stable run to run.
    """
    sizes = {path: size for path, (size, _) in weights.items()}
    ages = {path: age for path, (_, age) in weights.items()}
    biggest = max(sizes.values(), default=0)
    oldest = max(ages.values(), default=0)
    scored = {
        path: (
            pressure * (size / biggest if biggest else 0.0)
            + (1.0 - pressure) * (ages[path] / oldest if oldest else 0.0)
        )
        for path, size in sizes.items()
    }
    return [path for path, _ in sorted(scored.items(), key=lambda item: (-item[1], item[0]))]


def _take_until_below_target(ordered: Sequence[tuple[str, int]], used: int, total: int, target: float) -> list[str]:
    """The leading paths whose removal projects usage under `target`.

    Pure, for the same reason. `ordered` is `(path, size)` heaviest first;
    the walk stops at the first prefix whose freed bytes bring `used / total`
    to or under the fraction. A target already met takes nothing; a target
    no prefix meets takes everything.
    """
    batch: list[str] = []
    freed = 0
    for path, size in ordered:
        if (used - freed) / total <= target:
            break
        batch.append(path)
        freed += size
    return batch


def _referrer_first(ordered: Sequence[str], references: Mapping[str, set[str]]) -> list[str]:
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


class Collector:
    """Chooses what leaves the local store, and asks Nix to delete it."""

    def __init__(self, ctx: PynixdContext) -> None:
        self.ctx = ctx

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
        volatile = await run_sync(self._volatile_seeds)
        vetoed = await self._veto_closure(volatile)
        planned = await self.plan()
        spared = {str(path) for path in planned} & vetoed
        veto = LogNext(
            text=f"volatile veto: rechecked {len(volatile)} living roots, spared {len(spared)} planned paths"
        )
        lines.append(veto)
        if client is not None:
            await client.send(veto)
        log.info("gc_volatile_veto", fresh=len(volatile), spared=len(spared))
        weights = await self._weigh({path for path in planned if str(path) not in vetoed})
        if not weights:
            return self._answered(PynixdCollectGarbageResponse(store_paths=set(), bytes=0), lines)
        usage = self._disk_usage(self.ctx.local_store)
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
        ordered = _by_weight(weights, pressure)
        if action != PynixdGCAction.EXECUTE:
            # A dry-run answers the plan, not the pressure: the store's
            # `gc_target_usage` bounds what an execute pass deletes, but a
            # dry-run reports what is droppable, so an under-target disk
            # does not read as clean. Issue #73.
            batch = ordered
        else:
            batch = self._bound_batch(self.ctx.local_store, ordered, weights, usage, target_usage)
        if limit is not None:
            batch = batch[:limit]
        planned_paths = {str(path) for path in planned}
        if set(batch) != planned_paths:
            # Slicing breaks the referrer closure the planners promise, and
            # gc.cc refuses a batch whose referrer rides below the slice.
            # Re-close: referrers in the plan join, seeds that escape it drop.
            # Issues #69 and #70.
            batch = await self._close_batch(self.ctx.local_store, batch, planned_paths)
        if action != PynixdGCAction.EXECUTE:
            self._log_top(batch, weights, pressure)
            return self._answered(
                PynixdCollectGarbageResponse(
                    store_paths={StorePath(path) for path in batch},
                    bytes=sum(weights[path][0] for path in batch),
                ),
                lines,
            )
        batch = await self._order_batch(self.ctx.local_store, batch)
        return await self._delete(batch, weights, lines, client)

    @staticmethod
    def _answered(response: PynixdCollectGarbageResponse, lines: list[LogNext]) -> PynixdCollectGarbageResponse:
        """*response* carrying the pass's wire lines in its log buffer."""
        for line in lines:
            response.logs.add(line)
        return response

    def _volatile_seeds(self) -> set[str]:
        """The living roots, read synchronously for a worker thread.

        `/proc` and `temproots` move constantly and no watch covers them,
        so every pass reads them fresh just before planning. A store
        without a layout has no state to read, and vetoes nothing.
        """
        layout = getattr(self.ctx.local_store, "layout", None)
        if layout is None:
            return set()
        return walk_volatile(layout.state_dir, str(layout.store_dir))

    async def _veto_closure(self, volatile: set[str]) -> set[str]:
        """*volatile* plus what it references: the paths no pass may name.

        The daemon answers the closure without taking the collector lock,
        and a path the veto spares keeps whatever still names it, which is
        what keeps the delete set closed under referrers (`gc.cc:653`). A
        closure the daemon cannot answer -- no feature, a dropped seed --
        falls back to the seeds alone: sparing less than the full closure,
        but never nothing.
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
        if not volatile <= closed:
            log.warning("gc_veto_closure_incomplete", asked=len(volatile), answered=len(closed))
            return volatile
        return closed

    async def _weigh(self, paths: set[StorePath]) -> dict[str, tuple[int, int]] | None:
        """`(nar_size, age_seconds)` per planned path, or `None` when unknown.

        Sizes come from the path infos, ages from the access table; a path
        with no access row weighs age zero, which deprioritises it without
        excluding it. `None` fails the pass closed: a pass that cannot see
        the whole state deletes nothing, and shows nothing either.
        """
        if not paths:
            return {}
        local = self.ctx.local_store
        try:
            # Unmarked: these are the judged, not the living. Marking them
            # would freshen every candidate the pass weighs, and a dry-run
            # must not move what it measures. `mark` is a LocalDBStore
            # capability, not a Store one, so the attribute reads through
            # `getattr` like the other local-only answers here.
            execute = getattr(local, "execute")  # noqa: B009 -- `mark` is LocalDBStore's, not Store's
            resp = await execute(QueryPathInfosRequest(paths=paths), mark=False)
        except Exception:
            log.warning("gc_weigh_infos_failed", exc_info=True)
            return None
        sizes = {str(info.path): info.info.nar_size for info in resp.infos}
        query = getattr(getattr(local, "db", None), "query_access_times", None)
        if query is None:
            log.warning("gc_no_access_tracking")
            return None
        try:
            times = await query(set(sizes))
        except Exception:
            log.warning("gc_weigh_ages_failed", exc_info=True)
            return None
        if times is None:
            return None
        now = time.time()
        return {path: (size, max(0, int(now - times.get(path, now)))) for path, size in sizes.items()}

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

    async def plan(self) -> set[StorePath]:
        """The paths that may leave the store.

        Two rules, picked by `gc_max_age` on the local store. `None` keeps
        the substituter rule below; a number of seconds picks the age rule,
        which needs no substituter: dead and unreferenced for that long goes,
        whether or not any cache holds it. Setting the number is the operator
        taking ownership of the store's old paths, and the default keeps the
        cache behaviour.

        Both rules refresh Python liveness, falling back to Nix only when unavailable.
        """
        local = self.ctx.local_store
        max_age = getattr(local, "gc_max_age", None)
        if max_age is None:
            return await self._plan_deferred(local)
        return await self._plan_lru(local, max_age)

    async def _mirror_live(self, local: Store) -> set[str] | None:
        """The tracker's live set, refreshed just now, or `None` with no mirror.

        The tracker reads roots and SQLite in a worker thread.
        Missing state or a failed refresh returns `None`; callers then ask Nix.
        """
        try:
            layout = getattr(local, "layout", None)
            if layout is None:
                return None
            db_path = resolve_db_path(layout)
            if db_path is None:
                return None
            tracker = RootsTracker(layout.state_dir, str(layout.store_dir), db_path)
            return await run_sync(tracker.refresh)
        except Exception:
            log.warning("gc_mirror_refresh_failed", exc_info=True)
            return None

    async def _plan_deferred(self, local: Store) -> set[StorePath]:
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
        if not stores:
            return set()

        all_paths: set[StorePath] = (await local.execute(QueryAllValidPathsRequest())).paths
        if not all_paths:
            return set()

        held: set[StorePath] = set()
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

        mirror = await self._mirror_live(local)
        if mirror is None:
            live: set[StorePath] = (await local.call(_request(GCAction.RETURN_LIVE, set()))).paths_deleted
        else:
            live = {StorePath(path) for path in mirror}
        droppable = all_paths - keep - live
        log.info("gc_plan", valid=len(all_paths), held=len(held), live=len(live), droppable=len(droppable))
        return droppable

    async def _plan_lru(self, local: Store, max_age: int) -> set[StorePath]:
        """The dead paths nothing referenced for `max_age` seconds.

        `PynixdPathAccess` dates each path. Python resolves the live set.
        A path with no access row stays: "never seen" is not "seen long ago". The set is
        closed under referrers so Nix accepts it (`gc.cc:653`), which pulls
        in dead referrers even when they are fresh — deleting a path takes
        what still names it. A failure anywhere plans nothing: an LRU pass
        that cannot see the whole state deletes nothing.
        """
        all_paths: set[StorePath] = (await local.execute(QueryAllValidPathsRequest())).paths
        if not all_paths:
            return set()

        live_paths = await self._mirror_live(local)
        if live_paths is None:
            live = {str(path) for path in (await local.call(_request(GCAction.RETURN_LIVE, set()))).paths_deleted}
        else:
            live = live_paths
        dead = {str(path) for path in all_paths} - live

        stale = await self._stale_since(local, max_age)
        if stale is None:
            return set()
        seeds = dead & stale

        closed = await self._close_under_referrers(local, seeds)
        if closed is None:
            return set()
        droppable = {path for path in closed if path in dead}
        log.info(
            "gc_plan_lru",
            valid=len(all_paths),
            live=len(live),
            stale=len(stale),
            droppable=len(droppable),
        )
        return {StorePath(path) for path in droppable}

    async def _stale_since(self, local: Store, max_age: int) -> set[str] | None:
        """The paths unreferenced for `max_age` seconds, or `None` when unknown."""
        query = getattr(getattr(local, "db", None), "query_paths_not_referenced_since", None)
        if query is None:
            log.warning("gc_no_access_tracking")
            return None
        try:
            result = await query(max_age)
        except Exception:
            log.warning("gc_stale_query_failed", exc_info=True)
            return None
        if result is None:
            return None
        return {str(path) for path in result}

    async def _close_under_referrers(self, local: Store, seeds: set[str]) -> set[str] | None:
        """`seeds` plus everything that references them, transitively."""
        query = getattr(getattr(local, "db", None), "query_referrer_closure", None)
        if query is None:
            log.warning("gc_no_referrer_closure")
            return None
        try:
            closure: set[str] | None = await query(seeds)
            return closure
        except Exception:
            log.warning("gc_referrer_query_failed", exc_info=True)
            return None

    async def _close_batch(self, local: Store, batch: list[str], planned: set[str]) -> list[str]:
        """The batch closed under referrers again, against the plan.

        Each seed brings its transitive referrers; a seed whose closure
        escapes the plan cannot be deleted now, so it drops instead of
        poisoning the request. Membership in the plan is the liveness
        answer: the planners closed the plan, so every referrer the
        database knows is either planned or live. Seeds first in batch
        order, joined referrers after. An unanswerable database plans
        nothing rather than half a batch.
        """
        query = getattr(getattr(local, "db", None), "query_referrer_closure", None)
        if query is None:
            log.warning("gc_batch_close_unavailable")
            return []
        kept: dict[str, None] = {}
        for seed in batch:
            try:
                closed = await query({seed})
            except Exception:
                log.warning("gc_batch_close_failed", exc_info=True)
                return []
            if closed is None:
                log.warning("gc_batch_close_unavailable")
                return []
            outside = {path for path in closed if path not in planned}
            if outside:
                log.info("gc_batch_seed_dropped", seed=seed, live_referrers=len(outside))
                continue
            for path in closed:
                kept.setdefault(path)
        order = {path: index for index, path in enumerate(batch)}
        return sorted(kept, key=lambda path: (0, order[path]) if path in order else (1, path))

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
        return _referrer_first(batch, references)

    async def _held_by(self, store: Store, paths: set[StorePath]) -> set[StorePath]:
        """The paths of *paths* that *store* confirms it has.

        A store that cannot answer confirms nothing, so an unreachable
        substituter keeps every path it would have released.

        **`WantMassQuery` does not stop the sweep.** That flag asks a client
        that nobody configured to leave the cache alone, and `gc_defer` is an
        operator naming this store. `nix copy --to file://` writes `0` into
        every cache it makes, so honouring it here would make the flag useless
        for a lab cache. `HTTPBinaryCacheStore.query_valid_paths` asks for one
        narinfo at a time, so the sweep cannot flood a cache either way.
        """
        if is_http_binary_cache(store) and store.cache_info.get("WantMassQuery") == "0":
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

        directory = getattr(getattr(local, "layout", None), "real_store_dir", None)
        physical = await run_sync(_physical_batch_total, batch, directory)
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

        async def recheck(conn: Connection, paths: Sequence[str]) -> list[str]:
            # Bypass SQLite and caches: a failed delete can already invalidate paths.
            with anyio.fail_after(_DELETE_TIMEOUT_S):
                response = await conn.call(
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
        pending = list(dict.fromkeys(batch))
        for lap in (1, 2):
            if not pending:
                break
            if lap == 2:
                log.info("gc_delete_retry", paths=len(pending))
            queue = deque(list(group) for group in batched(pending, _DELETE_BATCH_SIZE, strict=False))
            retry: list[str] = []
            while queue:
                async with local.transfer_conn() as conn:
                    while queue:
                        group = queue.popleft()
                        await say(f"deleting '{group[0]}'" if len(group) == 1 else f"deleting {len(group)} paths")
                        try:
                            with anyio.fail_after(_DELETE_TIMEOUT_S):
                                response = await conn.call(
                                    _request(GCAction.DELETE_SPECIFIC, {StorePath(path) for path in group}),
                                    raise_on_error=True,
                                )
                        except BackendError as exc:
                            log.warning("gc_chunk_refused", paths=len(group), reason=str(exc))
                            try:
                                remaining = await recheck(conn, group)
                            except (BackendError, TimeoutError, ConnectionError, EOFError):
                                log.warning("gc_chunk_recheck_failed", paths=len(group), exc_info=True)
                                uncertain.update(group)
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
            async with local.transfer_conn() as conn:
                try:
                    await recheck(conn, group)
                except (BackendError, TimeoutError, ConnectionError, EOFError):
                    log.warning("gc_chunk_recheck_failed", paths=len(group), exc_info=True)
                    conn.dirty = True

        refused = len(set(pending) - deleted)
        if physical is not None and deleted == set(batch):
            freed = physical
        elif physical is not None and recovered:
            remaining_bytes = await run_sync(_physical_batch_total, set(batch) - deleted, directory)
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

    def _bound_batch(
        self,
        local: Store,
        ordered: list[str],
        weights: dict[str, tuple[int, int]],
        usage: tuple[int, int] | None,
        target_override: float | None = None,
    ) -> list[str]:
        """The leading paths to delete: all planned, or down to the target.

        `gc_target_usage` bounds the pass by disk pressure: in weight order,
        stopping once the freed bytes project usage to or under the
        fraction. The hourly loop then relieves a full disk over several
        passes rather than emptying the plan at once. `None` keeps one
        unbounded pass. An unreadable usage keeps it too: the target cannot
        bind what nobody measured. A per-call override wins over the store
        for one pass.
        """
        target = target_override if target_override is not None else getattr(local, "gc_target_usage", None)
        if target is None or usage is None:
            return ordered
        used, total = usage
        batch = _take_until_below_target([(path, weights[path][0]) for path in ordered], used, total, target)
        log.info(
            "gc_bounded_pass",
            target=target,
            usage=used / total,
            batch=len(batch),
            planned=len(ordered),
        )
        return batch

    @staticmethod
    def _disk_usage(local: Store) -> tuple[int, int] | None:
        """`(used, total)` bytes of pressure on the filesystem holding the store."""
        try:
            directory = getattr(getattr(local, "layout", None), "real_store_dir", None)
            if directory is None:
                return None
            return _pressure_usage(directory)
        except Exception:
            log.warning("gc_disk_usage_failed", exc_info=True)
            return None


def _pressure_usage(directory: str | Path) -> tuple[int, int]:
    """`(used, total)` bytes with pressure measured the way `df` measures it.

    `shutil.disk_usage` counts `bfree` (blocks free to anyone, root
    included) as free, but a writer that is not root can only spend
    `bavail`. On btrfs the two diverge by tens of percent of the disk,
    so a `bfree` gauge reads relief long before `df` -- and kubelet --
    agree. Read `statvfs` directly and count everything but `bavail` as
    used. Issue nixidae/nixidae#11.
    """
    stat = os.statvfs(directory)
    total = stat.f_blocks * stat.f_frsize
    return ((stat.f_blocks - stat.f_bavail) * stat.f_frsize, total)


class LivenessWatch:
    """Refresh Python liveness without taking the daemon GC lock."""

    def __init__(self, tracker: RootsTracker) -> None:
        self.tracker = tracker

    @classmethod
    def from_local(cls, local: Store) -> LivenessWatch | None:
        """A watch over *local*, or `None` when it cannot host a mirror.

        The mirror lives in the store's own database beside Nix's tables,
        and reads the store's own roots directories: without a layout there
        is neither. `None` is not an error -- a store served without its
        state simply gathers no evidence.
        """
        layout = getattr(local, "layout", None)
        if layout is None:
            return None
        db_path = resolve_db_path(layout)
        if db_path is None:
            return None
        return cls(RootsTracker(layout.state_dir, str(layout.store_dir), db_path))

    async def refresh(self) -> set[str]:
        """Refresh the mirror without asking Nix anything.

        Stable-root changes and the configured interval trigger refreshes.
        """
        return await run_sync(self.tracker.refresh)
