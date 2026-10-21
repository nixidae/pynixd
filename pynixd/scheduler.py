"""
Scheduler for build distribution.

Runs scheduling passes triggered by:
- New builds enqueued
- Build completes (slot opens)
- Path transfer completes (availability changes)

DAG-aware: builds are only schedulable when all input_srcs are present
in the local store. If inputs are missing but available on a remote store,
they are pulled automatically.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, cast

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID

from . import metrics, state
from .allocator import BuildAllocator, PreferLocal, RankedStore, RankedStores, TelemetryStoreRanker, pick_store
from .build_queue import BuildQueue, QueuedBuild
from .build_stats import BuildObservation, StoreLink, identity_of, slot_wait_seconds, transfer_seconds
from .daemon_extensions.query_closure_with_info import QueryClosureWithInfoRequest
from .exceptions import BackendError, InfrastructureError, error_text, leaves
from .goals.results import result_succeeded
from .serde import AddTempRootRequest, AddTempRootsRequest, LogNext, QueryValidPathsRequest
from .store import DaemonStore, LocalDBStore
from .store.transfer import stream_paths_store_to_store
from .store_path import StorePath
from .substitution_queue import SubstitutionQueue
from .system_features import effective_required_features
from .wire import FEATURE_ADD_TEMP_ROOTS

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping

    from anyio.abc import TaskGroup

    from nix_daemon_protocol.aliases import StorePathSet
    from nix_daemon_protocol.ids import BuildId, RequestId, StoreId
    from nix_daemon_protocol.logs import LogMessage

    from .connection import ClientConn, Connection
    from .context import PynixdContext
    from .derived_path import DerivedPath
    from .serde import (
        BuildDerivationRequest,
        BuildDerivationResponse,
        SetOptionsRequest,
    )

log = structlog.get_logger(__name__)


_LINK_REREAD_SECONDS = 60.0
"""How long a store's measured link stands before the next read."""


def _ms_since(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


class _OutputClock:
    """The log sink of a running build: it notes when each message arrives."""

    options = None

    def __init__(self, build: QueuedBuild) -> None:
        self._build = build

    async def send(self, msg: LogMessage) -> None:
        del msg
        self._build.last_output_at = time.monotonic()


def build_decision_seeds(drv_path: str, input_srcs: Iterable[str]) -> set[str]:
    """The seed set a build decision records, as build-kind.

    The derivation plus the inputs its request names: no derivation file
    is read, because the wire request already carries every input path.
    Deeper layers resolve through the store's own references at flush --
    if they needed building, their own decisions record them in turn.
    Issue #65.
    """
    return {drv_path, *input_srcs}


class Scheduler:
    """Schedules builds across stores based on locality and DAG deps."""

    def __init__(
        self,
        ctx: PynixdContext,
    ) -> None:
        self.ctx = ctx
        self.queue = BuildQueue()
        metrics.watch_queue(self.queue)
        metrics.watch_waiting(lambda: state.pending_reasons(self))
        self.substitution_queue = SubstitutionQueue(ctx)
        self.local_store = ctx.local_store
        self._dynamic_feature_matrix: dict[str, set[str]] = {}

        self.ranker = TelemetryStoreRanker(ctx.settings)
        self.allocator = BuildAllocator(self.stores, self.local_store, self.ranker)
        self.trigger_event = anyio.Event()
        self.running = False
        self._workers: TaskGroup | None = None
        self.held_by_pressure: set[BuildId] = set()
        """Builds the last pass left pending because each candidate store
        was at its build limit."""
        self._links: dict[StoreId, tuple[float, StoreLink]] = {}
        """Each store's measured link, and when it was read."""
        for store in [*self.stores.values(), self.local_store]:
            store.on_capacity = self.trigger

    @property
    def stores(self) -> dict[StoreId, DaemonStore]:
        return {
            store_id: store
            for store_id, store in self.ctx.stores.items()
            if isinstance(store, DaemonStore) and not store.no_schedule
        }

    @property
    def dynamic_feature_matrix(self) -> dict[str, set[str]]:
        return self._dynamic_feature_matrix

    def add_dynamic_feature(self, system: str, feature: str = "") -> None:
        """Register that a system+feature combo may become available.

        Used by Kubernetes controllers or config to declare that builders
        for a given platform exist, even if no store is currently connected.
        Builds requiring these features will queue instead of failing.
        """
        self._dynamic_feature_matrix.setdefault(system, set())
        if feature:
            self._dynamic_feature_matrix[system].add(feature)
        self.trigger()

    def add_dynamic_features(self, feature_matrix: dict[str, set[str]]) -> None:
        """Merge an entire feature_matrix into the dynamic matrix."""
        for system, features in feature_matrix.items():
            self._dynamic_feature_matrix.setdefault(system, set())
            self._dynamic_feature_matrix[system] |= features
        self.trigger()

    def trigger(self) -> None:
        """Signal that a scheduling pass is needed."""
        self.trigger_event.set()

    def on_store_added(self, store: DaemonStore, dynamic: bool = False) -> None:
        """Hook called by Server when a new store is added.

        If dynamic=True, the store's feature_matrix is also registered in
        the dynamic_feature_matrix so that builds for this platform continue
        to queue even after the store is removed.
        """
        log.info("store_added_to_scheduler", store_id=store.store_id, dynamic=dynamic)
        if isinstance(store, DaemonStore) and not store.no_schedule:
            self.allocator.add_store(store)
            store.on_capacity = self.trigger
        if dynamic and store.feature_matrix:
            self.add_dynamic_features(store.feature_matrix)
        self.trigger()

    def _dynamic_supports(self, system: str, features: set[str] | None = None) -> bool:
        """Check if the dynamic_feature_matrix declares support for a system+features combo."""
        sys_features = self._dynamic_feature_matrix.get(system)
        if sys_features is None:
            return False
        if not features:
            return True
        return features.issubset(sys_features)

    async def drain_store(self, store_id: StoreId, drain_timeout: float = 300.0) -> None:
        """Gracefully drain and stop using a store for new builds."""
        store = self.ctx.stores.get(store_id)
        if not isinstance(store, DaemonStore):
            return

        log.info(
            "draining_store_in_scheduler",
            store_id=store_id,
            drain_timeout=drain_timeout,
        )
        store.draining = True
        self.trigger()

        start = time.monotonic()
        while time.monotonic() - start < drain_timeout:
            if store.in_flight == 0:
                break
            await anyio.sleep(1.0)
        else:
            log.warning("drain_timeout_reached", store_id=store_id)

        pending = await self.queue.get_pending()
        for build in pending:
            if build.assigned_store_id == store_id:
                if build.is_building:
                    log.info(
                        "cancelling_build_for_store_removal",
                        build_id=build.build_id,
                        store_id=store_id,
                    )
                    build.cancel_worker()
                build.reset_for_retry(store_id, f"{store_id} was removed while the build ran")

        self.trigger()

    async def build_derivation(
        self,
        request: BuildDerivationRequest,
        scheduler_request_id: RequestId | None = None,
        derived_paths_for_request: set[DerivedPath] | None = None,
        from_goal_path: bool = False,
        goal_request_id: RequestId | None = None,
        options: SetOptionsRequest | None = None,
    ) -> tuple[BuildId, asyncio.Future[BuildDerivationResponse]]:
        """Add a build to the queue and trigger the scheduler."""
        t0 = time.monotonic()
        hint = None
        if isinstance(self.local_store, LocalDBStore):
            hint = await self.local_store.db.expected_build_ms(identity_of(request.derivation))
        t_hint = time.monotonic()

        res = await self.queue.enqueue(
            request,
            expected_duration=hint,
            scheduler_request_id=scheduler_request_id,
            derived_paths_for_request=derived_paths_for_request,
            from_goal_path=from_goal_path,
            goal_request_id=goal_request_id,
            options=options,
        )
        t_enqueue = time.monotonic()
        self.trigger()

        log.debug(
            "build_derivation_timing",
            drv=str(request.drv_path),
            hint=f"{t_hint - t0:.3f}s",
            enqueue=f"{t_enqueue - t_hint:.3f}s",
            total=f"{t_enqueue - t0:.3f}s",
        )
        return res

    @contextlib.asynccontextmanager
    async def workers(self) -> AsyncIterator[None]:
        """The task group every build runs in. Leaving it cancels them all."""
        async with anyio.create_task_group() as tg:
            self._workers = tg
            try:
                yield
            finally:
                self._workers = None
                tg.cancel_scope.cancel()

    def _spawn_worker(self, build: QueuedBuild, store: DaemonStore) -> None:
        workers = self._workers
        if workers is None:
            raise RuntimeError("builds run only inside Scheduler.workers")
        scope = anyio.CancelScope()
        build.worker = scope
        workers.start_soon(self._run_worker, build, store, scope)

    async def _run_worker(self, build: QueuedBuild, store: DaemonStore, scope: anyio.CancelScope) -> None:
        # One crashed build must not cancel its siblings in the group.
        try:
            # A cancel from before the start lands at the first await only,
            # after `execute_build` has already marked paths and named a store.
            if scope.cancel_called:
                return
            with scope:
                await self.execute_build(build, store)
        except Exception as e:
            # Failed, not left pending: the loop would retry a deterministic
            # crash on every pass. `fail` raises ValueError for a pruned build.
            log.exception("build_worker_crashed", build_id=build.build_id)
            with contextlib.suppress(ValueError):
                await self.queue.fail(build.build_id, f"pynixd internal error: {error_text(e)}")
        finally:
            build.worker_ended(scope)
            self.trigger()

    async def start(self) -> None:
        """Run the scheduler loop. Its end cancels every build."""
        self.running = True
        log.info("scheduler_started")
        async with self.workers():
            while self.running:
                try:
                    await self.trigger_event.wait()
                    self.trigger_event = anyio.Event()
                    await self.schedule()
                    await anyio.sleep(0.01)
                except Exception:
                    log.exception("scheduler_pass_failed")
                    await anyio.sleep(1.0)

    async def stop(self) -> None:
        """Stop the scheduler loop, which cancels every build."""
        self.running = False
        self.trigger()

    async def close(self) -> None:
        """Alias for stop() for consistency."""
        await self.substitution_queue.close()
        await self.stop()

    async def schedule(self) -> None:
        """The core scheduling logic.

        1. Populate metadata for builds that need it.
        2. Filter schedulable vs waiting builds.
        3. Assign schedulable builds to stores.
        4. Update store metrics.
        """
        pending = await self.queue.get_pending()
        if not pending:
            self._update_store_metrics()
            return

        schedulable, override_in_flight, running_builds = self._filter_schedulable(pending)

        waiting_slot = await self._assign_to_stores(schedulable, override_in_flight, running_builds)

        self._update_store_metrics()

        log.debug(
            "scheduling_pass_done",
            pending=len(pending),
            waiting_slot=len(waiting_slot),
            in_flight={s.store_id: s.in_flight for s in self.stores.values()},
            cpu_util={
                s.store_id: f"{s.cpu_util.utilization:.1f}%" if s.cpu_util else None for s in self.stores.values()
            },
        )

    def _filter_schedulable(
        self,
        pending: list[QueuedBuild],
    ) -> tuple[list[QueuedBuild], dict[StoreId, int], dict[StoreId, int]]:
        """Triage pending builds into schedulable.

        Returns (schedulable, override_in_flight, running_builds).
        override_in_flight accounts for builds assigned this cycle but not yet
        reflected in ``store.in_flight``.

        **`running_builds` counts builds, and `override_in_flight` counts
        connections.** `DaemonStore.in_flight` at `store/daemon.py:221` answers
        `pool.active_connections`, so a single `QueryPathInfo` raises it. The
        ranker wants that number, because a busy store is a poor place to send
        more work. `max-jobs` wants the other one, and the two are not
        interchangeable: `_local_slot_is_full` read `override_in_flight` first,
        so one active connection filled the single slot of `-j1` and every
        build waited for a completion that no build was running to give.
        Issue nixidae/nixidae#235.
        """
        schedulable: list[QueuedBuild] = []

        # Count builds that are already building per store (may exceed
        # store.in_flight because the counter hasn't been updated yet).
        assigned_count: dict[StoreId, int] = {s.store_id: 0 for s in self.stores.values()}
        for build in pending:
            if build.is_building and build.assigned_store_id:
                assigned_count[build.assigned_store_id] += 1

        # Use the max of the store's reported in_flight and our assigned_count
        # to avoid undercounting during rapid scheduling passes.
        override_in_flight: dict[StoreId, int] = {
            s.store_id: max(s.in_flight, assigned_count[s.store_id]) for s in self.stores.values()
        }

        for build in pending:
            if build.is_building:
                continue

            # All builds are schedulable; missing inputs are
            # discovered and pulled during execution.
            schedulable.append(build)

        return schedulable, override_in_flight, assigned_count

    async def _assign_to_stores(
        self,
        schedulable: list[QueuedBuild],
        override_in_flight: dict[StoreId, int],
        running_builds: dict[StoreId, int],
    ) -> list[QueuedBuild]:
        """Assign schedulable builds to backends.

        Handles tiny-build fast-track, standard remote assignment, and
        permanent failure for builds with no compatible store.

        Re-checks build state after ranking to avoid TOCTOU races.
        Returns builds that are waiting for a slot (no store available).
        """
        waiting_slot: list[QueuedBuild] = []
        assigned_this_pass: dict[StoreId, int] = {}
        self.held_by_pressure = set()

        for build in schedulable:
            build_features = effective_required_features(build.request.derivation)

            # Check if another pass already assigned this build
            if build.is_building:
                continue

            # **A build that no live request wants does not take a slot.**
            # This pass runs from the completion of the build that just
            # failed, so without it the freed slot of `-j1` goes to the next
            # build of a request that has already stopped. `main:build`
            # measured 48 us between the goal system deciding and that build
            # reaching the daemon, which is a race and not a margin.
            #
            # `BuildQueue.complete` writes the fact under its own lock and
            # `Scheduler.execute_build` triggers this pass afterwards, so the
            # answer is already there to read.
            #
            # **It ends the build rather than leaving it pending.** A goal of
            # the same request can still be waiting for it -- an input of a
            # root that has not answered -- and that root is what runs
            # `release_request`, so a build left pending waits for a release that waits
            # for the build. `cancel_unwanted` holds the measurement.
            # Issue nixidae/nixidae#325.
            if self.queue.nobody_wants(build):
                log.debug("build_wanted_by_nobody", build_id=build.build_id)
                await self.queue.cancel_unwanted(build.build_id)
                continue

            transfer = await self._transfer_seconds(build)
            ranked = self.allocator.rank_stores(
                build,
                assigned_this_pass,
                override_in_flight=override_in_flight,
                transfer_seconds=transfer,
            )

            local_store = self.local_store
            local_can_build = (
                local_store.is_healthy
                and not local_store.draining
                and not build.is_blacklisted(local_store.store_id)
                and local_store.supports_derivation(build.request.derivation.platform, build_features)
            )
            prefer_local = build.request.derivation.build_local and local_can_build
            # Nix weighs no load for a `preferLocalBuild` derivation, so the
            # local store stays a candidate under `min_schedule_score`.
            # Dropped, the build waits for a slot no running build frees:
            # PSI at 71 % on dynhetz hangs every `writeText` that way.
            if prefer_local and not any(c.store_id == local_store.store_id for c in ranked):
                ranked = RankedStores([*ranked, RankedStore(local_store.store_id, 0.0, local_store)])
            local_is_fallback = not ranked and local_can_build

            if not ranked and not local_is_fallback and not self._has_compatible_store(build, build_features):
                await self._fail_no_compatible_store(build, build_features)
                continue

            if ranked or local_is_fallback:
                candidates = list(ranked) if ranked else [RankedStore(local_store.store_id, 0.0, local_store)]
                roomy = [c for c in candidates if self._has_build_room(c.store, running_builds, assigned_this_pass)]
                if not roomy:
                    self.held_by_pressure.add(build.build_id)
                    waiting_slot.append(build)
                    continue
                rs = pick_store(
                    roomy,
                    local_store.store_id,
                    local_slot_full=self._local_slot_is_full(build, running_builds, assigned_this_pass),
                    prefer_local=PreferLocal(
                        wait_seconds=self._local_wait_seconds(),
                        transfer_seconds=transfer,
                        weight=self.ranker.settings.prefer_local_weight,
                    )
                    if prefer_local
                    else None,
                )
                if rs is None:
                    waiting_slot.append(build)
                    continue
                log.debug(
                    "build_assigned_to_store",
                    build_id=build.build_id,
                    store_id=rs.store_id,
                    score=rs.score,
                )
                # Named here, and not in `execute_build` alone. The count of
                # running builds reads this field, and a task that has not
                # started yet would otherwise carry no store and go uncounted.
                build.assigned_store_id = rs.store_id
                self._spawn_worker(build, rs.store)
                assigned_this_pass[rs.store_id] = assigned_this_pass.get(rs.store_id, 0) + 1
            else:
                # All compatible stores are busy, or this build can't be placed
                all_compatible = [
                    s
                    for s in self.stores.values()
                    if s.supports_derivation(build.request.derivation.platform, build_features)
                ]
                if all_compatible and all(build.is_blacklisted(s.store_id) for s in all_compatible):
                    await self._fail_all_compatible_blacklisted(build, build_features, all_compatible)
                    continue
                waiting_slot.append(build)

        return waiting_slot

    async def _transfer_seconds(self, build: QueuedBuild) -> dict[StoreId, float]:
        """Each remote store's expected delegation seconds for this build, where measured.

        Read once per build and store, and kept on the build: a pass ranks
        every pending build again.
        """
        if not isinstance(self.local_store, LocalDBStore):
            return {}
        identity = identity_of(build.request.derivation)
        for store_id in self.stores:
            if store_id == self.local_store.store_id or store_id in build.transfer_seconds:
                continue
            expected = await self.local_store.db.expected_bytes(identity, str(store_id))
            build.transfer_seconds[store_id] = transfer_seconds(expected, await self._link(store_id))
        return {store_id: seconds for store_id, seconds in build.transfer_seconds.items() if seconds is not None}

    def _local_wait_seconds(self) -> float | None:
        running = [
            (build.expected_duration, build.started_at)
            for build in self.queue.by_id.values()
            if build.is_building and build.assigned_store_id == self.local_store.store_id
        ]
        return slot_wait_seconds(running, time.monotonic())

    async def _link(self, store_id: StoreId) -> StoreLink:
        if not isinstance(self.local_store, LocalDBStore):
            return StoreLink()
        cached = self._links.get(store_id)
        if cached is not None and time.monotonic() - cached[0] < _LINK_REREAD_SECONDS:
            return cached[1]
        link = await self.local_store.db.store_link(str(store_id))
        self._links[store_id] = (time.monotonic(), link)
        return link

    @staticmethod
    def _has_build_room(
        store: DaemonStore,
        running_builds: Mapping[StoreId, int],
        assigned_this_pass: Mapping[StoreId, int],
    ) -> bool:
        """Whether the build limit of *store* lets one more build start. Issue #98."""
        running = running_builds.get(store.store_id, 0) + assigned_this_pass.get(store.store_id, 0)
        return store.build_limit.has_room(running)

    def _local_slot_is_full(
        self,
        build: QueuedBuild,
        running_builds: Mapping[StoreId, int],
        assigned_this_pass: Mapping[StoreId, int],
    ) -> bool:
        """Answer whether `max-jobs` of the client leaves no local slot.

        This is `Worker::waitForBuildSlot` at `worker.cc:261`, which counts
        `getNrLocalBuilds()` against `settings.maxBuildJobs` and starts no
        further local build until one ends. A build on a backend costs no
        slot, and this method reaches only the local store for that reason.

        **The option set of the build decides, and not a setting of pynixd.**
        `QueuedBuild.options` holds the set of the client that made the build,
        which is the same rule that `store.build_conn` takes. A build with no
        options is one that pynixd made itself, and it takes no limit: the
        request behind it named none.

        `main:build` of the functional suite reads this. It builds four
        fixed-output derivations that give the wrong hash, with `-j1`, and it
        asserts one `error:` line. `_build_slots` of `goals/requests.py`
        limited the root goals of one request already, and that is not the
        whole fan-out: a root goal realises the input derivations of its
        derivation at the same time, and each one is a separate build. Three
        builds ran together under `-j1` for that reason. Issue nixidae/nixidae#235.

        **`running_builds` counts builds, and not connections.**
        `DaemonStore.in_flight` answers `pool.active_connections`, which one
        `QueryPathInfo` raises. This method read that number first, so a
        single query filled the one slot of `-j1`, every build waited, and no
        build was running to end and trigger the next pass. The `ca` suite
        went from about a minute to more than ten. `_filter_schedulable`
        answers the build count beside it.

        **Each request counts its own builds.** `getNrLocalBuilds()` is the
        count of one worker, and Nix runs a worker per connection, so the
        builds of another client take none of this client's slots. A build
        that several requests want starts when any of them has a slot, as the
        first of two Nix workers would. Counting every local build against
        the first client's limit held an `ekn render` with `-j2` behind two
        hung test builds of other clients for 58 hours. Issue #89.

        A session runs one request at a time, so the goal request is the
        connection here. A build with no goal request -- a standalone
        `BuildDerivation` -- keeps the shared count below.
        """
        wanters = self.queue.live_wanters(build)
        if wanters:
            return not any(self.request_has_a_local_slot(request_id) for request_id in wanters)
        options = build.options
        if options is None:
            return False
        slots = max(1, int(options.max_build_jobs))
        local_id = self.local_store.store_id
        running = running_builds.get(local_id, 0) + assigned_this_pass.get(local_id, 0)
        if running < slots:
            return False
        log.debug(
            "build_waits_for_a_local_slot",
            build_id=build.build_id,
            running=running,
            max_build_jobs=slots,
        )
        return True

    def request_has_a_local_slot(self, request_id: RequestId) -> bool:
        """Whether *request_id* runs fewer local builds than its `max-jobs`.

        Counted from the queue on each call, not from the pass: a build that
        this pass assigned already has its task, so it counts as building.
        A walk of the queue per check is cheap at the queue sizes pynixd sees.
        """
        jobs = self.queue.max_jobs_of(request_id)
        if jobs is None:
            return True
        local_id = self.local_store.store_id
        running = sum(
            1
            for b in self.queue.by_id.values()
            if b.is_building and b.assigned_store_id == local_id and request_id in b.holders
        )
        if running < max(1, jobs):
            return True
        log.debug("request_waits_for_a_local_slot", request_id=int(request_id), running=running, max_build_jobs=jobs)
        return False

    def _has_compatible_store(
        self,
        build: QueuedBuild,
        build_features: set[str] | None,
    ) -> bool:
        """Check if any live or dynamic store could ever support this build."""
        all_stores = list(self.stores.values())
        return any(
            s.store_id != self.local_store.store_id
            and s.supports_derivation(build.request.derivation.platform, build_features)
            for s in all_stores
        ) or self._dynamic_supports(build.request.derivation.platform, build_features)

    async def _fail_no_compatible_store(
        self,
        build: QueuedBuild,
        build_features: set[str] | None,
    ) -> None:
        """Permanently fail a build with no compatible store."""
        error_msg = f"No compatible store for {build.request.derivation.platform}" + (
            f" (requires {', '.join(sorted(build_features))})" if build_features else ""
        )
        await self.queue.fail(build.build_id, error_msg)
        for line in error_msg.split("\n"):
            await build.post_log_and_fanout(LogNext(text=f"pynixd: {line}\n"))

    async def _fail_all_compatible_blacklisted(
        self,
        build: QueuedBuild,
        build_features: set[str] | None,
        compatible: list[DaemonStore],
    ) -> None:
        """Permanently fail a build blacklisted by all compatible stores."""
        error_msg = (
            f"All compatible stores failed for {build.request.derivation.platform}"
            + (f" (requires {', '.join(sorted(build_features))})" if build_features else "")
            + ":"
            + "".join(
                f"\n  {s.store_id}: {build.store_errors.get(s.store_id, 'no error recorded')}" for s in compatible
            )
        )
        await self.queue.fail(build.build_id, error_msg)
        for line in error_msg.split("\n"):
            await build.post_log_and_fanout(LogNext(text=f"pynixd: {line}\n"))

    def _update_store_metrics(self) -> None:
        """Update per-store metrics."""
        for s in self.stores.values():
            metrics.STORE_HEALTHY.labels(store_id=s.store_id).set(
                1 if s.is_healthy else 0,
            )
            if s.cpu_util:
                metrics.STORE_CPU_UTILIZATION.labels(store_id=s.store_id).set(
                    s.cpu_util.utilization,
                )

    @staticmethod
    async def _say_where_it_builds(build: QueuedBuild, store: DaemonStore) -> None:
        """Name the backend, but only when the backend is not the local one.

        Nix writes `building '<drv>'...` for a local build, and `building
        '<drv>' on '<machine>'...` for a remote one. The location is news only
        in the second case, and the backend daemon writes the first line
        itself, which pynixd forwards to the client.

        pynixd wrote `pynixd: starting build on local at <timestamp>` for every
        build, and two faults came from that. The extra line broke
        `main:cli-characterisation`, which compares the output of a command
        against a recorded `.exp` file. The timestamp also made the output
        different on each run, so no two recordings of one build could agree.
        """
        if store.store_id == LOCAL_STORE_ID:
            return
        await build.post_log_and_fanout(LogNext(text=f"pynixd: building on {store.store_id}\n"))

    async def execute_build(self, build: QueuedBuild, store: DaemonStore) -> None:
        """Execute build on a store, handling inputs and outputs.

        Internal phases:
        1. _prepare_build  — CA registration, resolve, strip features, stream inputs
        2. _execute        — Call backend via build_conn
        3. _collect_outputs — Pull outputs, register realisations, record stats
        """
        build.assigned_store_id = store.store_id
        # The decision is the use-event for the closure: queue the
        # derivation plus the inputs its request names as build seeds.
        # Synchronous queueing only -- the flush expands and writes on
        # its own schedule -- so recording never slows the build start.
        # Issue #65.
        db = getattr(self.local_store, "db", None)
        if db is not None:
            db.mark_paths(
                build_decision_seeds(
                    str(build.request.drv_path),
                    (str(p) for p in build.request.derivation.input_srcs),
                ),
                kind="build",
            )
        build_resp: BuildDerivationResponse | None = None
        completed = False
        try:
            async with store.build_conn(build.options) as conn:
                observation = build.observation = BuildObservation(
                    store_id=str(store.store_id),
                    status=-1,
                    queued_ms=_ms_since(build.enqueued_at),
                )
                inputs_started = time.monotonic()
                observation.inputs_bytes = await self._prepare_build(build, store, conn)
                observation.inputs_ms = _ms_since(inputs_started)
                await self._say_where_it_builds(build, store)
                build_resp = await self._execute(build, store, conn)
                if build.started_at is not None:
                    observation.build_ms = _ms_since(build.started_at)
                # Every success, not `BUILT` alone. A builder that already
                # holds the outputs answers `ALREADY_VALID`, and the local
                # store still lacks them. A dynamic derivation's produced
                # `.drv` then failed with "derivation not found" on each run
                # until the builder collected it. nixidae/nixidae#998.
                if result_succeeded(build_resp.result):
                    # The outputs first, and the completion after. It ran the
                    # other way round and logged a collection failure on the
                    # server, so a client held a successful BuildResult and
                    # then asked the local store for a path that is not there.
                    # The error it got named the missing path, never the copy
                    # that failed. Issue #12.
                    try:
                        await self._collect_outputs(build, store, conn, build_resp)
                    except Exception as e:
                        log.exception("output_pull_failed", build_id=build.build_id, store_id=store.store_id)
                        reason = f"built on {store.store_id}, but pynixd could not collect the outputs: {e}"
                        await build.post_log_and_fanout(LogNext(text=f"pynixd: {reason}\n"))
                        await self.queue.fail(build.build_id, reason)
                        self.trigger()
                        return

                    await self.queue.complete(build.build_id, build_resp)
                    completed = True
                    self.trigger()

        except Exception as e:
            # Decided by the leaves: a task group on the way wraps a
            # BackendError in a group, which no `except BackendError` matches.
            reason = error_text(e)
            if all(isinstance(leaf, BackendError | InfrastructureError) for leaf in leaves(e)):
                log.warning("build_failed_retryable", build_id=build.build_id, error=reason)
                await build.post_log_and_fanout(
                    LogNext(text=f"pynixd: build failed on {store.store_id}, retrying: {reason}\n")
                )
                build.reset_for_retry(store.store_id, reason)
            else:
                log.exception("build_crashed", build_id=build.build_id)
                await build.post_log_and_fanout(LogNext(text=f"pynixd: internal error, failing build: {reason}\n"))
                await self.queue.fail(build.build_id, f"pynixd internal error: {reason}")
            self.trigger()

        if build_resp is not None and not completed:
            await self.queue.complete(build.build_id, build_resp)
            self.trigger()

    async def _prepare_build(
        self,
        build: QueuedBuild,
        store: DaemonStore,
        conn: Connection,  # build connection (opaque to this method)
    ) -> int:
        """Register CA realisations, resolve deferred derivations, stream inputs; answer the NAR bytes sent.

        All operations that must happen before the build request is sent
        to the backend daemon.
        """
        # Strip pynixd-handled features from requiredSystemFeatures
        self.allocator.strip_handled_features(build)

        # 1. Filter inputs already present on the builder store
        input_srcs = build.request.derivation.input_srcs
        if input_srcs:
            paths_to_check = {StorePath(path=str(p)) for p in input_srcs}
            await self._hold_inputs_on(conn, paths_to_check)
            try:
                check = await store.execute(
                    QueryValidPathsRequest(
                        paths=paths_to_check,
                        substitute=0,
                    )
                )
                valid_paths = [str(cp) for cp in check.paths]
                known = [str(p) for p in input_srcs if str(p) in valid_paths]
            except (BackendError, OSError, ConnectionError):
                known = []
            missing_paths = {StorePath(str(p)) for p in input_srcs if str(p) not in known}
        else:
            missing_paths = set()
        if not missing_paths:
            return 0
        log.debug(
            "build_sending_inputs",
            build_id=build.build_id,
            store_id=store.store_id,
            count=len(missing_paths),
        )
        return await stream_paths_store_to_store(
            self.local_store,
            store,
            missing_paths,
            peer_store_id=store.store_id,
            direction="out",
        )

    async def _hold_inputs_on(self, conn: Connection, inputs: set[StorePath]) -> None:
        """Root the closure of *inputs* on the builder, for the build connection's life.

        Before the validity check, as `copyPaths` in `src/libstore/store-api.cc`
        does: a path the builder holds and its collector deletes between the
        check and the copy is neither sent nor present, and the copy fails on
        it. The whole closure and not the inputs alone, because a root keeps
        alive only the closure of a path that is valid, and an input the
        builder lacks protects none of its references there. Issue #887.
        """
        closure = await self.local_store.execute(QueryClosureWithInfoRequest(paths=inputs), client=None)
        held = inputs | {info.path for info in closure.infos}
        if FEATURE_ADD_TEMP_ROOTS in conn.standard_features:
            await conn.call(AddTempRootsRequest(paths=held), raise_on_error=True)
            return
        for path in sorted(held, key=str):
            await conn.call(AddTempRootRequest(path=path), raise_on_error=True)

    async def _execute(
        self,
        build: QueuedBuild,
        store: DaemonStore,
        conn: Connection,
    ) -> BuildDerivationResponse:
        """Call the backend daemon and return the response.

        Stderr is buffered in the response, then fanned out to the build's
        subscribers. `_OutputClock` only notes when each message arrives:
        a fan-out from inside the read would let a client that stops reading
        stall the upstream build.
        """
        log.debug("build_executing", build_id=build.build_id, store_id=store.store_id)
        build.started_at = build.last_output_at = time.monotonic()
        if build.wait_time is not None:
            metrics.QUEUE_WAIT_DURATION.observe(build.wait_time)

        resp = await conn.call(build.request, client=cast("ClientConn", _OutputClock(build)), options=build.options)
        if resp.logs.messages:
            # The count, so a run can tell one fan-out of many messages from
            # many fan-outs of one message.
            log.debug("build_logs_fanned_out", build_id=build.build_id, count=len(resp.logs.messages))
            for msg in resp.logs.messages:
                await build.post_log_and_fanout(msg)
        # **A failure of the build itself gets no log line here.** The message
        # travels in `BuildResult.error_msg`, and the goal that asked for the
        # build decides what a reader sees. `Goal::amDone` at `goal.cc:214`
        # takes the same decision: it logs the failure of a goal that another
        # goal waits for, and it stays quiet for a goal at the top, because
        # the caller reports that one.
        #
        # pynixd wrote one `pynixd: <line>` for each line of the message. A
        # `BuildPathsWithResults` request then carried a log that `nix-daemon`
        # does not send, and the client printed the same text twice. Issue
        # nixidae/nixidae#227.
        log.debug(
            "build_executed",
            build_id=build.build_id,
            status=resp.result.status,
        )
        return resp

    async def _collect_outputs(
        self,
        build: QueuedBuild,
        store: DaemonStore,
        conn: Connection,  # build connection
        resp: BuildDerivationResponse,
    ) -> None:
        """Pull outputs back to local store, register realisations, record stats.

        Runs after a successful build execution.
        """
        if not result_succeeded(resp.result):
            return

        # Pull outputs from remote store to local store
        ca_output_paths: StorePathSet = set()
        # `realised_outputs` reads whichever of the two wire shapes the answer
        # carried. Issue #14.
        realised = resp.result.realised_outputs()
        if realised:
            for realisation in realised.values():
                out_path = StorePath(str(realisation.out_path)) if realisation.out_path else StorePath("")
                if out_path:
                    ca_output_paths.add(
                        out_path.with_store_prefix(),
                    )
            build.ca_realisations = list(realised.values())

        outputs = build.request.derivation.output_paths()
        static_paths = {p for p in outputs.values() if p != StorePath("")}
        all_output_paths = static_paths | ca_output_paths
        for path in all_output_paths:
            self.ctx.output_locations[str(path)] = store.store_id
        log.info(
            "pulling_paths",
            store_id=store.store_id,
            count=len(all_output_paths),
        )

        # The record after the pull, so it holds the pull's cost, and in
        # `finally`, so a failed pull keeps the rest. Issue #12.
        pull_started = time.monotonic()
        try:
            pulled = await self._pull_outputs(store, all_output_paths)
            if build.observation is not None:
                build.observation.pull_ms = _ms_since(pull_started)
                build.observation.pull_bytes = pulled
        finally:
            await self._record_observation(build, resp)

    async def _record_observation(self, build: QueuedBuild, resp: BuildDerivationResponse) -> None:
        """Write what this build cost, for the next build of the same identity.

        Every success, `ALREADY_VALID` included: a build that built nothing
        measures what delegating it cost. The estimator reads `BUILT` alone.
        nixidae/nixidae#1001.
        """
        observation = build.observation
        if not isinstance(self.local_store, LocalDBStore) or observation is None:
            return
        observation.status = int(resp.result.status)
        observation.cpu_user_us = resp.result.cpu_user.value if resp.result.cpu_user else None
        observation.cpu_system_us = resp.result.cpu_system.value if resp.result.cpu_system else None
        identity = identity_of(build.request.derivation)
        await self.local_store.db.record_build_observation(identity, observation)
        log.info(
            "build_observed",
            name=identity.name,
            store_id=observation.store_id,
            status=observation.status,
            expected_ms=build.expected_duration,
            build_ms=observation.build_ms,
            inputs_ms=observation.inputs_ms,
            pull_ms=observation.pull_ms,
        )

    async def _pull_outputs(
        self,
        store: DaemonStore,
        paths: StorePathSet,
    ) -> int:
        """Bring the outputs of a backend build into the local store, answering the NAR bytes moved.

        The local store has to hold the paths, and a record of where they came
        from is not enough. A client on a Unix socket never asks pynixd for a
        NAR: `UDSRemoteStore::narFromPath` calls `Store::narFromPath`, which
        reads the store directory through `LocalFSStore::getFSAccessor`. So
        `nix copy --from unix://...` of a backend-built path reported "path
        ... does not exist" and named the local store directory -- issue nixidae/nixidae#199.

        `ctx.output_locations` still holds, and it still answers for the
        clients that do use the wire, such as `ssh-ng://` and the HTTP cache.
        This is the transfer that path mapping cannot replace.

        One route, over the wire, for every backend. A second route copied the
        store directory and then the SQLite rows of the builder database
        straight into the local one, when both ends were a `LocalDBStore`.
        Issue nixidae/nixidae#197 removed it, and it was wrong in four ways:

        - No NAR was written and no hash was checked, so a truncated copy
          registered as valid.
        - The `Refs` copy carried only rows whose referrer was a new output,
          and inserted with `INSERT OR IGNORE ... SELECT`. A reference whose
          path had no row in the destination yet was dropped in silence.
        - It wrote into a running daemon's database behind that daemon's back.
        - It copied `ultimate` verbatim, which marked the local store as the
          builder of a path it never built. Nix sets `ultimate` in
          `unix/build/derivation-builder.cc` alone -- the local builder -- and
          clears it on every copy. A distributed build leaves `ultimate =
          false` in the store that receives the output, and the shortcut said
          `true`.

        `stream_paths_store_to_store` has none of those. It asks for the
        closure, so a reference cannot go missing, and it hands each path to
        the destination daemon, which hashes it and registers it itself.
        """
        if not paths:
            return 0
        log.debug(
            "pull_outputs_streaming",
            store_id=store.store_id,
            count=len(paths),
        )
        return await stream_paths_store_to_store(
            store,
            self.local_store,
            paths,
            peer_store_id=store.store_id,
            direction="in",
        )
