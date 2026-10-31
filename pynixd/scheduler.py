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

import contextlib
import random
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID

from . import metrics, state
from ._lazy import ssh_errors
from .allocator import (
    BuildAllocator,
    ExcludedStore,
    PreferLocal,
    RankedStore,
    RankedStores,
    TelemetryStoreRanker,
    explore_store,
    pick_store,
)
from .build_queue import BuildQueue, QueuedBuild
from .build_stats import (
    BuildEstimateSource,
    BuildObservation,
    ExpectedBytes,
    StoreEstimate,
    StoreLink,
    identity_of,
    slot_wait_seconds,
    transfer_seconds,
)
from .config import NoLocalJobs
from .daemon_extensions.query_closure_with_info import QueryClosureWithInfoRequest
from .exceptions import BackendError, InfrastructureError, error_text, leaves
from .goals.results import result_succeeded
from .placement_trace import CandidateTrace, Outcome, PlacementDecision, PlacementTrace
from .serde import AddTempRootRequest, AddTempRootsRequest, BuildResultStatus, LogNext, QueryValidPathsRequest
from .store import DaemonStore, LocalDBStore
from .store.transfer import stream_paths_store_to_store
from .store_path import StorePath
from .substitution_queue import SubstitutionQueue
from .system_features import BUILTIN_SYSTEM, effective_required_features
from .wire import FEATURE_ADD_TEMP_ROOTS

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Mapping

    from anyio.abc import TaskGroup

    from nix_daemon_protocol.aliases import StorePathSet
    from nix_daemon_protocol.ids import BuildId, RequestId, StoreId
    from nix_daemon_protocol.logs import LogMessage

    from .answer import Answer
    from .connection import ClientConn, Connection
    from .context import PynixdContext
    from .derived_path import DerivedPath
    from .psi import SystemHealth
    from .serde import (
        BuildDerivationRequest,
        BuildDerivationResponse,
        BuildResult,
        SetOptionsRequest,
    )

log = structlog.get_logger(__name__)


_LINK_REREAD_SECONDS = 60.0
"""How long a store's measured link stands before the next read."""
_PULL_ATTEMPTS = 3
_MISSING_BYTES_TIMEOUT = 5.0
"""Seconds a store has to say which inputs it holds, before its cost counts as unknown."""
_PULL_BACKOFF_SECONDS = 2.0
"""The first wait before a pull again; it doubles. A test sets 0, not a
patched `anyio.sleep`: nixidae/nixidae#1019."""


_MACHINE_FAULTS = (
    "No space left on device",
    "Disk quota exceeded",
    "Cannot allocate memory",
    "Too many open files",
    "Read-only file system",
    "Input/output error",
    "getting status of",
    "is not valid",
    "Killed",
)
"""Words in a failed build's message that blame the builder's machine, not the derivation."""


def machine_fault(result: BuildResult) -> str | None:
    """Why a failed result is the machine's fault, or `None` for the derivation's own failure.

    Nix marks a build that failed with a full disk, or any unsandboxed build,
    `TransientFailure` (`derivation-builder.cc:242-250`), and an error outside
    the builder `MiscFailure` (`derivation-building-goal.cc:805`). The text
    catches a disk that filled and emptied again before Nix looked.
    """
    if result_succeeded(result):
        return None
    status = BuildResultStatus(result.status)
    if status in (BuildResultStatus.TRANSIENT_FAILURE, BuildResultStatus.MISC_FAILURE):
        return status.name.lower()
    message = str(result.error_msg)
    return next((words for words in _MACHINE_FAULTS if words in message), None)


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


def output_paths(build: QueuedBuild, resp: BuildDerivationResponse) -> StorePathSet:
    """The output paths of a successful build: the static ones and the realised ones."""
    realised = resp.result.realised_outputs() or {}
    ca_paths = {StorePath(str(r.out_path)).with_store_prefix() for r in realised.values() if r.out_path}
    static_paths = {p for p in build.request.derivation.output_paths().values() if p != StorePath("")}
    return static_paths | ca_paths


async def add_temp_roots(conn: Connection, paths: StorePathSet) -> None:
    """Root *paths* on *conn*'s daemon for the connection's life."""
    if not paths:
        return
    if FEATURE_ADD_TEMP_ROOTS in conn.standard_features:
        await conn.call(AddTempRootsRequest(paths=paths), raise_on_error=True)
        return
    for path in sorted(paths, key=str):
        await conn.call(AddTempRootRequest(path=path), raise_on_error=True)


def expected_build_seconds(build: QueuedBuild, local_id: StoreId) -> float | None:
    """The build alone: its local estimate, else the shortest any store has."""
    local = build.estimates.get(local_id)
    if local is not None and local.build_seconds is not None:
        return local.build_seconds
    known = [estimate.build_seconds for estimate in build.estimates.values() if estimate.build_seconds is not None]
    return min(known) if known else None


def build_decision_seeds(drv_path: str, input_srcs: Iterable[str]) -> set[str]:
    """The seed set a build decision records, as build-kind.

    The derivation plus the inputs its request names: no derivation file
    is read, because the wire request already carries every input path.
    Deeper layers resolve through the store's own references at flush --
    if they needed building, their own decisions record them in turn.
    Issue #65.
    """
    return {drv_path, *input_srcs}


@dataclass
class _Notes:
    """What one decision of `_assign_one` saw, for the trace."""

    outcome: Outcome | None = None
    reason: str | None = None
    ranked: list[RankedStore] = field(default_factory=list)
    excluded: list[ExcludedStore] = field(default_factory=list)
    roomy: set[StoreId] | None = None
    chosen: StoreId | None = None
    prefer_local: bool = False
    kept_local: bool = False
    local_can_build: bool = False
    local_slot_full: bool | None = None

    def end(self, outcome: Outcome, reason: str | None = None) -> None:
        self.outcome = outcome
        self.reason = reason


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
        self.rng = random.Random()  # noqa: S311 -- placement, not cryptography
        """Draws for `explore_share`; a test seeds or replaces it."""
        self.allocator = BuildAllocator(self.stores, self.local_store, self.ranker)
        self.trigger_event = anyio.Event()
        self.running = False
        self._workers: TaskGroup | None = None
        self.held_by_pressure: set[BuildId] = set()
        """Builds the last pass left pending because each candidate store
        was at its build limit."""
        self.builds_waiting = False
        """Whether the last pass left a build waiting for a store."""
        self._links: dict[StoreId, tuple[float, StoreLink]] = {}
        """Each store's measured link, and when it was read."""
        self.trace = PlacementTrace(None, ctx.settings.placement_trace)
        """Writes nothing until `start` hands it the database, which the local store opens after this."""
        self.pass_id = 0
        self._weights = self.ranker.settings.model_dump_json()
        for store in [*self.stores.values(), self.local_store]:
            store.on_capacity = self.trigger
            if isinstance(store, DaemonStore):
                store.sample_listeners.add(self.on_sample)

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
            store.sample_listeners.add(self.on_sample)
        if dynamic and store.feature_matrix:
            self.add_dynamic_features(store.feature_matrix)
        self.trigger()

    def _dynamic_supports(self, system: str, features: set[str] | None = None) -> bool:
        """Check if the dynamic_feature_matrix declares support for a system+features combo."""
        sys_features = self._dynamic_feature_matrix.get(system)
        if sys_features is None and system == BUILTIN_SYSTEM and self._dynamic_feature_matrix:
            sys_features = set().union(*self._dynamic_feature_matrix.values())
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
        delegated: bool = False,
    ) -> tuple[BuildId, Answer[BuildDerivationResponse]]:
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
            delegated=delegated,
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
        self.trace.db = getattr(self.local_store, "db", None)
        async with self.workers():
            if self._workers is not None and self.trace.db is not None:
                self._workers.start_soon(self.trace.run)
            while self.running:
                try:
                    await self.trigger_event.wait()
                    self.trigger_event = anyio.Event()
                    await self.schedule()
                    await anyio.sleep(0.01)
                except Exception:
                    log.exception("scheduler_pass_failed")
                    await anyio.sleep(1.0)

    def on_sample(self, health: SystemHealth) -> None:
        """Run a pass on a store's sample while a build waits.

        A store the ranking dropped, under `min_schedule_score` or short of
        disk, comes back by its figures alone, and nothing else would wake
        the loop: the build waited until an unrelated event. `on_capacity`
        covers only a rising build limit. nixidae/nixidae#1016.
        """
        del health
        if self.builds_waiting:
            self.trigger()

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
        self.pass_id += 1
        pending = await self.queue.get_pending()
        if not pending:
            self.builds_waiting = False
            self._update_store_metrics()
            return

        schedulable, running_builds = self._filter_schedulable(pending)

        waiting_slot = await self._assign_to_stores(schedulable, running_builds)
        self.builds_waiting = bool(waiting_slot)

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
    ) -> tuple[list[QueuedBuild], dict[StoreId, int]]:
        """Triage pending builds into schedulable, and count the running builds per store.

        **The count is of builds, not connections.** `DaemonStore.in_flight`
        answers `pool.active_connections`, which a single `QueryPathInfo`
        raises. Counted as builds, one query fills the single slot of `-j1`
        (nixidae/nixidae#235), and each client query of the local store costs
        it a build's worth of score, so builds go remote (nixidae/nixidae#1125).
        """
        schedulable: list[QueuedBuild] = []

        assigned_count: dict[StoreId, int] = {s.store_id: 0 for s in self.stores.values()}
        for build in pending:
            if build.is_building and build.assigned_store_id:
                assigned_count[build.assigned_store_id] += 1

        for build in pending:
            if build.is_building:
                continue

            # All builds are schedulable; missing inputs are
            # discovered and pulled during execution.
            schedulable.append(build)

        return schedulable, assigned_count

    async def _assign_to_stores(
        self,
        schedulable: list[QueuedBuild],
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
            # One build that raises fails alone. The pass ran on, and the next
            # pass met the same build first, so no build anywhere started.
            try:
                if await self._assign_one(build, running_builds, assigned_this_pass):
                    waiting_slot.append(build)
            except Exception as e:
                log.exception("build_assignment_crashed", build_id=build.build_id)
                reason = error_text(e)
                await build.post_log_and_fanout(LogNext(text=f"pynixd: internal error, failing build: {reason}\n"))
                await self.queue.fail(build.build_id, f"pynixd internal error: {reason}")

        return waiting_slot

    async def _assign_one(
        self,
        build: QueuedBuild,
        running_builds: dict[StoreId, int],
        assigned_this_pass: dict[StoreId, int],
    ) -> bool:
        """Start *build* on a store, or fail it; answer whether it waits for a slot."""
        if build.is_building:
            return False
        notes = _Notes()
        try:
            return await self._place(build, running_builds, assigned_this_pass, notes)
        finally:
            if notes.outcome is not None:
                self._trace_decision(build, notes, running_builds, assigned_this_pass)

    async def _place(
        self,
        build: QueuedBuild,
        running_builds: dict[StoreId, int],
        assigned_this_pass: dict[StoreId, int],
        notes: _Notes,
    ) -> bool:
        build_features = effective_required_features(build.request.derivation)

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
            notes.end(Outcome.CANCELLED, "no live request wants it")
            await self.queue.cancel_unwanted(build.build_id)
            return False

        transfer = await self._transfer_seconds(build)
        ranked = self.allocator.rank_stores(
            build,
            assigned_this_pass,
            running_builds=running_builds,
            transfer_seconds=transfer,
            estimates=build.estimates,
            expected_build_seconds=expected_build_seconds(build, self.local_store.store_id),
        )
        notes.excluded = list(ranked.excluded) if isinstance(ranked, RankedStores) else []

        local_store = self.local_store
        local_can_build = (
            local_store.is_healthy
            and not local_store.draining
            and not build.is_blacklisted(local_store.store_id)
            and local_store.supports_derivation(build.request.derivation.platform, build_features)
        )
        no_local_jobs = self.ranker.settings.no_local_jobs
        if no_local_jobs is not NoLocalJobs.one_slot and self._client_forbids_local(build):
            has_remote = self._has_compatible_store(build, build_features)
            if not has_remote and no_local_jobs is NoLocalJobs.remote_only:
                notes.end(Outcome.FAILED, "the client forbids local builds, and no remote store can build it")
                await self._fail_with_no_local_jobs(build)
                return False
            if has_remote:
                local_can_build = False
                ranked = RankedStores([c for c in ranked if c.store_id != local_store.store_id])
                notes.excluded.append(ExcludedStore(local_store.store_id, "the client forbids local builds"))
        prefer_local = build.request.derivation.build_local and local_can_build
        notes.prefer_local = prefer_local
        notes.local_can_build = local_can_build
        # Nix weighs no load for a `preferLocalBuild` derivation, so the
        # local store stays a candidate under `min_schedule_score`.
        # Dropped, the build waits for a slot no running build frees:
        # PSI at 71 % on dynhetz hangs every `writeText` that way.
        if prefer_local and not any(c.store_id == local_store.store_id for c in ranked):
            ranked = RankedStores([*ranked, RankedStore(local_store.store_id, 0.0, local_store)])
            notes.kept_local = True
        # A store this build failed on goes after every store it has not
        # tried, so a retry does not land on the same broken machine.
        # nixidae/nixidae#1015.
        if build.store_failures:
            ranked = RankedStores(
                [c for c in ranked if c.store_id not in build.store_failures]
                + [c for c in ranked if c.store_id in build.store_failures]
            )
        # The local store is ranked like the others (nixidae/nixidae#1001). An
        # empty ranking with a local store that can build means the ranking
        # dropped it, for pressure or for disk, and it waits: full is full.
        notes.ranked = list(ranked)
        if not ranked and not local_can_build and not self._has_compatible_store(build, build_features):
            notes.end(Outcome.FAILED, "no store can build it")
            await self._fail_no_compatible_store(build, build_features)
            return False

        if ranked:
            roomy = [c for c in ranked if self._has_build_room(c.store, running_builds, assigned_this_pass)]
            notes.roomy = {c.store_id for c in roomy}
            if not roomy:
                notes.end(Outcome.WAITING, "every candidate is at its build limit")
                self.held_by_pressure.add(build.build_id)
                return True
            notes.local_slot_full = self._local_slot_is_full(build, running_builds, assigned_this_pass)
            rs = pick_store(
                roomy,
                local_store.store_id,
                local_slot_full=notes.local_slot_full,
                prefer_local=PreferLocal(
                    wait_seconds=self._local_wait_seconds(),
                    # Only a store whose input cost is known: a round trip
                    # alone is no price for a delegation (`_missing_input_bytes`).
                    transfer_seconds={
                        store_id: seconds for store_id, seconds in transfer.items() if store_id in build.inputs_measured
                    },
                    weight=self.ranker.settings.prefer_local_weight,
                )
                if prefer_local
                else None,
            )
            if rs is None:
                notes.end(
                    Outcome.WAITING,
                    "preferLocalBuild: the local slot is expected sooner than a delegation pays"
                    if prefer_local
                    else "the local slot is full, and no other candidate has room",
                )
                return True
            share = self.ranker.settings.explore_share
            explored = (
                explore_store(
                    [c for c in roomy if c.store_id not in build.store_failures],
                    rs,
                    local_store.store_id,
                    local_slot_full=notes.local_slot_full,
                    share=share,
                    draw=self.rng.random(),
                    pick=self.rng.random(),
                )
                if share > 0 and not prefer_local
                else None
            )
            if explored is not None:
                rs = explored
            notes.chosen = rs.store_id
            notes.end(Outcome.ASSIGNED, "exploration" if explored is not None else None)
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
                notes.end(Outcome.FAILED, "every capable store failed it too often")
                await self._fail_all_compatible_blacklisted(build, build_features, all_compatible)
                return False
            notes.end(Outcome.WAITING, "no store is a candidate now")
            return True
        return False

    def _trace_decision(
        self,
        build: QueuedBuild,
        notes: _Notes,
        running_builds: Mapping[StoreId, int],
        assigned_this_pass: Mapping[StoreId, int],
    ) -> None:
        """Hand the decision to the trace, with each store it weighed or left out."""
        if not self.trace.enabled or notes.outcome is None:
            return
        identity = identity_of(build.request.derivation)
        local_id = self.local_store.store_id
        decision = PlacementDecision(
            pass_id=self.pass_id,
            build_id=str(build.build_id),
            attempt=build.retries,
            drv_path=str(build.request.drv_path),
            name=identity.name,
            system=identity.system,
            shape=identity.shape,
            queued_ms=_ms_since(build.enqueued_at),
            expected_build_ms=build.expected_duration,
            prefer_local=notes.prefer_local,
            local_can_build=notes.local_can_build,
            local_slot_full=notes.local_slot_full,
            local_wait_seconds=self._slot_wait_seconds(local_id),
            weights=self._weights,
            outcome=notes.outcome,
            reason=notes.reason,
            chosen_store_id=str(notes.chosen) if notes.chosen is not None else None,
        )
        if not self.trace.due(build.last_traced, decision):
            return
        candidates: dict[StoreId, CandidateTrace] = {}
        for rank, candidate in enumerate(notes.ranked):
            roomy = candidate.store_id in notes.roomy if notes.roomy is not None else None
            # Read before the assignment counted: the chosen store had room.
            has_room = (
                roomy
                if roomy is not None
                else self._has_build_room(candidate.store, running_builds, assigned_this_pass)
            )
            candidates[candidate.store_id] = CandidateTrace(
                store_id=str(candidate.store_id),
                rank=rank,
                reason="preferLocalBuild keeps the local store a candidate"
                if notes.kept_local and candidate.store_id == local_id
                else None,
                chosen=candidate.store_id == notes.chosen,
                roomy=roomy,
                failed_here=build.store_failures.get(candidate.store_id, 0),
                breakdown=candidate.breakdown,
                estimate=build.estimates.get(candidate.store_id),
                slot_wait_seconds=0.0 if has_room else self._slot_wait_seconds(candidate.store_id),
            )
        for excluded in notes.excluded:
            candidates.setdefault(
                excluded.store_id,
                CandidateTrace(
                    store_id=str(excluded.store_id),
                    reason=excluded.reason,
                    failed_here=build.store_failures.get(excluded.store_id, 0),
                    breakdown=excluded.breakdown,
                    estimate=build.estimates.get(excluded.store_id),
                ),
            )
        decision.candidates = list(candidates.values())
        build.last_traced = (decision.key, time.monotonic())
        if decision.outcome is Outcome.ASSIGNED:
            build.placement = decision
        self.trace.record(decision)

    async def _transfer_seconds(self, build: QueuedBuild) -> dict[StoreId, float]:
        """Each remote store's expected delegation seconds for this build, where measured.

        Read once per build and store, and kept on the build: a pass ranks
        every pending build again. The input bytes are what the store lacks
        now (`_missing_input_bytes`); the pull bytes are the identity's
        history, since no output exists before the build.
        """
        if not isinstance(self.local_store, LocalDBStore):
            return {}
        db = self.local_store.db
        identity = identity_of(build.request.derivation)
        platform = build.request.derivation.platform
        features = effective_required_features(build.request.derivation)
        local_id = self.local_store.store_id
        todo = [
            store
            for store_id, store in self.stores.items()
            if store_id != local_id
            and store_id not in build.transfer_seconds
            and store.is_healthy
            and store.supports_derivation(platform, features)
        ]
        estimate_local = self._wants_estimates and local_id not in build.estimates
        if not todo and not estimate_local:
            return self._known_transfer_seconds(build)
        sizes = await self._input_closure_sizes(build) if todo else {}

        async def build_ms(store_id: StoreId) -> tuple[int | None, BuildEstimateSource | None]:
            # Only the trace and a time-aware ranking read a build estimate per
            # store; a pass pays no query for it otherwise.
            if not self._wants_estimates:
                return None, None
            return await db.expected_build_ms_on(identity, str(store_id))

        async def measure(store: DaemonStore) -> None:
            expected = await db.expected_bytes(identity, str(store.store_id))
            link = await self._link(store.store_id)
            missing = await self._missing_input_bytes(store, sizes)
            setup = self.ranker.settings.remote_setup_seconds
            build.transfer_seconds[store.store_id] = transfer_seconds(
                ExpectedBytes(inputs=missing, pull=expected.pull), link, setup
            )
            if missing is not None and (missing == 0 or link.send_bytes_per_s):
                build.inputs_measured.add(store.store_id)
            ms, source = await build_ms(store.store_id)
            build.estimates[store.store_id] = StoreEstimate(
                inputs_bytes=missing,
                pull_bytes=expected.pull,
                link=link,
                build_ms=ms,
                build_source=source,
                setup_seconds=setup,
            )

        async def measure_local() -> None:
            ms, source = await build_ms(local_id)
            build.estimates[local_id] = StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=ms, build_source=source)

        async with anyio.create_task_group() as tg:
            for store in todo:
                tg.start_soon(measure, store)
            if estimate_local:
                tg.start_soon(measure_local)
        return self._known_transfer_seconds(build)

    @property
    def _wants_estimates(self) -> bool:
        ranking = self.ranker.settings
        return (
            self.trace.enabled
            or ranking.time_penalty_per_second is not None
            or ranking.cpu_reference_seconds is not None
        )

    @staticmethod
    def _known_transfer_seconds(build: QueuedBuild) -> dict[StoreId, float]:
        return {store_id: seconds for store_id, seconds in build.transfer_seconds.items() if seconds is not None}

    async def _input_closure_sizes(self, build: QueuedBuild) -> dict[StorePath, int]:
        """The NAR size of each path in the closure of the build's inputs."""
        inputs = {StorePath(path=str(path)) for path in build.request.derivation.input_srcs}
        if not inputs:
            return {}
        closure = await self.local_store.execute(QueryClosureWithInfoRequest(paths=inputs), client=None)
        return {info.path: info.info.nar_size for info in closure.infos}

    async def _missing_input_bytes(self, store: DaemonStore, sizes: Mapping[StorePath, int]) -> int | None:
        """The NAR bytes of the input closure that *store* lacks: what delegating there sends.

        The same `QueryValidPaths` the transfer itself asks
        (`stream_paths_store_to_store`). History was a guess: home-manager's
        `man-paths` had never run on sg1-n3, a round trip alone priced it
        under a second, and it sent 9.3 GB in 648 s for a 0.3 s build.
        `None` when the store does not answer in time; the cost is then unknown.
        """
        if not sizes:
            return 0
        try:
            with anyio.fail_after(_MISSING_BYTES_TIMEOUT):
                valid = await store.execute(QueryValidPathsRequest(paths=set(sizes), substitute=0))
        except (TimeoutError, BackendError, InfrastructureError, OSError, EOFError, *ssh_errors()):
            log.debug("missing_input_bytes_unknown", store_id=store.store_id, exc_info=True)
            return None
        held = set(valid.paths)
        return sum(size for path, size in sizes.items() if path not in held)

    def _local_wait_seconds(self) -> float | None:
        return self._slot_wait_seconds(self.local_store.store_id)

    def _slot_wait_seconds(self, store_id: StoreId) -> float | None:
        running = [
            (build.expected_duration, build.started_at)
            for build in self.queue.by_id.values()
            if build.is_building and build.assigned_store_id == store_id
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

    def _client_forbids_local(self, build: QueuedBuild) -> bool:
        """Whether every client that wants *build* sent `--max-jobs 0`.

        A build that several requests want may run locally when any of them
        allows it, as `_local_slot_is_full` counts.
        """
        wanters = self.queue.live_wanters(build)
        if wanters:
            return all(self.queue.max_jobs_of(request_id) == 0 for request_id in wanters)
        return build.options is not None and int(build.options.max_build_jobs) == 0

    async def _fail_with_no_local_jobs(self, build: QueuedBuild) -> None:
        """Nix 2.35's refusal of a `-j0` build that no builder takes (`derivation-building-goal.cc:300`)."""
        error_msg = (
            f"Cannot build '{build.request.drv_path}'.\n"
            "Reason: local builds are disabled (max-jobs = 0)\n"
            "Hint: set 'max-jobs' to a non-zero value to enable local builds, "
            "or configure remote builders via 'builders'"
        )
        await self.queue.fail(build.build_id, error_msg, BuildResultStatus.INPUT_REJECTED)

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
        """Permanently fail a build with no compatible store, in Nix 2.35's words.

        `reject` at `derivation-building-goal.cc:285` answers InputRejected
        with "platform mismatch" or "missing system features". pynixd's
        "current system" is every store it has, so it names what they offer.
        """
        platform = build.request.derivation.platform
        offered: dict[str, set[str]] = {}
        for store in self.stores.values():
            for system, features in (store._feature_matrix or {}).items():
                offered.setdefault(system, set()).update(features)
        error_msg = f"Cannot build '{build.request.drv_path}'."
        if platform not in offered:
            error_msg += (
                f"\nReason: platform mismatch\nRequired system: '{platform}'"
                f"\nCurrent system: '{', '.join(sorted(offered)) or 'none'}'"
            )
        else:
            error_msg += (
                f"\nReason: missing system features\nRequired features: {{{', '.join(sorted(build_features or ()))}}}"
                f"\nAvailable features: {{{', '.join(sorted(offered[platform]))}}}"
            )
        await self.queue.fail(build.build_id, error_msg, BuildResultStatus.INPUT_REJECTED)
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
            async with contextlib.AsyncExitStack() as held:
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
                        await self._hold_outputs(held, build, store, build_resp)
                    elif (fault := self._retryable_elsewhere(build, store, build_resp)) is not None:
                        # The builder's machine failed, not the derivation: try
                        # another store, the local one included, as a crashed
                        # connection already does.
                        # nixidae/nixidae#1015.
                        log.warning(
                            "build_failed_on_the_machine", build_id=build.build_id, store_id=store.store_id, fault=fault
                        )
                        self._note_disk_full(store, str(build_resp.result.error_msg))
                        await build.post_log_and_fanout(
                            LogNext(text=f"pynixd: build failed on {store.store_id} ({fault}), retrying elsewhere\n")
                        )
                        build.reset_for_retry(store.store_id, str(build_resp.result.error_msg) or fault)
                        build_resp = None
                        self.trigger()
                # The build connection goes back before the pull, so the slot
                # takes the next build while this one pulls. A pull took 92 s
                # against a build of 40 s for t3code, with the slot idle for
                # all of it. nixidae/nixidae#1075.
                if build_resp is not None and result_succeeded(build_resp.result):
                    # The outputs first, and the completion after. It ran the
                    # other way round and logged a collection failure on the
                    # server, so a client held a successful BuildResult and
                    # then asked the local store for a path that is not there.
                    # The error it got named the missing path, never the copy
                    # that failed. Issue #12.
                    try:
                        await self._collect_outputs(build, store, build_resp)
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
            # A dropped connection is the builder's fault as well: a keepalive
            # that closes a silent peer raises here. nixidae/nixidae#1063.
            reason = error_text(e)
            retryable = (BackendError, InfrastructureError, EOFError, OSError, *ssh_errors())
            if all(isinstance(leaf, retryable) for leaf in leaves(e)):
                log.warning("build_failed_retryable", build_id=build.build_id, error=reason)
                self._note_disk_full(store, reason)
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

    def _note_disk_full(self, store: DaemonStore, message: str) -> None:
        """Hold *store* back from builds when it ran out of disk. nixidae/nixidae#1013."""
        if "No space left on device" in message:
            store.disk_full_until = time.monotonic() + self.ranker.settings.disk_full_hold_seconds
            log.warning("store_held_back_for_disk", store_id=store.store_id)

    def _retryable_elsewhere(self, build: QueuedBuild, store: DaemonStore, resp: BuildDerivationResponse) -> str | None:
        """The machine fault behind a failed result, when another store has not tried yet.

        The local store is a store like the others here: a remote's fault
        may fall back to it, and its own fault may move to a remote. It stays
        out when the client forbids local builds (`--max-jobs 0`).
        """
        fault = machine_fault(resp.result)
        if fault is None:
            return None
        platform = build.request.derivation.platform
        features = effective_required_features(build.request.derivation)
        local_forbidden = self._client_forbids_local(build)
        untried = [
            other
            for other in self.stores.values()
            if other.store_id != store.store_id
            and other.store_id not in build.store_failures
            and not (other.store_id == self.local_store.store_id and local_forbidden)
            and other.supports_derivation(platform, features)
        ]
        return fault if untried else None

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

        # The builder substitutes first, and pynixd pushes only what no
        # substituter of the builder holds: a builder next to a cache pulls
        # from it, not over the link from this host. A failed substitution is
        # only a warning on the builder (`store-api.cc:758`), and the push
        # covers it. nixidae/nixidae#1074.
        input_srcs = build.request.derivation.input_srcs
        substitute = True
        if input_srcs:
            paths_to_check = {StorePath(path=str(p)) for p in input_srcs}
            await self._hold_inputs_on(conn, paths_to_check)
            try:
                check = await store.execute(
                    QueryValidPathsRequest(
                        paths=paths_to_check,
                        substitute=1,
                    )
                )
                valid_paths = [str(cp) for cp in check.paths]
                known = [str(p) for p in input_srcs if str(p) in valid_paths]
            except (BackendError, OSError, ConnectionError) as e:
                # A builder that fell silent while it substituted is asked
                # again by the push, without the download.
                log.warning("builder_substitution_failed", store_id=store.store_id, error=error_text(e))
                known = []
                substitute = False
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
            substitute=substitute,
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
        await add_temp_roots(conn, inputs | {info.path for info in closure.infos})

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

        resp: BuildDerivationResponse = await conn.call(
            build.request,
            client=cast("ClientConn", _OutputClock(build)),
            options=build.options,
            keep_logs=True,
        )
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

    async def _hold_outputs(
        self, held: contextlib.AsyncExitStack, build: QueuedBuild, store: DaemonStore, resp: BuildDerivationResponse
    ) -> None:
        """Root the outputs on the builder for the pull, past the build connection.

        The daemon worker of the build connection holds a root on each output,
        and it ends with that connection. A transfer connection taken here, in
        *held*, holds the roots instead. It is taken inside the build
        connection, so it is re-entrant, and every acquire of the pull under
        it skips the pool limit as before: a pull holds the builder and waits
        for local, a push the reverse, and full pools would deadlock the two.
        """
        conn = await held.enter_async_context(store.transfer_conn())
        await add_temp_roots(conn, output_paths(build, resp))

    async def _collect_outputs(
        self,
        build: QueuedBuild,
        store: DaemonStore,
        resp: BuildDerivationResponse,
    ) -> None:
        """Pull outputs back to local store, register realisations, record stats.

        Runs after a successful build execution.
        """
        if not result_succeeded(resp.result):
            return

        # `realised_outputs` reads whichever of the two wire shapes the answer
        # carried. Issue #14.
        realised = resp.result.realised_outputs()
        if realised:
            build.ca_realisations = list(realised.values())
        all_output_paths = output_paths(build, resp)
        log.info(
            "pulling_paths",
            store_id=store.store_id,
            count=len(all_output_paths),
        )

        # The record after the pull, so it holds the pull's cost, and in
        # `finally`, so a failed pull keeps the rest. Issue #12.
        pull_started = time.monotonic()
        try:
            pulled = await self._pull_with_retries(build, store, all_output_paths)
            if build.observation is not None:
                build.observation.pull_ms = _ms_since(pull_started)
                build.observation.pull_bytes = pulled
        finally:
            await self._record_observation(build, resp)
        # After the pull, and not before it: `_ensure_opaque` answers success
        # for a path named here, and a failed pull left nothing local to serve.
        for path in all_output_paths:
            self.ctx.output_locations[str(path)] = store.store_id

    async def _pull_with_retries(self, build: QueuedBuild, store: DaemonStore, paths: StorePathSet) -> int:
        """The pull, tried again when it fails: the build succeeded, and one dropped stream is no reason to fail it.

        A 1.6 GB output once failed with "0 bytes read", and the same pull
        worked minutes later. nixidae/nixidae#942. A pull again skips what
        already arrived: it asks the local store which paths it holds.

        Each attempt asks for the store by id. That "0 bytes read" was sg1-n1's
        pynixd restarting: a reverse builder comes back as a new store object
        under the same id, 3.25 s later, and the old object has no connection
        left. The backoff (2 s, then 4 s) outlasts that.
        """
        attempt = 1
        while True:
            try:
                return await self._pull_outputs(store, paths)
            except Exception as e:
                if attempt >= _PULL_ATTEMPTS:
                    raise
                log.warning(
                    "output_pull_retrying",
                    build_id=build.build_id,
                    store_id=store.store_id,
                    attempt=attempt,
                    error=error_text(e),
                )
                await anyio.sleep(_PULL_BACKOFF_SECONDS * 2 ** (attempt - 1))
                attempt += 1
                current = self.ctx.stores.get(store.store_id)
                if isinstance(current, DaemonStore):
                    store = current

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
        observation_id = await self.local_store.db.record_build_observation(identity, observation)
        if build.placement is not None and observation_id is not None:
            self.trace.observed(build.placement, observation_id)
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
