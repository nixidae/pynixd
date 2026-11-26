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
from typing import TYPE_CHECKING

import anyio
import structlog

from pynixd_store.build_stats import (
    identity_of,
)

from . import metrics, state
from .allocator import (
    BuildAllocator,
    TelemetryStoreRanker,
)
from .build_queue import BuildQueue, QueuedBuild
from .command_options import PynixdOption
from .exceptions import error_text
from .execution import Executor
from .placement import (
    Placer,
    TransferEstimator,
    request_has_a_local_slot,
)
from .placement_trace import PlacementTrace
from .serde import (
    BuildDerivationResponse,
)
from .store import LocalDBStore, ScheduledStore
from .store.local_db import write_router
from .store.transfer import stream_paths_store_to_store
from .substitution_queue import SubstitutionQueue

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable, Sequence

    from anyio.abc import TaskGroup

    from nix_daemon_protocol.ids import BuildId, RequestId, StoreId

    from .answer import Answer
    from .connection import ClientConn
    from .context import PynixdContext
    from .derived_path import DerivedPath
    from .psi import SystemHealth
    from .serde import (
        BuildDerivationRequest,
        SetOptionsRequest,
    )
    from .store.local_daemon import LocalStore

log = structlog.get_logger(__name__)


def nest(build: QueuedBuild, builds: Iterable[QueuedBuild], askers: Sequence[ClientConn]) -> None:
    """Mark *build* nested in each running build whose build pod session among *askers* asked for it."""
    served = {asker.serves_build for asker in askers if asker.serves_build is not None}
    if not served:
        return
    for outer in builds:
        if outer.is_building and str(outer.request.drv_path) in served and outer is not build:
            build.nested_in.add(outer.build_id)
            build.depth = max(build.depth, outer.depth + 1)


def waits_on_nested(builds: Iterable[QueuedBuild]) -> set[BuildId]:
    """The running builds that wait on a nested build not yet done: their slot is free (nixidae/nixidae#1576).

    The outer build's pod still runs, so the cluster, not the slot count, limits how many pods run.
    """
    return {outer for build in builds if not build.is_done for outer in build.nested_in}


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
        self.local_builder = self.local_store.daemon_client
        """The daemon client that builds for the local store; `local_store` answers its reads."""
        self._dynamic_feature_matrix: dict[str, set[str]] = {}

        self.ranker = TelemetryStoreRanker(ctx.settings.ranking)
        self.allocator = BuildAllocator(self.stores, self.local_builder, self.ranker)
        self.trigger_event = anyio.Event()
        self.running = False
        self.stores_opened: anyio.Event | None = None
        """Set once the server's stores have opened; the loop places no build before, or every early one goes local."""
        self._workers: TaskGroup | None = None
        self.builds_waiting = False
        """Whether the last pass left a build waiting for a store."""
        self.pass_listeners: list[Callable[[], None]] = []
        """Called after each pass: a provisioner reads what it left waiting."""
        self.launchable: Callable[[QueuedBuild], bool] = lambda _build: False
        """Whether a provisioning pool may launch a machine for a build; a provisioner sets it."""
        self.single_use: Callable[[ScheduledStore], bool] = lambda _store: False
        """Whether a store takes one build and then no more: a single-use pool's builder; a provisioner sets it."""
        self.trace = PlacementTrace(None, ctx.settings.placement_trace)
        """Writes nothing until `start` hands it the database, which the local store opens after this."""
        self.estimator = TransferEstimator(
            self.local_store, lambda: self.stores, ctx.stores, self.ranker.settings, self.trace
        )
        self.placer: Placer[ScheduledStore] = Placer(
            queue=self.queue,
            local=self.local_builder,
            stores=lambda: self.stores,
            ranking=self.allocator,
            settings=self.ranker.settings,
            trace=self.trace,
            estimator=self.estimator,
            rng=random.Random(),  # noqa: S311 -- placement, not cryptography
            dynamic=self._dynamic_feature_matrix,
            launchable=lambda build: self.launchable(build),
            spawn=self._spawn_worker,
        )
        local = self.local_store
        self.executor: Executor[ScheduledStore, LocalStore | LocalDBStore, LocalDBStore] = Executor(
            queue=self.queue,
            local=local,
            local_builder=self.local_builder,
            here=local if isinstance(local, LocalDBStore) else None,
            sandbox_at=self._sandbox_at,
            current=self._current,
            stores=lambda: self.stores,
            settings=self.ranker.settings,
            trigger=self.trigger,
            strip_handled_features=self.allocator.strip_handled_features,
            stream=stream_paths_store_to_store,  # type: ignore[arg-type]  # zuban-bug: a union type argument fails a callable Protocol; mypy accepts the call
            marks=lambda: local.db,
            add_build_log=lambda name, text: write_router(local).add_build_log(name, text),
            output_locations=ctx.output_locations,
            trace=self.trace,
            builder_may_come=self.placer.builder_may_come,
        )
        for client in [*self.stores.values(), self.local_builder]:
            client.on_capacity = self.trigger
            client.sample_listeners.add(self.on_sample)

    def _sandbox_at(self, store_id: StoreId) -> LocalDBStore | None:
        """A local store other than this one that pynixd's builder builds into, as it runs no daemon."""
        store = self.ctx.stores.get(store_id)
        return store if isinstance(store, LocalDBStore) and store is not self.local_store else None

    def _current(self, store_id: StoreId) -> ScheduledStore | None:
        """The store under *store_id* now: a reverse builder that reconnects is a new object under the same id."""
        store = self.ctx.stores.get(store_id)
        return None if store is None else store.daemon_client

    @property
    def held_by_pressure(self) -> set[BuildId]:
        """Builds the last pass left pending because each candidate store was at its build limit."""
        return self.placer.held_by_pressure

    @property
    def no_candidate(self) -> set[BuildId]:
        """Builds the last pass left pending because no store could take them."""
        return self.placer.no_candidate

    @property
    def stores(self) -> dict[StoreId, ScheduledStore]:
        return {
            store_id: client
            for store_id, store in self.ctx.stores.items()
            if (client := store.daemon_client) is not None and not client.no_schedule
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

    def on_store_added(self, store: ScheduledStore, dynamic: bool = False) -> None:
        """Hook called by Server when a new store is added.

        If dynamic=True, the store's feature_matrix is also registered in
        the dynamic_feature_matrix so that builds for this platform continue
        to queue even after the store is removed.
        """
        log.info("store_added_to_scheduler", store_id=store.store_id, dynamic=dynamic)
        if not store.no_schedule:
            self.allocator.add_store(store)
            store.on_capacity = self.trigger
            store.sample_listeners.add(self.on_sample)
        if dynamic and store.feature_matrix:
            self.add_dynamic_features(store.feature_matrix)
        self.trigger()

    async def drain_store(self, store_id: StoreId, drain_timeout: float = 300.0) -> None:
        """Gracefully drain and stop using a store for new builds."""
        held = self.ctx.stores.get(store_id)
        store = None if held is None else held.daemon_client
        if store is None:
            return

        log.info(
            "draining_store_in_scheduler",
            store_id=store_id,
            drain_timeout=drain_timeout,
        )
        store.draining = True
        self.trigger()

        with anyio.move_on_after(drain_timeout) as waited:
            await store.until_idle()
        if waited.cancelled_caught:
            log.warning("drain_timeout_reached", store_id=store_id, in_flight=store.in_flight)

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
        askers: Sequence[ClientConn] = (),
    ) -> tuple[BuildId, Answer[BuildDerivationResponse]]:
        """Add a build to the queue and trigger the scheduler. *askers* go to `BuildQueue.enqueue`."""
        t0 = time.monotonic()
        hint = None
        memory = None
        if isinstance(self.local_store, LocalDBStore):
            identity = identity_of(request.derivation)
            hint = await self.local_store.db.expected_build_ms(identity)
            memory = await self.local_store.db.memory_model(identity)
        t_hint = time.monotonic()

        builders = None
        pools: frozenset[str] = frozenset()
        value = options.overrides.get("builders") if options is not None else None
        if value is not None and not value.strip():
            # Nix's word for "no remote builders": the build stays on the local store. An untrusted client may say it
            # too (`daemon.cc:297`).
            builders = frozenset()
        elif value and self.ctx.session_builders is not None:
            target = await self.ctx.session_builders.stores_for(value)
            builders, pools = (target.stores, target.pools) if target is not None else (frozenset(), frozenset())
        named = next((c.pynixd_options[PynixdOption.POOL] for c in askers if PynixdOption.POOL in c.pynixd_options), "")
        if named and self.ctx.session_builders is not None:
            pools |= self.ctx.session_builders.named_pools(name for name in named.split(",") if name)
            if builders is None:
                # Not `None`: the build runs on its pools and the local store only.
                builders = frozenset()

        res = await self.queue.enqueue(
            request,
            expected_duration=hint,
            scheduler_request_id=scheduler_request_id,
            derived_paths_for_request=derived_paths_for_request,
            from_goal_path=from_goal_path,
            goal_request_id=goal_request_id,
            options=options,
            delegated=delegated,
            askers=askers,
            builders=builders,
            pools=pools,
        )
        queued = self.queue.by_id.get(res[0])
        if queued is not None:
            nest(queued, self.queue.by_id.values(), askers)
        if queued is not None and queued.memory_model is None:
            queued.memory_model = memory
        if queued is not None and isinstance(self.local_store, LocalDBStore):
            queued.external_systems |= await self.local_store.external_builder_systems(options)
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
        """The task group every build runs in, and the queue's log senders. Leaving it cancels them all."""
        async with self.queue.sending(), anyio.create_task_group() as tg:
            self._workers = tg
            try:
                yield
            finally:
                self._workers = None
                tg.cancel_scope.cancel()

    def _spawn_worker(self, build: QueuedBuild, store: ScheduledStore) -> None:
        workers = self._workers
        if workers is None:
            raise RuntimeError("builds run only inside Scheduler.workers")
        if self.single_use(store):
            # Here, with no await since the placer chose it: the next placement in this pass skips the store.
            store.draining = True
        scope = anyio.CancelScope()
        build.worker = scope
        workers.start_soon(self._run_worker, build, store, scope)

    async def _run_worker(self, build: QueuedBuild, store: ScheduledStore, scope: anyio.CancelScope) -> None:
        # One crashed build must not cancel its siblings in the group.
        try:
            # A cancel from before the start lands at the first await only,
            # after `Executor.run` has already marked paths and named a store.
            if scope.cancel_called:
                return
            with scope:
                await self.executor.run(build, store)
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
        self.trace.db = self.local_store.db
        if self.stores_opened is not None:
            await self.stores_opened.wait()
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
        self.placer.pass_id += 1
        pending = await self.queue.get_pending()
        if not pending:
            self.builds_waiting = False
            self.placer.no_candidate = set()
            self.placer.held_by_pressure = set()
            self._update_store_metrics()
            self._passed()
            return

        schedulable, running_builds = self._filter_schedulable(pending)

        waiting_slot = await self.placer.assign(schedulable, running_builds)
        self.builds_waiting = bool(waiting_slot)

        self._update_store_metrics()
        self._passed()

        log.debug(
            "scheduling_pass_done",
            pending=len(pending),
            waiting_slot=len(waiting_slot),
            in_flight={s.store_id: s.in_flight for s in self.stores.values()},
            cpu_util={
                s.store_id: f"{s.cpu_util.utilization:.1f}%" if s.cpu_util else None for s in self.stores.values()
            },
        )

    def _passed(self) -> None:
        for listener in self.pass_listeners:
            listener()

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
        waiting = waits_on_nested(pending)
        for build in pending:
            if build.is_building and build.assigned_store_id and build.build_id not in waiting:
                assigned_count[build.assigned_store_id] += 1

        for build in pending:
            if build.is_building:
                continue

            # All builds are schedulable; missing inputs are
            # discovered and pulled during execution.
            schedulable.append(build)

        return schedulable, assigned_count

    def request_has_a_local_slot(self, request_id: RequestId) -> bool:
        """Whether *request_id* runs fewer local builds than its `max-jobs`."""
        return request_has_a_local_slot(self.queue, self.local_store.store_id, request_id)

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
