"""What placing a build reads: the local slots of each client, and what a delegation costs.

The slot rules read a `BuildQueue` and the local store's id alone, so a test
passes a real queue and no fake. `TransferEstimator` measures the cost.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

import anyio
import structlog

from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest
from pynixd_store.build_stats import (
    ExpectedBytes,
    StoreEstimate,
    StoreLink,
    identity_of,
    slot_wait_seconds,
    transfer_seconds,
)
from pynixd_store.hintfmt import ANSI_NORMAL, ANSI_RED, magenta
from pynixd_store.store_path import StorePath

from ._lazy import ssh_errors
from .allocator import (
    Candidate,
    ExcludedStore,
    PreferLocal,
    RankedStore,
    RankedStores,
    explore_store,
    fit_memory,
    outside_the_session,
    pick_store,
    reserved_memory,
)
from .config import NoLocalJobs
from .daemon_extensions.query_closure_with_info import QueryClosureWithInfoRequest
from .exceptions import BackendError, InfrastructureError, error_text
from .placement_trace import CandidateTrace, Outcome, PlacementDecision
from .serde import BuildMode, BuildResultStatus, LogNext
from .store import LocalDBStore
from .system_features import BUILTIN_SYSTEM, effective_required_features

if TYPE_CHECKING:
    import random
    from collections.abc import Callable, Mapping

    from nix_daemon_protocol.ids import BuildId, RequestId, StoreId
    from nix_daemon_protocol.query_valid_paths import QueryValidPathsResponse
    from pynixd_store.build_stats import BuildEstimateSource

    from .build_queue import BuildQueue, QueuedBuild
    from .config import StoreRankingSettings
    from .placement_trace import PlacementTrace
    from .store import DaemonStore
    from .store.base import Store

log = structlog.get_logger(__name__)

_LINK_REREAD_SECONDS = 60.0
"""How long a store's measured link stands before the next read."""
_MISSING_BYTES_TIMEOUT = 5.0
"""Seconds a store has to say which inputs it holds, before its cost counts as unknown."""


def slot_wait(queue: BuildQueue, store_id: StoreId) -> float | None:
    """When the soonest running build on *store_id* is expected to end, or `None` when no estimate says."""
    running = [
        (build.expected_duration, build.started_at)
        for build in queue.by_id.values()
        if build.is_building and build.assigned_store_id == store_id
    ]
    return slot_wait_seconds(running, time.monotonic())


def request_has_a_local_slot(queue: BuildQueue, local_id: StoreId, request_id: RequestId) -> bool:
    """Whether *request_id* runs fewer local builds than its `max-jobs`.

    Counted from the queue on each call, not from the pass: a build that
    this pass assigned already has its task, so it counts as building.
    A walk of the queue per check is cheap at the queue sizes pynixd sees.
    """
    jobs = queue.max_jobs_of(request_id)
    if jobs is None:
        return True
    running = sum(
        1 for b in queue.by_id.values() if b.is_building and b.assigned_store_id == local_id and request_id in b.holders
    )
    if running < max(1, jobs):
        return True
    log.debug("request_waits_for_a_local_slot", request_id=int(request_id), running=running, max_build_jobs=jobs)
    return False


def local_slot_is_full(
    queue: BuildQueue,
    local_id: StoreId,
    build: QueuedBuild,
    running_builds: Mapping[StoreId, int],
    assigned_this_pass: Mapping[StoreId, int],
) -> bool:
    """Answer whether `max-jobs` of the client leaves no local slot.

    This is `Worker::waitForBuildSlot` at `worker.cc:261`, which counts
    `getNrLocalBuilds()` against `settings.maxBuildJobs` and starts no
    further local build until one ends. A build on a backend costs no
    slot, and this function reaches only the local store for that reason.

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
    wanters = queue.live_wanters(build)
    if wanters:
        return not any(request_has_a_local_slot(queue, local_id, request_id) for request_id in wanters)
    options = build.options
    if options is None:
        return False
    slots = max(1, int(options.max_build_jobs))
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


def client_forbids_local(queue: BuildQueue, build: QueuedBuild) -> bool:
    """Whether every client that wants *build* sent `--max-jobs 0`.

    A build that several requests want may run locally when any of them
    allows it, as `local_slot_is_full` counts.
    """
    wanters = queue.live_wanters(build)
    if wanters:
        return all(queue.max_jobs_of(request_id) == 0 for request_id in wanters)
    return build.options is not None and int(build.options.max_build_jobs) == 0


class AnswersValidity(Protocol):
    """A store that says which of some paths it holds."""

    @property
    def store_id(self) -> StoreId: ...
    async def execute(self, request: QueryValidPathsRequest, /) -> QueryValidPathsResponse: ...


async def missing_input_bytes(
    store: AnswersValidity, answers: Mapping[StoreId, AnswersValidity], sizes: Mapping[StorePath, int]
) -> int | None:
    """The NAR bytes of the input closure that *store* lacks: what delegating there sends.

    The same `QueryValidPaths` the transfer itself asks
    (`stream_paths_store_to_store`). History was a guess: home-manager's
    `man-paths` had never run on sg1-n3, a round trip alone priced it
    under a second, and it sent 9.3 GB in 648 s for a 0.3 s build.
    `None` when the store does not answer in time; the cost is then unknown.
    The store in *answers* answers, not its daemon client: a store with no daemon reads its own database.
    """
    if not sizes:
        return 0
    answering = answers.get(store.store_id, store)
    try:
        with anyio.fail_after(_MISSING_BYTES_TIMEOUT):
            valid = await answering.execute(QueryValidPathsRequest(paths=set(sizes), substitute=0))
    except (TimeoutError, BackendError, InfrastructureError, OSError, EOFError, *ssh_errors()):
        log.debug("missing_input_bytes_unknown", store_id=store.store_id, exc_info=True)
        return None
    held = set(valid.paths)
    return sum(size for path, size in sizes.items() if path not in held)


class TransferEstimator:
    """What delegating a build to each store is expected to cost, from the local store's measurements."""

    def __init__(
        self,
        local_store: Store,
        stores: Callable[[], Mapping[StoreId, DaemonStore]],
        answers: Mapping[StoreId, AnswersValidity],
        settings: StoreRankingSettings,
        trace: PlacementTrace,
    ) -> None:
        self.local_store = local_store
        self.stores = stores
        """The stores a build may go to, read on each call: a builder can join or leave between passes."""
        self.answers = answers
        """The store behind each daemon client; a store with no daemon reads its own database."""
        self.settings = settings
        self.trace = trace
        self._links: dict[StoreId, tuple[float, StoreLink]] = {}
        """Each store's measured link, and when it was read."""

    async def transfer_seconds(self, build: QueuedBuild) -> dict[StoreId, float]:
        """Each remote store's expected delegation seconds for this build, where measured.

        Read once per build and store, and kept on the build: a pass ranks
        every pending build again. The input bytes are what the store lacks
        now (`missing_input_bytes`); the pull bytes are the identity's
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
            for store_id, store in self.stores().items()
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
            missing = await missing_input_bytes(store, self.answers, sizes)
            setup = self.settings.remote_setup_seconds
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
        ranking = self.settings
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

    async def _link(self, store_id: StoreId) -> StoreLink:
        if not isinstance(self.local_store, LocalDBStore):
            return StoreLink()
        cached = self._links.get(store_id)
        if cached is not None and time.monotonic() - cached[0] < _LINK_REREAD_SECONDS:
            return cached[1]
        link = await self.local_store.db.store_link(str(store_id))
        self._links[store_id] = (time.monotonic(), link)
        return link


class HasRoom(Protocol):
    def has_room(self, running: int, /) -> bool: ...


class PlacedStore(Candidate, Protocol):
    """What placing a build reads of a store, beyond what the allocator reads."""

    @property
    def build_limit(self) -> HasRoom: ...
    # `dict`, as `DaemonStore` declares it: a store's setter must accept the
    # member's type, so `Mapping` here refuses every `DaemonStore`.
    @property
    def feature_matrix(self) -> dict[str, set[str]]: ...


class Ranks[S: Candidate](Protocol):
    def rank_stores(
        self,
        build: QueuedBuild,
        assigned_this_pass: Mapping[StoreId, int],
        running_builds: Mapping[StoreId, int] | None = None,
        transfer_seconds: Mapping[StoreId, float] | None = None,
        estimates: Mapping[StoreId, StoreEstimate] | None = None,
        expected_build_seconds: float | None = None,
    ) -> RankedStores[S]: ...


class Estimates(Protocol):
    async def transfer_seconds(self, build: QueuedBuild, /) -> dict[StoreId, float]: ...


def has_build_room(
    store: PlacedStore,
    running_builds: Mapping[StoreId, int],
    assigned_this_pass: Mapping[StoreId, int],
) -> bool:
    """Whether the build limit of *store* lets one more build start. Issue #98."""
    running = running_builds.get(store.store_id, 0) + assigned_this_pass.get(store.store_id, 0)
    return store.build_limit.has_room(running)


def ms_since(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def expected_build_seconds(build: QueuedBuild, local_id: StoreId) -> float | None:
    """The build alone: its local estimate, else the shortest any store has."""
    local = build.estimates.get(local_id)
    if local is not None and local.build_seconds is not None:
        return local.build_seconds
    known = [estimate.build_seconds for estimate in build.estimates.values() if estimate.build_seconds is not None]
    return min(known) if known else None


@dataclass
class _Notes[S: Candidate]:
    """What one decision of `_assign_one` saw, for the trace."""

    outcome: Outcome | None = None
    reason: str | None = None
    ranked: list[RankedStore[S]] = field(default_factory=list)
    excluded: list[ExcludedStore] = field(default_factory=list)
    roomy: set[StoreId] | None = None
    short_of_memory: dict[StoreId, str] = field(default_factory=dict)
    chosen: StoreId | None = None
    prefer_local: bool = False
    kept_local: bool = False
    local_can_build: bool = False
    local_slot_full: bool | None = None

    def end(self, outcome: Outcome, reason: str | None = None) -> None:
        self.outcome = outcome
        self.reason = reason


class Placer[S: PlacedStore]:
    """Places each schedulable build on a store, or fails it; `Scheduler.schedule` runs one pass through `assign`."""

    def __init__(
        self,
        *,
        queue: BuildQueue,
        local: S,
        stores: Callable[[], Mapping[StoreId, S]],
        ranking: Ranks[S],
        settings: StoreRankingSettings,
        trace: PlacementTrace,
        estimator: Estimates,
        rng: random.Random,
        dynamic: Mapping[str, set[str]],
        spawn: Callable[[QueuedBuild, S], None],
    ) -> None:
        self.queue = queue
        self.local = local
        """The local store's builder."""
        self.stores = stores
        """Read on each call: a builder can join or leave between passes."""
        self.ranking = ranking
        self.settings = settings
        self.trace = trace
        self.estimator = estimator
        self.rng = rng
        """Draws for `explore_share`; a test passes a seeded one."""
        self.dynamic = dynamic
        """The platforms a builder may still register for: the scheduler's mapping, which `add_dynamic_feature` writes."""
        self.spawn = spawn
        """Starts a placed build on its store."""
        self.held_by_pressure: set[BuildId] = set()
        """Builds the last pass left pending because each candidate store
        was at its build limit."""
        self.pass_id = 0
        self._weights = settings.model_dump_json()

    async def assign(
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
        reserved = reserved_memory(self.queue.by_id.values())
        self.held_by_pressure = set()

        for build in schedulable:
            # One build that raises fails alone. The pass ran on, and the next
            # pass met the same build first, so no build anywhere started.
            try:
                if await self._assign_one(build, running_builds, assigned_this_pass, reserved):
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
        reserved: dict[StoreId, int],
    ) -> bool:
        """Start *build* on a store, or fail it; answer whether it waits for a slot."""
        if build.is_building:
            return False
        notes: _Notes[S] = _Notes()
        try:
            return await self._place(build, running_builds, assigned_this_pass, reserved, notes)
        finally:
            if notes.outcome is not None:
                self._trace_decision(build, notes, running_builds, assigned_this_pass)

    async def _place(
        self,
        build: QueuedBuild,
        running_builds: dict[StoreId, int],
        assigned_this_pass: dict[StoreId, int],
        reserved: dict[StoreId, int],
        notes: _Notes[S],
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

        transfer = await self.estimator.transfer_seconds(build)
        ranked = self.ranking.rank_stores(
            build,
            assigned_this_pass,
            running_builds=running_builds,
            transfer_seconds=transfer,
            estimates=build.estimates,
            expected_build_seconds=expected_build_seconds(build, self.local.store_id),
        )
        notes.excluded = list(ranked.excluded)

        local_store = self.local
        local_can_build = (
            local_store.is_healthy
            and not local_store.draining
            and not build.is_blacklisted(local_store.store_id)
            and local_store.supports_derivation(build.request.derivation.platform, build_features)
        )
        no_local_jobs = self.settings.no_local_jobs
        if no_local_jobs is not NoLocalJobs.one_slot and client_forbids_local(self.queue, build):
            has_remote = self._has_compatible_store(build, build_features)
            if not has_remote and no_local_jobs is NoLocalJobs.remote_only:
                notes.end(Outcome.FAILED, "the client forbids local builds, and no remote store can build it")
                await self._fail_with_no_local_jobs(build)
                return False
            if has_remote:
                local_can_build = False
                ranked = RankedStores([c for c in ranked if c.store_id != local_store.store_id])
                notes.excluded.append(ExcludedStore(local_store.store_id, "the client forbids local builds"))
        # `derivation-building-goal.cc:599`: a check acts on this store, so it builds here or nowhere.
        here_only = build.request.build_mode != BuildMode.NORMAL
        if here_only:
            ranked = RankedStores([c for c in ranked if c.store_id == local_store.store_id])
        prefer_local = (build.request.derivation.build_local or here_only) and local_can_build
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
        if not ranked and not local_can_build and (here_only or not self._has_compatible_store(build, build_features)):
            notes.end(Outcome.FAILED, "no store can build it")
            await self._fail_no_compatible_store(build, build_features)
            return False

        if ranked:
            roomy = [c for c in ranked if has_build_room(c.store, running_builds, assigned_this_pass)]
            notes.roomy = {c.store_id for c in roomy}
            if not roomy:
                notes.end(Outcome.WAITING, "every candidate is at its build limit")
                self.held_by_pressure.add(build.build_id)
                return True
            fit = fit_memory(roomy, build.expected_peak_bytes, reserved)
            roomy = fit.stores
            notes.short_of_memory = fit.short
            notes.local_slot_full = local_slot_is_full(
                self.queue, self.local.store_id, build, running_builds, assigned_this_pass
            )
            rs = pick_store(
                roomy,
                local_store.store_id,
                local_slot_full=notes.local_slot_full,
                prefer_local=PreferLocal(
                    wait_seconds=slot_wait(self.queue, self.local.store_id),
                    # Only a store whose input cost is known: a round trip
                    # alone is no price for a delegation (`missing_input_bytes`).
                    transfer_seconds={
                        store_id: seconds for store_id, seconds in transfer.items() if store_id in build.inputs_measured
                    },
                    weight=self.settings.prefer_local_weight,
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
            share = self.settings.explore_share
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
            notes.end(Outcome.ASSIGNED, "exploration" if explored is not None else fit.reason)
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
            self.spawn(build, rs.store)
            assigned_this_pass[rs.store_id] = assigned_this_pass.get(rs.store_id, 0) + 1
            if build.expected_peak_bytes:
                reserved[rs.store_id] = reserved.get(rs.store_id, 0) + build.expected_peak_bytes
        else:
            # All compatible stores are busy, or this build can't be placed
            all_compatible = [
                s
                for s in self._stores_of(build)
                if s.supports_derivation(build.request.derivation.platform, build_features)
            ]
            # A builder of a dynamic platform can still register, and its
            # registration runs a pass (`on_store_added`) that places the build.
            if (
                all_compatible
                and all(build.is_blacklisted(s.store_id) for s in all_compatible)
                and not self._dynamic_supports_build(build, build_features)
            ):
                notes.end(Outcome.FAILED, "every capable store failed it too often")
                await self._fail_all_compatible_blacklisted(build, build_features, all_compatible)
                return False
            notes.end(Outcome.WAITING, "no store is a candidate now")
            return True
        return False

    def _trace_decision(
        self,
        build: QueuedBuild,
        notes: _Notes[S],
        running_builds: Mapping[StoreId, int],
        assigned_this_pass: Mapping[StoreId, int],
    ) -> None:
        """Hand the decision to the trace, with each store it weighed or left out."""
        if not self.trace.enabled or notes.outcome is None:
            return
        identity = identity_of(build.request.derivation)
        local_id = self.local.store_id
        decision = PlacementDecision(
            pass_id=self.pass_id,
            build_id=str(build.build_id),
            attempt=build.retries,
            drv_path=str(build.request.drv_path),
            name=identity.name,
            system=identity.system,
            shape=identity.shape,
            queued_ms=ms_since(build.enqueued_at),
            expected_build_ms=build.expected_duration,
            prefer_local=notes.prefer_local,
            local_can_build=notes.local_can_build,
            local_slot_full=notes.local_slot_full,
            local_wait_seconds=slot_wait(self.queue, local_id),
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
                roomy if roomy is not None else has_build_room(candidate.store, running_builds, assigned_this_pass)
            )
            candidates[candidate.store_id] = CandidateTrace(
                store_id=str(candidate.store_id),
                rank=rank,
                reason="preferLocalBuild keeps the local store a candidate"
                if notes.kept_local and candidate.store_id == local_id
                else notes.short_of_memory.get(candidate.store_id),
                chosen=candidate.store_id == notes.chosen,
                roomy=roomy,
                failed_here=build.store_failures.get(candidate.store_id, 0),
                breakdown=candidate.breakdown,
                estimate=build.estimates.get(candidate.store_id),
                slot_wait_seconds=0.0 if has_room else slot_wait(self.queue, candidate.store_id),
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

    async def _fail_with_no_local_jobs(self, build: QueuedBuild) -> None:
        """Nix 2.35's refusal of a `-j0` build that no builder takes (`derivation-building-goal.cc:300`)."""
        error_msg = (
            f"Cannot build '{build.request.drv_path}'.\n"
            "Reason: local builds are disabled (max-jobs = 0)\n"
            "Hint: set 'max-jobs' to a non-zero value to enable local builds, "
            "or configure remote builders via 'builders'"
        )
        await self.queue.fail(build.build_id, error_msg, BuildResultStatus.INPUT_REJECTED)

    def _stores_of(self, build: QueuedBuild) -> list[S]:
        """The stores *build* may use: the local store, and the remote ones `outside_the_session` keeps."""
        local_id = self.local.store_id
        return [s for s in self.stores().values() if s.store_id == local_id or outside_the_session(s, build) is None]

    def _dynamic_supports(self, system: str, features: set[str] | None = None) -> bool:
        """Check if the dynamic_feature_matrix declares support for a system+features combo."""
        sys_features = self.dynamic.get(system)
        if sys_features is None and system == BUILTIN_SYSTEM and self.dynamic:
            sys_features = set().union(*self.dynamic.values())
        if sys_features is None:
            return False
        if not features:
            return True
        return features.issubset(sys_features)

    def _dynamic_supports_build(self, build: QueuedBuild, build_features: set[str]) -> bool:
        """`_dynamic_supports`, for a build that may use pynixd's own stores; a client's builders register none."""
        return build.builders is None and self._dynamic_supports(build.request.derivation.platform, build_features)

    def _has_compatible_store(
        self,
        build: QueuedBuild,
        build_features: set[str],
    ) -> bool:
        """Check if any live or dynamic store could ever support this build."""
        return any(
            s.store_id != self.local.store_id
            and s.supports_derivation(build.request.derivation.platform, build_features)
            for s in self._stores_of(build)
        ) or self._dynamic_supports_build(build, build_features)

    async def _fail_no_compatible_store(
        self,
        build: QueuedBuild,
        build_features: set[str],
    ) -> None:
        """Permanently fail a build with no compatible store, as `reject` (`derivation-building-goal.cc:289`) does.

        Each reason that applies adds its block, in Nix's order and colours.

        NIX-DEVIATION (#27): `reject` names `thisSystem` and the local `system-features`. pynixd builds on a
        pool, so "Current system" lists every system of every store. "Available features" names what the
        stores of the derivation's system offer, or every store's features when no store has that system.
        One machine's values would hide the stores that could take the build with a change. Reverse it if a
        client parses these lines as one machine's. nixidae/nixidae#1216.
        """
        platform = build.request.derivation.platform
        offered: dict[str, set[str]] = {}
        for store in self._stores_of(build):
            for system, features in store.feature_matrix.items():
                offered.setdefault(system, set()).update(features)
        required = build_features or set()
        available = offered.get(platform) or set().union(*offered.values())
        reasons = ""
        if platform not in offered:
            reasons += (
                f"\nReason: {ANSI_RED}platform mismatch{ANSI_NORMAL}\nRequired system: '{magenta(platform)}'"
                f"\nCurrent system: '{magenta(', '.join(sorted(offered)) or 'none')}'"
            )
        if not reasons or not required <= available:
            reasons += (
                f"\nReason: {ANSI_RED}missing system features{ANSI_NORMAL}"
                f"\nRequired features: {{{', '.join(sorted(required))}}}"
                f"\nAvailable features: {{{', '.join(sorted(available))}}}"
            )
        error_msg = f"Cannot build '{magenta(build.request.drv_path)}'.{reasons}"
        await self.queue.fail(build.build_id, error_msg, BuildResultStatus.INPUT_REJECTED)
        for line in error_msg.split("\n"):
            await build.post_log_and_fanout(LogNext(text=f"pynixd: {line}\n"))

    async def _fail_all_compatible_blacklisted(
        self,
        build: QueuedBuild,
        build_features: set[str],
        compatible: list[S],
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
