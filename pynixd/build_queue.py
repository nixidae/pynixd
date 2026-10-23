"""Queued build tracking for the scheduler."""

from __future__ import annotations

import asyncio
import itertools
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID, BuildId, RequestId, StoreId
from nix_daemon_protocol.logs import LogStartActivity, LogStopActivity
from nix_daemon_protocol.protocol import ActivityType, Verbosity

from . import metrics, wire
from .exceptions import DelegationLoopError
from .serde import BuildDerivationResponse, BuildMode, BuildResult, BuildResultStatus
from .serde.context import WriteContext
from .state_models import QueueCounts
from .store_path import StorePath

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from nix_daemon_protocol.logs import LogMessage

    from .build_stats import BuildObservation
    from .connection import ClientConn
    from .derived_path import DerivedPath
    from .serde import BuildDerivationRequest, Realisation, SetOptionsRequest
log = structlog.get_logger(__name__)

MAX_STORE_RETRIES = 3

_act_counter = itertools.count()


def _next_activity_id() -> int:
    """An activity id that cannot collide with the ones a backend sends.

    pynixd forwards a backend daemon's log messages with their activity ids
    unchanged, so an id pynixd makes for itself has to miss that space.
    `src/libutil/logging.cc:208` is where Nix answers the same question:

        id(nextId++ + (((uint64_t) getPid()) << 32))

    Same scheme here. Two daemons on one machine have different pids, so the
    high half separates them. Two on different machines can share a pid, and
    that is a collision Nix has as well with a remote builder -- copying the
    scheme copies its limit, and inventing a different one would not remove
    it.
    """
    return next(_act_counter) + (os.getpid() << 32)


@dataclass
class SchedulerBuildRequest:
    """Tracks the full lifecycle of a build_derived_paths() call.

    Individual QueuedBuilds come and go (completing, spawning trampoline
    derivations, etc.), but this request future stays until all paths
    the client asked for are satisfied.
    """

    id: RequestId
    derived_paths: set[DerivedPath]
    build_mode: BuildMode
    client: ClientConn | None
    future: asyncio.Future[dict[DerivedPath, BuildResult]]
    active_build_ids: set[BuildId] = field(default_factory=set)
    all_build_ids: set[BuildId] = field(default_factory=set)
    build_to_derived: dict[BuildId, set[DerivedPath]] = field(default_factory=dict)
    results: dict[DerivedPath, BuildResult] = field(default_factory=dict)

    def add_build(self, build_id: BuildId, derived_paths: set[DerivedPath]) -> None:
        """Track a new build as part of this request."""
        self.active_build_ids.add(build_id)
        self.all_build_ids.add(build_id)
        self.build_to_derived[build_id] = derived_paths

    def build_completed(self, build_id: BuildId) -> bool:
        """Remove build from active set. Returns True if request is complete."""
        self.active_build_ids.discard(build_id)
        return not self.active_build_ids

    def resolve_if_done(self) -> bool:
        """Resolve the future if all active builds are done."""
        if not self.active_build_ids and not self.future.done():
            self.future.set_result(dict(self.results))
            return True
        return False


class QueuedBuild:
    """State for a single derivation build task.

    Lifecycle:
    - is_pending: queued but not assigned
    - is_building: task spawned and running
    - is_done: future resolved
    """

    def __init__(
        self,
        build_id: BuildId,
        request: BuildDerivationRequest,
        future: asyncio.Future[BuildDerivationResponse],
        expected_duration: int | None = None,
        scheduler_request_ids: set[RequestId] | None = None,
        options: SetOptionsRequest | None = None,
    ) -> None:
        """Track a single derivation build through its lifecycle.

        Args:
            build_id: Unique build identifier.
            request: The build request (drv, build mode, etc.).
            future: Resolved when the build completes or fails.
            expected_duration: Hint from the scheduler for store assignment.
            scheduler_request_ids: Request IDs this build belongs to (dedup).
            options: The option set of the client that asked for the build.
        """
        self.build_id = build_id
        self.request = request
        self.options = options
        """The option set that the connection of this build must carry.

        A build runs after the request of the client returned, so the client
        is not on the stack any more. The queue keeps the set instead.

        **A build is shared between the clients that ask for it, and each one
        has its own options.** The first client to ask decides, as the first
        of two Nix clients holds the output lock and builds with its own set.
        A later holder enforces its own timeouts on its wait:
        `BuildDerivationGoal._wait_within_my_limits`. Issue nixidae/nixidae#231.
        """
        self.future = future
        self.expected_duration = expected_duration
        self.enqueued_at = time.monotonic()
        self.started_at: float | None = None
        self.observation: BuildObservation | None = None
        """What this build cost, by phase, filled while `execute_build` runs."""
        self.transfer_seconds: dict[StoreId, float | None] = {}
        """Each remote store's expected delegation seconds, `None` where unmeasured."""
        self.inputs_measured: set[StoreId] = set()
        """The remote stores this build's identity has sent inputs to before."""
        self.last_output_at: float | None = None
        """When the upstream daemon last sent this build anything. A holder
        that is not the owner reads it for its own `max-silent-time`."""
        self.finished_at: float | None = None
        self.retries = 0
        self.store_failures: dict[StoreId, int] = {}
        self.store_errors: dict[StoreId, str] = {}
        """The last failure of each store, which the final error names."""
        self.worker: anyio.CancelScope | None = None
        """The scope of the worker that runs this build, from assignment until it ends.

        `Scheduler._assign_to_stores` makes it before the worker starts, so a
        pass that runs first counts the build as building. A cancel before the
        worker enters the scope cancels it on entry."""

        self.from_goal_path: bool = False

        # CA realisations from this build's outputs, populated after successful
        # build completion. Used to register realisations on builder stores
        # before building dependent (deferred) derivations.
        self.ca_realisations: list[Realisation] = []

        # The store that was assigned to execute this build, set when
        # execute_build begins.
        self.assigned_store_id: StoreId | None = None

        # BuildRequest(s) this build belongs to.
        # Multiple requests can share a single build (dedup).
        # Empty set for standalone build_derivation() calls.
        self.scheduler_request_ids: set[RequestId] = scheduler_request_ids or set()

        # Append-only byte buffer of all stderr messages (serialized via
        # NixWriter interface). New subscribers get this replayed on join.
        self._log_writer = wire.BytesWriter("build_log")

        # Client connections subscribed to this build's stderr stream.
        self.subscribers: list[ClientConn] = []
        self._subscriber_refs: dict[ClientConn, int] = {}
        self.cancel_when_unsubscribed = False
        self._waiting_acts: dict[ClientConn, int] = {}
        """The BUILD_WAITING activity open for each client that joined a build
        somebody else asked for. Per client, because the client that asked for
        the build reads no such activity. Issue #25."""

        # The goal requests that hold this build. `BuildQueue.release_request`
        # ends the build when the last one lets go. A request holds once,
        # whatever number of its goals reach the derivation. Issue #91.
        self.holders: set[RequestId] = set()
        self.standalone = 0
        """Requests outside the goal system that want this build: op 36, or pynixd itself.

        No goal request lets go for them, so a build one of them wants is
        never cancelled for want of holders. In Nix two daemons never kill
        each other's build (`derivation-building-goal.cc:421`).
        nixidae/nixidae#1012."""

        # Guards add_subscriber replay vs post_log_bytes fanout so that a
        # joining client never misses bytes that arrive during catch-up.
        self._sub_lock = anyio.Lock()

    @property
    def is_building(self) -> bool:
        """Whether a worker runs this build, or is about to."""
        return self.worker is not None

    def cancel_worker(self) -> None:
        if self.worker is not None:
            self.worker.cancel()

    def worker_ended(self, scope: anyio.CancelScope) -> None:
        """The worker of *scope* is gone. A retry may already run a newer one."""
        if self.worker is scope:
            self.worker = None

    @property
    def is_done(self) -> bool:
        """Whether the build future has been resolved (success or failure)."""
        return self.future.done()

    @property
    def is_pending(self) -> bool:
        """Whether the build is queued but not yet assigned or running."""
        return not self.is_building and not self.is_done

    def is_blacklisted(self, store_id: StoreId) -> bool:
        """Check if a specific store has exceeded the retry limit for this build."""
        return self.store_failures.get(store_id, 0) >= MAX_STORE_RETRIES

    def reset_for_retry(self, failed_store_id: StoreId, reason: str) -> None:
        """Reset state for retry on next scheduling pass.

        Called after a build or infrastructure failure. The build will
        be re-scheduled to a different store if available, or the same
        store if it hasn't exceeded the per-backend retry limit.
        """
        self.retries += 1
        self.started_at = None
        self.worker = None

        # Increment failure count for this specific store
        self.store_failures[failed_store_id] = self.store_failures.get(failed_store_id, 0) + 1
        self.store_errors[failed_store_id] = reason

    @property
    def wait_time(self) -> float | None:
        """Seconds between enqueue and build start."""
        if self.started_at is None:
            return None
        return self.started_at - self.enqueued_at

    @property
    def build_time(self) -> float | None:
        """Seconds between build start and finish."""
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at

    @property
    def total_time(self) -> float | None:
        """Total seconds between enqueue and finish."""
        if self.finished_at is None:
            return None
        return self.finished_at - self.enqueued_at

    @property
    def description(self) -> str:
        """A human-readable description of the build."""
        pname = self.request.derivation.env.get("pname", "unknown")
        version = self.request.derivation.env.get("version", "unknown")
        return f"{pname}-{version}"

    # ── Log pub/sub methods ───────────────────────────────────────────

    async def post_log(self, msg: LogMessage) -> bytes:
        """Serialize and store a log entry. Returns the raw bytes."""
        before = self._log_writer.tell()
        await msg.to_writer(WriteContext(writer=self._log_writer, version=wire.PROTOCOL_VERSION))
        return self._log_writer.get_bytes_since(before)

    async def _send_raw_safe(self, sub: ClientConn, raw: bytes) -> ClientConn | None:
        """Send raw bytes to a subscriber, removing it on failure.

        **A write to a transport that is already closed raises
        `RuntimeError`, and not `OSError`.** uvloop says "unable to perform
        operation on <UnixTransport closed=True reading=False ...>; the
        handler is closed". `BrokenPipeError` and `ConnectionResetError` come
        from a peer that goes away during the write, and this is the other
        case: the write starts after the loop dropped the transport.

        A build of pynixd outlives the request that asked for it, so this is
        the ordinary end of a build whose client left. `ca:new-build-cmd`
        measured it: `slow` of the `cancelled-builds` fixture sleeps 10 s, the
        test kills the daemon while it runs, and the exception left
        `Scheduler.execute_build` through its own error path. Nothing
        retrieves the exception of that task, so asyncio reported it as one
        that was never retrieved and no client learned anything. Issue nixidae/nixidae#235.
        """
        try:
            await sub.send_raw(raw)
        except (OSError, RuntimeError):
            return sub
        else:
            return None

    async def post_log_bytes(self, raw: bytes) -> None:
        """Fan out raw log bytes to all subscribers via TaskGroup.

        Dead subscribers (send failure) are removed automatically.
        """
        if not self.subscribers or not raw:
            return
        async with self._sub_lock:
            subscribers = list(self.subscribers)
        if not subscribers:
            return
        failed: list[ClientConn] = []
        async with anyio.create_task_group() as tg:
            for sub in subscribers:
                tg.start_soon(_send_and_record_failed, self, sub, raw, failed)
        for sub in failed:
            await self.remove_subscriber(sub)

    async def post_log_and_fanout(self, msg: LogMessage) -> None:
        """Store a log entry and fan out to all subscribers."""
        raw = await self.post_log(msg)
        await self.post_log_bytes(raw)

    async def add_subscriber(self, client: ClientConn, *, cancel_on_unsubscribe: bool = False) -> None:
        """Register a client to receive this build's logs.

        Replays the full logged history so far, then the client
        receives new entries in real-time via post_log_bytes.
        If replay fails (broken connection), the subscriber is not added.

        **One client subscribes many times to one build, and the replay runs
        once.** A build is shared: a client asks for it as a root goal, and it
        asks for it again as the input derivation of another goal. The count
        in `_subscriber_refs` stopped the fan-out of a new line to that client
        twice, and it did not stop the replay. Each further subscription sent
        the whole log again, so the client printed the error of one build two
        or three times. `build.sh:167` of the functional suite counts the
        `error:` lines. Issue nixidae/nixidae#235.

        NIX-DEVIATION (#27): `DerivationBuildingGoal` at
        `derivation-building-goal.cc:421-426` opens an `actBuildWaiting` and
        polls `outputLocks.lockPaths` until the other builder is done. It
        gives the message and nothing else, because that builder is another
        process and Nix has no channel to it. pynixd dedupes rather than
        locks, so both builds are in one process and one log, and the second
        client reads the first builder's whole output. The difference is worth
        its cost because a user who waits on a build wants to see it, and the
        cost is nothing on the wire that a client does not already handle: it
        is the same `STDERR_NEXT` stream, and the activity below is the same
        type and level Nix sends. To reverse the decision, measure a run where
        the replay confuses the reader about which build wrote a line -- the
        one measured risk, `build.sh:167` counting `error:` twice, is what
        `_subscriber_refs` above already answers.

        **This is not a `NIX-DEFECT (#23)`.** Nix is not wrong here. Its
        process model denies it the other builder's log; it did not decide
        against carrying it.
        """
        async with self._sub_lock:
            if cancel_on_unsubscribe:
                self.cancel_when_unsubscribed = True
            if client in self._subscriber_refs:
                self._subscriber_refs[client] += 1
                return
            if self._waits_for_somebody_elses_build():
                await self._say_it_is_waiting(client)
            if self._log_writer.tell():
                try:
                    # One line for each replay, so a run can count them. A
                    # client that reads one build message twice reads it once
                    # live and once from here.
                    log.debug("build_log_replayed", build_id=self.build_id, bytes=self._log_writer.tell())
                    await client.send_raw(self._log_writer.get_bytes())
                except (OSError, BrokenPipeError, ConnectionResetError):
                    log.debug("subscriber_replay_failed", build_id=self.build_id)
                    return
            self.subscribers.append(client)
            self._subscriber_refs[client] = 1

    def _waits_for_somebody_elses_build(self) -> bool:
        """Whether a client subscribing now joins a build it did not ask for.

        Another subscriber is the common case. `started_at` covers the one
        where the client that asked for the build has gone: the build is still
        running and this client still did not start it.
        """
        return bool(self.subscribers) or self.started_at is not None

    def _what_it_is_waiting_on(self) -> str:
        """The paths Nix would name, in the order Nix names them.

        `derivation-building-goal.cc:409-418` locks an output path where it
        knows one, and `<drv>.<outputName>` where it does not -- a floating
        content-addressed output has no path until it is built. Then
        `:421-426` writes `waiting for lock on` and that list.

        Same rule here, and the paths cost nothing: `request.derivation` is
        already in hand, so this is not the `drv_path`-only message that issue
        #25 expected to be necessary.
        """
        outputs = self.request.derivation.output_paths()
        named = sorted(
            str(path) if path != StorePath("") else f"{self.request.drv_path}.{name}" for name, path in outputs.items()
        )
        # A derivation with no outputs at all reaches no lock file in Nix
        # either, so there is nothing to copy. The drv path is what pynixd
        # deduped on, which is the true answer to "waiting on what".
        return ", ".join(f"'{one}'" for one in named or [str(self.request.drv_path)])

    async def _say_it_is_waiting(self, client: ClientConn) -> None:
        """Open a BUILD_WAITING activity for one client.

        Not through `post_log`: that writes to the shared log, which every
        later subscriber replays. This activity belongs to one client, and the
        client that asked for the build must not read it.
        """
        act_id = _next_activity_id()
        message = LogStartActivity(
            act_id=act_id,
            level=Verbosity.WARN,
            type=ActivityType.BUILD_WAITING,
            text=f"waiting for lock on {self._what_it_is_waiting_on()}",
        )
        if await self._send_one(client, message) is None:
            self._waiting_acts[client] = act_id

    async def stop_waiting_activities(self) -> None:
        """Close every BUILD_WAITING this build opened. Called once it ends."""
        async with self._sub_lock:
            waiting = list(self._waiting_acts.items())
            self._waiting_acts.clear()
        for client, act_id in waiting:
            await self._send_one(client, LogStopActivity(act_id=act_id))

    async def _send_one(self, client: ClientConn, msg: LogMessage) -> ClientConn | None:
        writer = wire.BytesWriter()
        await msg.to_writer(WriteContext(writer=writer, version=wire.PROTOCOL_VERSION))
        return await self._send_raw_safe(client, writer.get_bytes())

    async def remove_subscriber(self, client: ClientConn) -> bool:
        """Remove one subscription reference for a client.

        Returns True when a reference was removed.
        """
        async with self._sub_lock:
            count = self._subscriber_refs.get(client)
            if count is None:
                return False
            if count > 1:
                self._subscriber_refs[client] = count - 1
                return True
            del self._subscriber_refs[client]
            self.subscribers.remove(client)
            return True


async def _send_and_record_failed(
    build: QueuedBuild,
    client: ClientConn,
    raw: bytes,
    failed: list[ClientConn],
) -> None:
    failed_client = await build._send_raw_safe(client, raw)
    if failed_client is not None:
        failed.append(failed_client)


def _the_order_nix_takes(build: QueuedBuild) -> tuple[str, str, int]:
    """The sort key of `get_pending`: the name of the derivation, then the path."""
    path = str(build.request.drv_path)
    name = path.rpartition("/")[2].partition("-")[2]
    return (name, path, int(build.build_id))


def _dedup_key(request: BuildDerivationRequest) -> tuple[str, int]:
    """The derivation and the build mode: a CHECK or REPAIR never joins a NORMAL build.

    A NORMAL build's answer says nothing about determinism, and it repairs
    nothing. Nix shares a goal only inside one worker (`worker.cc:116` keys
    `derivationBuildingGoals` by path), and each client connection has its
    own worker, so one client's check never rides another's build.
    nixidae/nixidae#1012.
    """
    return (str(request.drv_path), int(request.build_mode))


class BuildQueue:
    """Global queue for build operations with deduplication."""

    def __init__(self) -> None:
        """Initialize an empty build queue with deduplication indices."""
        self._queue: list[QueuedBuild] = []
        self._by_path: dict[tuple[str, int], QueuedBuild] = {}
        """Dedup: `_dedup_key` -> the build of that derivation and mode."""
        self._by_id: dict[BuildId, QueuedBuild] = {}  # For DAG lookups
        self._requests: dict[RequestId, SchedulerBuildRequest] = {}
        self.next_id = 1
        self.next_request_id = 1
        # Each goal system that met a failed build and did not set
        # `keep-going`. `complete` and `fail` write it, under this lock and
        # before `Scheduler.trigger`, which is what lets `_assign_to_stores`
        # read a fact rather than win a race. Issue nixidae/nixidae#325.
        self._given_up: set[RequestId] = set()
        # `max-jobs` of each live goal request, `None` for no limit. A build's
        # own `options` are its first client's, and a deduplicated request
        # brings a limit of its own. `Scheduler._local_slot_is_full` reads
        # this. Issue #89.
        self._max_jobs: dict[RequestId, int | None] = {}
        self._keeps_going: dict[RequestId, bool] = {}
        """`keep-going` of each live goal request; a request with no options keeps going."""
        # The builds each goal request holds, so one release finds them all.
        self._held: dict[RequestId, set[BuildId]] = {}
        self.lock: anyio.Lock = anyio.Lock()

    def counts(self) -> QueueCounts:
        """Builds by state, from a walk of the queue.

        The queue gauge and `pynixd state` both read this. Counting by
        increments drifted: a build that failed after its task ended counted
        as pending, and a retry took a second pending decrement, so dynhetz
        read pending -2 and more builds running than connections (#89).
        """
        pending = building = done = 0
        for build in self._queue:
            if build.is_done:
                done += 1
            elif build.is_building:
                building += 1
            elif build.is_pending:
                pending += 1
        return QueueCounts(pending=pending, building=building, done=done)

    def max_jobs_of(self, request_id: RequestId) -> int | None:
        """`max-jobs` of a live goal request, or `None` when it named none."""
        return self._max_jobs.get(request_id)

    def live_wanters(self, build: QueuedBuild) -> set[RequestId]:
        """The goal requests that still want *build*."""
        return build.holders - self._given_up

    @property
    def queue(self) -> Sequence[QueuedBuild]:
        """Read-only view of the current build queue."""
        return self._queue

    @property
    def by_id(self) -> Mapping[BuildId, QueuedBuild]:
        """Read-only mapping of build_id to QueuedBuild."""
        return self._by_id

    @property
    def by_path(self) -> Mapping[tuple[str, int], QueuedBuild]:
        """Read-only mapping of (drv_path, build mode) to QueuedBuild."""
        return self._by_path

    @property
    def requests(self) -> Mapping[RequestId, SchedulerBuildRequest]:
        """Read-only mapping of request_id to SchedulerBuildRequest."""
        return self._requests

    async def create_request(
        self,
        derived_paths: set[DerivedPath],
        build_mode: BuildMode,
        client: ClientConn | None,
    ) -> tuple[RequestId, SchedulerBuildRequest]:
        """Create a SchedulerBuildRequest and return it."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[DerivedPath, BuildResult]] = loop.create_future()
        async with self.lock:
            req_id = RequestId(self.next_request_id)
            req = SchedulerBuildRequest(
                id=req_id,
                derived_paths=derived_paths,
                build_mode=build_mode,
                client=client,
                future=future,
            )
            self.next_request_id += 1
            self._requests[req.id] = req
            return req.id, req

    async def enqueue(
        self,
        request: BuildDerivationRequest,
        expected_duration: int | None = None,
        scheduler_request_id: RequestId | None = None,
        derived_paths_for_request: set[DerivedPath] | None = None,
        from_goal_path: bool = False,
        goal_request_id: RequestId | None = None,
        options: SetOptionsRequest | None = None,
        delegated: bool = False,
    ) -> tuple[BuildId, asyncio.Future[BuildDerivationResponse]]:
        """Add a build to the queue (deduplicates if already present).

        Returns (build_id, future) - caller awaits the future for the response.
        Dedup only applies to builds that are still pending (not building/done).

        If scheduler_request_id is set, the build is tracked as part of that
        SchedulerBuildRequest. derived_paths_for_request maps this build to
        the original DerivedPaths it satisfies.

        Deduplication uses drv_path and the build mode as the identity — a .drv
        file is immutable, so the same pair always means the same build.

        Raises:
            DelegationLoopError: *delegated* is set, and this instance runs the
                same build on a remote store. That store, or one behind it,
                asked for it, and a join would wait on itself.
        """
        if from_goal_path and goal_request_id is None:
            raise RuntimeError("a goal build needs the goal request that holds it")

        async with self.lock:
            existing = self._by_path.get(_dedup_key(request))
            if (
                delegated
                and existing is not None
                and existing.is_building
                and existing.assigned_store_id not in (None, LOCAL_STORE_ID)
            ):
                raise DelegationLoopError(str(request.drv_path), str(existing.assigned_store_id))
            if goal_request_id is not None:
                self._max_jobs[goal_request_id] = None if options is None else int(options.max_build_jobs)
                self._keeps_going[goal_request_id] = options is None or bool(options.keep_going)
            # Check for duplicate — dedup with queued or in-progress builds
            if existing is not None and not existing.is_done:
                log.debug("build_deduped", id=existing.build_id)
                existing.from_goal_path = existing.from_goal_path or from_goal_path
                # Under the same lock as the dedup. A hold taken after
                # `enqueue` returned could find the build cancelled between
                # the two calls, by another request that let go in that moment.
                if from_goal_path and goal_request_id is not None:
                    self._hold_locked(existing, goal_request_id)
                if goal_request_id is None:
                    existing.standalone += 1
                if scheduler_request_id is not None:
                    existing.scheduler_request_ids.add(scheduler_request_id)
                    if derived_paths_for_request:
                        sched_req = self._requests.get(scheduler_request_id)
                        if sched_req is not None:
                            sched_req.add_build(
                                existing.build_id,
                                derived_paths_for_request,
                            )
                return existing.build_id, existing.future

            # Create new build with future
            loop = asyncio.get_running_loop()
            future: asyncio.Future[BuildDerivationResponse] = loop.create_future()
            build_id = BuildId(self.next_id)
            scheduler_request_ids: set[RequestId] = (
                {scheduler_request_id} if scheduler_request_id is not None else set()
            )
            build = QueuedBuild(
                build_id=build_id,
                request=request,
                future=future,
                expected_duration=expected_duration,
                scheduler_request_ids=scheduler_request_ids,
                options=options,
            )
            build.from_goal_path = from_goal_path
            if from_goal_path and goal_request_id is not None:
                self._hold_locked(build, goal_request_id)
            if goal_request_id is None:
                build.standalone += 1
            self.next_id += 1
            self._queue.append(build)
            self._by_path[_dedup_key(request)] = build
            self._by_id[build.build_id] = build

            if scheduler_request_id is not None and derived_paths_for_request:
                sched_req = self._requests.get(scheduler_request_id)
                if sched_req is not None:
                    sched_req.add_build(build.build_id, derived_paths_for_request)

            log.info(
                "build_enqueued",
                build_id=build.build_id,
                description=build.description,
                request_ids=list(scheduler_request_ids),
            )
            return build.build_id, future

    async def subscribe(self, build_id: BuildId, client: ClientConn, *, cancel_on_unsubscribe: bool = False) -> bool:
        """Subscribe a client to a build's log stream.

        Returns True if the build was found and subscriber added.
        """
        async with self.lock:
            build = self._by_id.get(build_id)
            if build is None:
                return False
            await build.add_subscriber(client, cancel_on_unsubscribe=cancel_on_unsubscribe)
            return True

    async def unsubscribe(self, build_id: BuildId, client: ClientConn) -> bool:
        """Remove a client subscription and cancel abandoned client-bound builds."""
        async with self.lock:
            build = self._by_id.get(build_id)
            if build is None:
                return False
            removed = await build.remove_subscriber(client)
            if removed and build.cancel_when_unsubscribed and not build.subscribers and not build.is_done:
                self._cancel_locked(build, "pynixd: build cancelled because all clients disconnected")
            return removed

    def _hold_locked(self, build: QueuedBuild, request_id: RequestId) -> None:
        build.holders.add(request_id)
        self._held.setdefault(request_id, set()).add(build.build_id)

    async def release_request(self, request_id: RequestId) -> None:
        """A goal request lets go of every build it holds, and is forgotten.

        **A build ends when no goal of any request waits for it.** Nix takes
        the same decision one step earlier: `Worker::removeGoal` at
        `worker.cc:173` clears `topGoals` when a top goal fails and
        `keep-going` is off, `Worker::run` leaves its loop, and the goals that
        are left are destroyed. A destroyed `DerivationBuildingGoal` kills its
        builder.

        pynixd cannot copy that step for step, because a build here serves
        every client that asked for the same derivation, and one client that
        gave up must not take the work of the others.
        `_the_result_unless_it_stops` in `goals/requests.py` states that rule.
        The build ends when the **last** request lets go, and not when the
        first one does.

        `main:build` measures both halves. `build.sh:269` builds the
        `cancelled-builds` fixture with `-j2`. Its `slow` derivation writes to
        a fifo, which blocks until a reader opens the fifo, and the only
        reader is `fast-fail`, which fails. The test then removes the
        directory of the fifo, so no reader can ever appear. Nix answers in
        about two seconds and kills that builder. pynixd left it running, a
        second request deduplicated onto it, and the run reached the 300 s
        timeout of the test. Issue nixidae/nixidae#235.

        The give-up mark and `max-jobs` go in the same step, so no scheduling
        pass sees the request forgotten while it still holds a build
        (nixidae/nixidae#325). A second call finds nothing. Issue #91.
        """
        async with self.lock:
            for build_id in self._held.pop(request_id, set()):
                build = self._by_id.get(build_id)
                if build is None:
                    continue
                build.holders.discard(request_id)
                build.scheduler_request_ids.discard(request_id)
                if not build.holders and not build.standalone and not build.is_done:
                    self._cancel_locked(build, "pynixd: build cancelled because no goal waits for it")
            self._given_up.discard(request_id)
            self._max_jobs.pop(request_id, None)
            self._keeps_going.pop(request_id, None)
        await self.prune()

    async def get_pending(self) -> list[QueuedBuild]:
        """Every build that is not done, in the order Nix would take them.

        **Nix takes a derivation by its name, and not by the order it
        arrived.** `Worker::awake` is a `std::set` over `CompareGoalPtrs`,
        which reads `Goal::key()`, and `DerivationBuildingGoal::key()` at
        `derivation-building-goal.cc:54` builds `"dd$" + name + "$" + path`.
        `goal.hh:604` states the rule: `aardvark` runs before `baboon`.

        `build_id` is a counter, so sorting by it gave the order the requests
        arrived. That decides which build runs first whenever `max-jobs` makes
        the slots scarce, and the answer of a request then depends on the
        order a client happened to ask. `_goal_order` in `goals/requests.py`
        takes the same decision one level up. Issue nixidae/nixidae#235.

        The id stays as the last part of the key, so two derivations of one
        name keep a stable order.
        """
        async with self.lock:
            return sorted(
                [b for b in self._queue if not b.is_done],
                key=_the_order_nix_takes,
            )

    async def complete(
        self,
        build_id: BuildId,
        response: BuildDerivationResponse,
    ) -> None:
        """Mark build as completed, resolve the future."""
        await self._stop_waiting(build_id)
        async with self.lock:
            for b in self._queue:
                if b.build_id == build_id:
                    if b.future.done():
                        return
                    b.finished_at = time.monotonic()
                    b.future.set_result(response)
                    self._note_a_request_that_gave_up(b, response.result.status)
                    log.info("build_completed", build_id=build_id)

                    # Every success: a builder that held the outputs answers
                    # `ALREADY_VALID`, and nixidae/nixidae#998 counted it a failure.
                    status = "success" if BuildResultStatus(response.result.status).is_success else "failure"
                    metrics.BUILDS_COMPLETED.labels(status=status).inc()
                    if b.build_time is not None:
                        metrics.BUILD_DURATION.observe(b.build_time)

                    return
        raise ValueError(f"Build {build_id} not found")

    async def _stop_waiting(self, build_id: BuildId) -> None:
        """Close the BUILD_WAITING activities of a build that is ending.

        Before `self.lock` and not under it: it sends to clients, and a client
        that has gone away takes the removal path, which takes `_sub_lock`.
        Holding the queue lock across a write to a socket also holds every
        other build up for as long as that write takes.
        """
        build = self._by_id.get(build_id)
        if build is not None:
            await build.stop_waiting_activities()

    async def fail(
        self, build_id: BuildId, error_msg: str, status: BuildResultStatus = BuildResultStatus.MISC_FAILURE
    ) -> None:
        """Mark build as failed, resolve future with an error response."""
        await self._stop_waiting(build_id)
        async with self.lock:
            for b in self._queue:
                if b.build_id == build_id:
                    if b.future.done():
                        return
                    b.finished_at = time.monotonic()
                    response = BuildDerivationResponse(
                        result=BuildResult(
                            status=status,
                            error_msg=error_msg,
                            times_built=0,
                            is_non_deterministic=0,
                            start_time=0,
                            stop_time=0,
                            built_outputs={},
                        ),
                    )
                    b.future.set_result(response)
                    self._note_a_request_that_gave_up(b, response.result.status)
                    log.info("build_failed", build_id=build_id, error_msg=error_msg)

                    metrics.BUILDS_COMPLETED.labels(status="failure").inc()

                    return
        raise ValueError(f"Build {build_id} not found")

    def _note_a_request_that_gave_up(self, build: QueuedBuild, status: int) -> None:
        """Record each goal system that this failure ends. Call it under the lock.

        **A request stops at its first failed build, unless it set
        `keep-going`.** `Worker::removeGoal` at `worker.cc:173` clears
        `topGoals` then, and `Worker::run` leaves its loop, so Nix starts
        nothing further for that request.

        pynixd cannot copy the mechanism, because its goals are coroutines and
        the request learns of the failure several awaits later. It copies the
        *decision* instead, and it takes it here, where the failure first
        becomes a fact. `Scheduler.execute_build` calls `complete` and then
        `trigger`, so this runs before the scheduling pass that would give the
        freed slot to the next build. Issue nixidae/nixidae#325.

        The option set is the one of the client that made the build, which is
        the same set `_local_slot_is_full` reads. A build with no options is
        one that pynixd made itself, and it ends no request.
        """
        if BuildResultStatus(status).is_success:
            return
        # Each holder by its own option: `keepGoing` belongs to one worker
        # (`worker.cc:173`). The build's own options are the first asker's.
        # nixidae/nixidae#1012.
        self._given_up |= {holder for holder in build.holders if not self._keeps_going.get(holder, True)}

    def nobody_wants(self, build: QueuedBuild) -> bool:
        """Has every goal system that wanted *build* already given up?

        This is the question `Scheduler._assign_to_stores` asks before it hands
        out a build slot. It is a fact and not a race: the failure that ends a
        request is recorded by `complete` under this lock, and the scheduling
        pass runs after `trigger`.

        **A build that a second, live request also wants is still wanted.**
        That is the property `AGENTS.md` states -- a build of pynixd serves
        every client that asked for it -- and the subset test is what keeps
        it.

        A build that no goal system holds answers False, because the set is
        empty and every set contains the empty set. Such a build belongs to
        `build_derived_paths` or to pynixd itself, and no request ends it.
        """
        return bool(build.holders) and build.holders <= self._given_up and not build.standalone

    async def cancel_unwanted(self, build_id: BuildId) -> None:
        """End a build that no live request wants, and answer everyone waiting.

        **Skipping such a build is not enough, and leaving it pending
        deadlocks.** A build of a request that gave up can still have a goal
        of that request awaiting its future: `_realise_input_derivations`
        waits for the input goals of a root, and the root has not answered
        yet, so `release_request` has not run. `main:build` measured it
        -- build 12 of `nix build -f fod-failing.nix -L x4` was skipped, never
        cancelled and never completed, and the whole test hit its 300 s cap.

        Resolving the future is what breaks that: the goal reads a failure,
        the root fails with it, the request answers, and `release_request` then finds
        nothing left to do. Nix reaches the same end differently --
        `Goal::amDone` at `goal.cc:242` drops every waitee that is left when
        one fails and `keep-going` is off, so those derivations are not built
        either. Issue nixidae/nixidae#325.
        """
        async with self.lock:
            build = self._by_id.get(build_id)
            if build is None or build.is_done:
                return
            # **The client hears nothing about this build.** Nix hears nothing
            # either: the waitees that `Goal::amDone` drops are simply not
            # built, and no goal reports them. `build.sh:167` counts the
            # `error:` lines of the run and expects one, and a reason here
            # made it three -- one for x1, which really failed, and one each
            # for x2 and x3, which this cancelled.
            self._cancel_locked(build, "", log_reason="the request that wanted it stopped")

    def _cancel_locked(self, build: QueuedBuild, error_msg: str, *, log_reason: str | None = None) -> None:
        """Cancel a queued build while ``self.lock`` is held.

        *error_msg* reaches the client, through
        `EnsureDerivedPathGoal._tell_the_client_it_failed`, which writes a
        block for a goal that another goal waits for. **An empty one writes
        nothing**, because that method guards on the text being there, and
        *log_reason* then keeps the reason in the log line where a person
        reading a run can still find it.
        """
        build.finished_at = time.monotonic()
        build.cancel_worker()
        response = BuildDerivationResponse(
            result=BuildResult(
                status=BuildResultStatus.MISC_FAILURE,
                error_msg=error_msg,
                times_built=0,
                is_non_deterministic=0,
                start_time=0,
                stop_time=0,
                built_outputs={},
            ),
        )
        if not build.future.done():
            build.future.set_result(response)
        # The reason belongs in the line, because two roads reach this
        # method: a client that disconnected, and a goal system that let go.
        log.info("build_cancelled", build_id=build.build_id, reason=log_reason or error_msg)

        metrics.BUILDS_COMPLETED.labels(status="failure").inc()

    async def prune(self) -> None:
        """Drop finished builds nobody waits for and nobody streams.

        `prune_request` below drops one request's builds and has no caller
        left; standalone `BuildDerivation` carries no request id at all. So
        every completed build stayed queued with its whole log, and the
        growth benchmark watched memory climb ~50 MiB per chatty build.
        This sweeps them instead: done, no requesting ids of either kind,
        no subscribers. A deduped client still subscribed keeps its build;
        its own finish prunes it. Called after a request lets go, never
        mid-build. Also drops the dedup entry, guarded so a newer build
        that reused the derivation path keeps its own.
        """
        async with self.lock:
            doomed = {
                id(build)
                for build in self._queue
                if build.is_done and not build.scheduler_request_ids and not build.holders and not build.subscribers
            }
            if not doomed:
                return
            self._queue[:] = [build for build in self._queue if id(build) not in doomed]
            for build_id, build in list(self._by_id.items()):
                if id(build) in doomed:
                    del self._by_id[build_id]
                    if self._by_path.get(_dedup_key(build.request)) is build:
                        del self._by_path[_dedup_key(build.request)]
        log.debug("builds_pruned", count=len(doomed))

    async def prune_request(self, request_id: RequestId) -> None:
        """Remove completed builds from the queue if no other request references them.

        Called after a SchedulerBuildRequest resolves. Builds that are still
        referenced by other requests are kept; only the request id is removed
        from their set. Delegates the sweep to `prune`, then drops the
        request record itself.
        """
        async with self.lock:
            for build in self._queue:
                build.scheduler_request_ids.discard(request_id)
        await self.prune()
        async with self.lock:
            self._requests.pop(request_id, None)

    def count(self, status: str) -> int:
        """Get count of builds with given status.

        Safe to call from async context (no await points, so no
        interleaving with queue mutations).
        """
        match status:
            case "pending":
                return len([b for b in self._queue if b.is_pending])
            case "running":
                return len([b for b in self._queue if b.is_building])
            case "done":
                return len([b for b in self._queue if b.is_done])
        return 0
