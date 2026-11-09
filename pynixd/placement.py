"""What placing a build reads: the local slots of each client, and what a delegation costs.

The slot rules read a `BuildQueue` and the local store's id alone, so a test
passes a real queue and no fake. `TransferEstimator` measures the cost.
"""

from __future__ import annotations

import time
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
from pynixd_store.store_path import StorePath

from ._lazy import ssh_errors
from .daemon_extensions.query_closure_with_info import QueryClosureWithInfoRequest
from .exceptions import BackendError, InfrastructureError
from .store import LocalDBStore
from .system_features import effective_required_features

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from nix_daemon_protocol.ids import RequestId, StoreId
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
