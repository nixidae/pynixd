"""Where a build may go, from the build queue alone: the local slots of each client.

`Scheduler` places builds with these. Each one reads a `BuildQueue` and the
local store's id, so a test passes a real queue and no fake.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import structlog

from pynixd_store.build_stats import slot_wait_seconds

if TYPE_CHECKING:
    from collections.abc import Mapping

    from nix_daemon_protocol.ids import RequestId, StoreId

    from .build_queue import BuildQueue, QueuedBuild

log = structlog.get_logger(__name__)


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
