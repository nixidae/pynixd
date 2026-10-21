"""A client's `--max-jobs 0` keeps its builds off the local store, as in Nix.

`Worker::waitForBuildSlot` at `worker.cc:262` grants no local slot under 0,
and `worker.cc:351` fails when no builder can take the build. pynixd read 0
as one slot, and with the local store ranked beside the remotes, a `-j0`
build could run locally while remotes stood free. `NoLocalJobs` keeps the
two other readings as opt-in settings. nixidae/nixidae#1003.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId, StoreId
from pynixd.allocator import RankedStore
from pynixd.build_queue import BuildQueue
from pynixd.config import NoLocalJobs, StoreRankingSettings
from pynixd.scheduler import Scheduler
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildMode,
    SetOptionsRequest,
    StorePath,
    TimeSpan,
    Verbosity,
)

if TYPE_CHECKING:
    from pynixd.build_queue import QueuedBuild

REMOTE = StoreId("sg1-n1")


class FakeStore:
    def __init__(self, store_id: StoreId) -> None:
        self.store_id = store_id
        self.is_healthy = True
        self.draining = False

    def supports_derivation(self, platform: str, features: set[str]) -> bool:
        del platform, features
        return True


class NoLocalJobsScheduler:
    """Enough of `Scheduler` for `_assign_to_stores`. The ranker puts the
    local store first, so only the rule under test keeps a build off it."""

    _assign_one = Scheduler._assign_one  # noqa: SLF001 -- the assignment is the unit
    _client_forbids_local = Scheduler._client_forbids_local  # noqa: SLF001 -- the rule under test
    _fail_with_no_local_jobs = Scheduler._fail_with_no_local_jobs  # noqa: SLF001 -- Nix's message

    def __init__(self, queue: BuildQueue, mode: NoLocalJobs, *, remote: bool, configured: bool) -> None:
        self.queue = queue
        self.local_store = FakeStore(LOCAL_STORE_ID)
        ranked = [RankedStore(LOCAL_STORE_ID, 0.0, cast("Any", self.local_store))]
        if remote:
            ranked.append(RankedStore(REMOTE, 0.0, cast("Any", FakeStore(REMOTE))))
        self.allocator = SimpleNamespace(rank_stores=lambda *_a, **_k: list(ranked))
        self.ranker = SimpleNamespace(settings=StoreRankingSettings(no_local_jobs=mode))
        self.remote = remote
        self.stores: dict[StoreId, object] = {LOCAL_STORE_ID: self.local_store}
        if configured:
            self.stores[REMOTE] = FakeStore(REMOTE)
        self.assigned: list[StoreId] = []

    def _has_compatible_store(self, *_args: Any) -> bool:
        return self.remote

    async def _transfer_seconds(self, *_args: Any) -> dict[StoreId, float]:
        return {}

    def _local_slot_is_full(self, *_args: Any) -> bool:
        return False

    def _has_build_room(self, *_args: Any) -> bool:
        return True

    def _local_wait_seconds(self) -> float | None:
        return None

    def _spawn_worker(self, build: QueuedBuild, store: Any) -> None:
        build.worker = anyio.CancelScope()
        self.assigned.append(store.store_id)


def _options(max_build_jobs: int) -> SetOptionsRequest:
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=False,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=max_build_jobs,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=0,
        use_substitutes=True,
        overrides={},
    )


async def _assign(
    mode: NoLocalJobs, *, jobs: int = 0, remote: bool = True, configured: bool = True
) -> tuple[NoLocalJobsScheduler, str]:
    queue = BuildQueue()
    build_id, future = await queue.enqueue(
        BuildDerivationRequest(
            drv_path=StorePath(path="/nix/store/00000000000000000000000000000001-trivial.drv"),
            derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"),
            build_mode=BuildMode.NORMAL,
        ),
        from_goal_path=True,
        goal_request_id=RequestId(1),
        options=_options(jobs),
    )
    scheduler = NoLocalJobsScheduler(queue, mode, remote=remote, configured=configured)
    await Scheduler._assign_to_stores(cast("Any", scheduler), [queue.by_id[build_id]], {}, {})  # noqa: SLF001 -- the assignment is the unit
    error = str(future.result().result.error_msg) if future.done() else ""
    return scheduler, error


class TestRemoteOnly:
    """The default, and Nix's behaviour."""

    @pytest.mark.anyio
    async def test_sends_the_build_to_a_remote_although_local_ranks_first(self):
        scheduler, _ = await _assign(NoLocalJobs.remote_only)

        assert scheduler.assigned == [REMOTE]

    @pytest.mark.anyio
    async def test_fails_with_nix_message_when_no_builder_exists(self):
        scheduler, error = await _assign(NoLocalJobs.remote_only, remote=False, configured=False)

        assert scheduler.assigned == []
        assert "either increase '--max-jobs' or enable remote builds" in error

    @pytest.mark.anyio
    async def test_fails_with_nix_message_when_no_builder_fits(self):
        _, error = await _assign(NoLocalJobs.remote_only, remote=False, configured=True)

        assert "remote machines may not have all required system features" in error

    def test_is_the_default(self):
        assert StoreRankingSettings().no_local_jobs is NoLocalJobs.remote_only


class TestTheOtherReadings:
    @pytest.mark.anyio
    async def test_remote_first_builds_locally_when_no_remote_can(self):
        scheduler, _ = await _assign(NoLocalJobs.remote_first, remote=False, configured=False)

        assert scheduler.assigned == [LOCAL_STORE_ID]

    @pytest.mark.anyio
    async def test_remote_first_still_prefers_a_remote(self):
        scheduler, _ = await _assign(NoLocalJobs.remote_first)

        assert scheduler.assigned == [REMOTE]

    @pytest.mark.anyio
    async def test_one_slot_reads_zero_as_one(self):
        scheduler, _ = await _assign(NoLocalJobs.one_slot)

        assert scheduler.assigned == [LOCAL_STORE_ID]


@pytest.mark.anyio
async def test_a_client_with_a_local_slot_is_untouched():
    scheduler, _ = await _assign(NoLocalJobs.remote_only, jobs=1)

    assert scheduler.assigned == [LOCAL_STORE_ID]
