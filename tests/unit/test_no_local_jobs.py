"""A client's `--max-jobs 0` keeps its builds off the local store, as in Nix.

`Worker::waitForBuildSlot` at `worker.cc:262` grants no local slot under 0,
and `worker.cc:351` fails when no builder can take the build. pynixd read 0
as one slot, and with the local store ranked beside the remotes, a `-j0`
build could run locally while remotes stood free. `NoLocalJobs` keeps the
two other readings as opt-in settings. nixidae/nixidae#1003.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, RequestId, StoreId
from pynixd.build_queue import BuildQueue
from pynixd.config import NoLocalJobs, StoreRankingSettings
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode, StorePath
from tests._conftest.placement import FakeStore, FixedRanking, Spawned, fake_placer, options

REMOTE = StoreId("sg1-n1")


async def _assign(
    mode: NoLocalJobs, *, jobs: int = 0, remote: bool = True, configured: bool = True
) -> tuple[Spawned, str]:
    """The ranker puts the local store first, so only the rule under test keeps a build off it.

    *configured* puts a remote among the stores; *remote* lets it build the derivation and rank.
    """
    queue = BuildQueue()
    build_id, future = await queue.enqueue(
        BuildDerivationRequest(
            drv_path=StorePath(path="/nix/store/00000000000000000000000000000001-trivial.drv"),
            derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"),
            build_mode=BuildMode.NORMAL,
        ),
        from_goal_path=True,
        goal_request_id=RequestId(1),
        options=options(jobs),
    )
    local = FakeStore(LOCAL_STORE_ID)
    builder = FakeStore(REMOTE, systems=None if remote else set())
    stores = {LOCAL_STORE_ID: local, REMOTE: builder} if configured else {LOCAL_STORE_ID: local}
    ranking = FixedRanking.of(local, builder) if remote else FixedRanking.of(local)
    spawned = Spawned()
    placer = fake_placer(
        queue, ranking, spawned, local=local, stores=stores, settings=StoreRankingSettings(no_local_jobs=mode)
    )
    await placer.assign([queue.by_id[build_id]], {})
    error = str(future.result().result.error_msg) if future.done() else ""
    return spawned, error


class TestRemoteOnly:
    """The default, and Nix's behaviour."""

    @pytest.mark.anyio
    async def test_sends_the_build_to_a_remote_although_local_ranks_first(self) -> None:
        spawned, _ = await _assign(NoLocalJobs.remote_only)

        assert spawned.stores == [REMOTE]

    @pytest.mark.anyio
    async def test_fails_with_nix_message_when_no_builder_exists(self) -> None:
        """Nix 2.35 answers InputRejected with this (`derivation-building-goal.cc:300`)."""
        spawned, error = await _assign(NoLocalJobs.remote_only, remote=False, configured=False)

        assert spawned.stores == []
        assert "Reason: local builds are disabled (max-jobs = 0)" in error

    @pytest.mark.anyio
    async def test_fails_with_nix_message_when_no_builder_fits(self) -> None:
        _, error = await _assign(NoLocalJobs.remote_only, remote=False, configured=True)

        assert "Reason: local builds are disabled (max-jobs = 0)" in error

    def test_is_the_default(self) -> None:
        assert StoreRankingSettings().no_local_jobs is NoLocalJobs.remote_only


class TestTheOtherReadings:
    @pytest.mark.anyio
    async def test_remote_first_builds_locally_when_no_remote_can(self) -> None:
        spawned, _ = await _assign(NoLocalJobs.remote_first, remote=False, configured=False)

        assert spawned.stores == [LOCAL_STORE_ID]

    @pytest.mark.anyio
    async def test_remote_first_still_prefers_a_remote(self) -> None:
        spawned, _ = await _assign(NoLocalJobs.remote_first)

        assert spawned.stores == [REMOTE]

    @pytest.mark.anyio
    async def test_one_slot_reads_zero_as_one(self) -> None:
        spawned, _ = await _assign(NoLocalJobs.one_slot)

        assert spawned.stores == [LOCAL_STORE_ID]


@pytest.mark.anyio
async def test_a_client_with_a_local_slot_is_untouched() -> None:
    spawned, _ = await _assign(NoLocalJobs.remote_only, jobs=1)

    assert spawned.stores == [LOCAL_STORE_ID]
