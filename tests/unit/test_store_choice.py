"""Where a build goes: the local store ranks with the others, and delegation has a measured cost.

nixidae/nixidae#1001, and `preferLocalBuild` from nixidae/nixidae#947.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, BuildId, StoreId
from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest, QueryValidPathsResponse
from pynixd.allocator import (
    SHORT_OF_DISK,
    BuildAllocator,
    ExcludedStore,
    PreferLocal,
    RankedStore,
    RankedStores,
    TelemetryStoreRanker,
    explore_store,
    fit_memory,
    pick_store,
    score_store,
)
from pynixd.build_queue import BuildQueue
from pynixd.config import StoreRankingSettings
from pynixd.connection import ClientConn
from pynixd.placement import missing_input_bytes
from pynixd.psi import MemInfo, SystemHealth
from pynixd.scheduler import Scheduler
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode, StorePath
from pynixd_store.build_stats import ExpectedBytes, StoreEstimate, StoreLink, slot_wait_seconds, transfer_seconds
from tests._conftest.placement import (
    FakeBuild,
    FakeMonitor,
    FakeStore,
    FixedRanking,
    FixedRng,
    Spawned,
    cpu,
    fake_placer,
    options,
)

if TYPE_CHECKING:
    from pynixd.build_queue import QueuedBuild
    from pynixd.wire import NixWriter

REMOTE = StoreId("sg1-n1")
OTHER = StoreId("sg1-n2")
MB = 1_000_000


def _ranked(*store_ids: StoreId) -> list[RankedStore[FakeStore]]:
    return [RankedStore(store_id, 0.0, FakeStore(store_id)) for store_id in store_ids]


def _rank(transfer: dict[StoreId, float] | None = None) -> list[StoreId]:
    local = FakeStore(LOCAL_STORE_ID)
    stores = {LOCAL_STORE_ID: local, REMOTE: FakeStore(REMOTE)}
    ranker = TelemetryStoreRanker(StoreRankingSettings())
    allocator = BuildAllocator(stores, local, ranker)
    ranked = allocator.rank_stores(FakeBuild(), {}, transfer_seconds=transfer)
    return [candidate.store_id for candidate in ranked]


class TestTheTransferCost:
    def test_adds_both_directions_and_the_round_trip(self) -> None:
        link = StoreLink(send_bytes_per_s=10 * MB, receive_bytes_per_s=20 * MB, dispatch_ms=500)

        assert transfer_seconds(ExpectedBytes(inputs=100 * MB, pull=40 * MB), link) == pytest.approx(12.5)

    def test_an_unmeasured_store_has_no_cost(self) -> None:
        assert transfer_seconds(ExpectedBytes(inputs=100 * MB), StoreLink()) is None

    def test_a_known_round_trip_alone_is_a_cost(self) -> None:
        """A tiny build whose bytes were never measured still pays the dispatch."""
        assert transfer_seconds(ExpectedBytes(), StoreLink(dispatch_ms=300)) == pytest.approx(0.3)

    def test_the_setup_adds_to_every_delegation(self) -> None:
        link = StoreLink(send_bytes_per_s=10 * MB)

        assert transfer_seconds(ExpectedBytes(inputs=10 * MB), link, 1.0) == pytest.approx(2.0)
        assert transfer_seconds(ExpectedBytes(), StoreLink(), 1.0) == pytest.approx(1.0)
        assert StoreEstimate(build_ms=230, setup_seconds=1.0).total_seconds() == pytest.approx(1.23)


@pytest.mark.parametrize(("setup", "expected"), [(1.0, [LOCAL_STORE_ID, REMOTE]), (0.0, [REMOTE, LOCAL_STORE_ID])])
def test_the_setup_keeps_a_trivial_build_local(setup: float, expected: list[StoreId]) -> None:
    """dynhetz, 2026-10-29: a trivial `runCommand` took 318 ms here and was
    expected at 230 ms on sg1, whose measured phases priced no round trip.
    All 20 of 20 went to sg1. nixidae/nixidae#1125."""
    local = FakeStore(LOCAL_STORE_ID)
    local.cpu_util = cpu(14.0)
    remote = FakeStore(REMOTE)
    remote.cpu_util = cpu(1.0)
    settings = StoreRankingSettings(time_penalty_per_second=50.0, cpu_reference_seconds=5.0)
    ranker = TelemetryStoreRanker(settings)
    allocator = BuildAllocator({LOCAL_STORE_ID: local, REMOTE: remote}, local, ranker)
    estimates = {
        LOCAL_STORE_ID: StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=318),
        REMOTE: StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=230, setup_seconds=setup),
    }

    ranked = allocator.rank_stores(FakeBuild(), {}, estimates=estimates, expected_build_seconds=0.318)

    assert [candidate.store_id for candidate in ranked] == expected


class TestTheLocalSlotWait:
    def test_is_the_soonest_end(self) -> None:
        assert slot_wait_seconds([(10_000, 100.0), (60_000, 100.0)], now=104.0) == pytest.approx(6.0)

    def test_a_build_past_its_estimate_ends_now(self) -> None:
        assert slot_wait_seconds([(1_000, 100.0)], now=200.0) == 0.0

    def test_no_estimate_is_no_answer(self) -> None:
        assert slot_wait_seconds([(None, 100.0)], now=101.0) is None


class TestTheRanking:
    def test_the_local_store_is_a_candidate(self) -> None:
        assert LOCAL_STORE_ID in _rank()

    def test_a_remote_wins_a_tie(self) -> None:
        """The local store was only a fallback, and an unmeasured world ranks as it did."""
        assert _rank() == [REMOTE, LOCAL_STORE_ID]

    def test_a_measured_transfer_puts_the_local_store_first(self) -> None:
        assert _rank({REMOTE: 5.0}) == [LOCAL_STORE_ID, REMOTE]


class TestTheMinimumScore:
    """Load decides who is full, and a transfer cost only orders the stores.

    dynhetz, 2026-10-22: a runCommand over a 366 MB source waited 20 minutes
    in "scheduling". Its transfer cost put every idle sg1 builder below 0,
    and load put the local store there."""

    @staticmethod
    def _rank(*, utilization: float, busy: int = 0, transfer: dict[StoreId, float] | None = None) -> list[StoreId]:
        local = FakeStore(LOCAL_STORE_ID)
        local.no_schedule = True
        remote = FakeStore(REMOTE)
        remote.cpu_util = cpu(utilization)
        stores = {LOCAL_STORE_ID: local, REMOTE: remote}
        ranker = TelemetryStoreRanker(StoreRankingSettings())
        ranked = BuildAllocator(stores, local, ranker).rank_stores(
            FakeBuild(), {}, running_builds={REMOTE: busy}, transfer_seconds=transfer
        )
        return [candidate.store_id for candidate in ranked]

    def test_a_large_transfer_keeps_an_idle_store(self) -> None:
        assert self._rank(utilization=0.0, transfer={REMOTE: 50.0}) == [REMOTE]

    def test_load_still_drops_a_store(self) -> None:
        """Busy at 100 %: no CPU points, and a running build costs 50. Full is full."""
        assert self._rank(utilization=100.0, busy=1) == []

    def test_connections_without_builds_cost_nothing(self) -> None:
        """Client queries hold the local store's connections; they are no load. nixidae/nixidae#1125."""
        local = FakeStore(LOCAL_STORE_ID)
        local.cpu_util = cpu(0.0)
        local.in_flight = 9
        ranker = TelemetryStoreRanker(StoreRankingSettings())
        ranked = BuildAllocator({LOCAL_STORE_ID: local}, local, ranker).rank_stores(
            FakeBuild(), {}, running_builds={LOCAL_STORE_ID: 0}
        )
        assert [(candidate.store_id, candidate.score) for candidate in ranked] == [(LOCAL_STORE_ID, 100.0)]


class TestAStoreNobodyCanMonitor:
    """A builder that refuses SFTP and exec still builds, after every store
    pynixd can see. nixidae/nixidae#1016."""

    @staticmethod
    def _rank(*, busy: int, min_score: float = -10_000.0) -> list[StoreId]:
        local = FakeStore(LOCAL_STORE_ID)
        local.no_schedule = True
        seen = FakeStore(OTHER)
        seen.cpu_util = cpu(100.0)
        stores = {LOCAL_STORE_ID: local, REMOTE: FakeStore(REMOTE), OTHER: seen}
        ranker = TelemetryStoreRanker(StoreRankingSettings(min_schedule_score=min_score))
        ranked = BuildAllocator(stores, local, ranker).rank_stores(FakeBuild(), {}, running_builds={OTHER: busy})
        return [candidate.store_id for candidate in ranked]

    def test_ranks_under_a_fully_busy_monitored_store(self) -> None:
        assert self._rank(busy=3) == [OTHER, REMOTE]

    def test_still_gets_builds_under_the_minimum_score(self) -> None:
        """The monitored store falls under the minimum; the blind one stays."""
        assert self._rank(busy=3, min_score=-100.0) == [REMOTE]


class TestTheExpectedTime:
    """The whole build's expected seconds on each store, and the CPU points
    scaled by how long the build runs. nixidae/nixidae#1125: a 200 ms build on
    an idle local store went to sg1, where it took 600 ms."""

    @staticmethod
    def _rank(
        *,
        local_cpu: float,
        expected_build_seconds: float,
        time_penalty: float | None = 50.0,
        cpu_reference: float | None = 5.0,
    ) -> list[StoreId]:
        local = FakeStore(LOCAL_STORE_ID)
        local.cpu_util = cpu(local_cpu)
        remote = FakeStore(REMOTE)
        remote.cpu_util = cpu(0.0)
        settings = StoreRankingSettings(time_penalty_per_second=time_penalty, cpu_reference_seconds=cpu_reference)
        ranker = TelemetryStoreRanker(settings)
        ranked = BuildAllocator({LOCAL_STORE_ID: local, REMOTE: remote}, local, ranker)
        estimates = {
            LOCAL_STORE_ID: StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=200),
            REMOTE: StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=600),
        }
        return [
            candidate.store_id
            for candidate in ranked.rank_stores(
                FakeBuild(),
                {},
                estimates=estimates,
                expected_build_seconds=expected_build_seconds,
            )
        ]

    def test_a_short_build_stays_on_a_busier_local_store(self) -> None:
        assert self._rank(local_cpu=60.0, expected_build_seconds=0.2) == [LOCAL_STORE_ID, REMOTE]

    def test_a_long_build_goes_to_the_idle_store(self) -> None:
        assert self._rank(local_cpu=60.0, expected_build_seconds=30.0) == [REMOTE, LOCAL_STORE_ID]

    def test_without_the_cpu_scale_the_idle_cpu_wins(self) -> None:
        assert self._rank(local_cpu=60.0, expected_build_seconds=0.2, cpu_reference=None) == [REMOTE, LOCAL_STORE_ID]

    def test_without_the_time_cost_the_tie_goes_remote(self) -> None:
        assert self._rank(local_cpu=0.0, expected_build_seconds=0.2, time_penalty=None) == [REMOTE, LOCAL_STORE_ID]

    def test_the_threshold_reads_the_unscaled_load(self) -> None:
        """A full store stays full, however short the build."""
        busy = FakeStore(REMOTE)
        busy.cpu_util = cpu(100.0)
        settings = StoreRankingSettings(cpu_reference_seconds=5.0)

        terms = score_store(busy, settings, 1, 0, None, expected_build_seconds=0.1)

        assert terms.cpu_scale == pytest.approx(0.02)
        assert terms.load_score == pytest.approx(-50.0)
        assert not terms.passed

    def test_a_short_build_barely_feels_the_herd(self) -> None:
        """A burst of 20 trivial probes spread to sg1 once eight sat locally. nixidae/nixidae#1125."""
        store = FakeStore(REMOTE)
        store.cpu_util = cpu(0.0)
        settings = StoreRankingSettings(cpu_reference_seconds=5.0)

        alone = score_store(store, settings, 0, 0, None, expected_build_seconds=0.1)
        herded = score_store(store, settings, 0, 1, None, expected_build_seconds=0.1)

        assert herded.herd_points == pytest.approx(-100.0)
        assert herded.final_score - alone.final_score == pytest.approx(-2.0)


class TestPerCoreLoad:
    """A build costs one core's share of the CPU points. nixidae/nixidae#1125:
    at 50 and 100 points a build, a 16-core store took one build per pass."""

    @staticmethod
    def _store(cores: float | None) -> FakeStore:
        store = FakeStore(REMOTE)
        store.cpu_util = cpu(0.0)
        store.monitor = FakeMonitor(SystemHealth(cores=cores))
        return store

    def test_a_large_store_takes_a_build_per_core(self) -> None:
        settings = StoreRankingSettings(per_core_load=True)

        terms = score_store(self._store(16.0), settings, 8, 4, None)

        assert terms.concurrency_points == pytest.approx(-50.0)
        assert terms.herd_points == pytest.approx(-25.0)
        assert terms.passed

    def test_a_store_of_unknown_cores_keeps_the_fixed_prices(self) -> None:
        settings = StoreRankingSettings(per_core_load=True)

        terms = score_store(self._store(None), settings, 1, 1, None)

        assert terms.concurrency_points == pytest.approx(-settings.concurrency_penalty)
        assert terms.herd_points == pytest.approx(-settings.thundering_herd_penalty)


class TestLocalWinsTies:
    @staticmethod
    def _rank(*, local_wins_ties: bool) -> list[StoreId]:
        local = FakeStore(LOCAL_STORE_ID)
        stores = {LOCAL_STORE_ID: local, REMOTE: FakeStore(REMOTE)}
        settings = StoreRankingSettings(local_wins_ties=local_wins_ties)
        ranker = TelemetryStoreRanker(settings)
        allocator = BuildAllocator(stores, local, ranker)
        return [candidate.store_id for candidate in allocator.rank_stores(FakeBuild(), {})]

    def test_a_remote_wins_a_tie_by_default(self) -> None:
        assert self._rank(local_wins_ties=False) == [REMOTE, LOCAL_STORE_ID]

    def test_the_local_store_wins_a_tie_when_set(self) -> None:
        assert self._rank(local_wins_ties=True) == [LOCAL_STORE_ID, REMOTE]


class TestThePick:
    def test_takes_the_best_ranked(self) -> None:
        picked = pick_store(_ranked(REMOTE, LOCAL_STORE_ID), LOCAL_STORE_ID, local_slot_full=False)

        assert picked is not None
        assert picked.store_id == REMOTE

    def test_a_full_local_slot_passes_to_the_next_store(self) -> None:
        picked = pick_store(_ranked(LOCAL_STORE_ID, REMOTE), LOCAL_STORE_ID, local_slot_full=True)

        assert picked is not None
        assert picked.store_id == REMOTE


class TestExploration:
    """A store's estimate for a shape changes only when it builds that shape.
    dynhetz, 2026-10-29: the local `trace-probe` median stayed at 1.1 s after
    a fix made it 0.45 s, so 20 of 20 probes went to sg1. nixidae/nixidae#1125."""

    @staticmethod
    def _explore(*, draw: float, pick: float = 0.0, local_slot_full: bool = False) -> StoreId | None:
        ranked = _ranked(REMOTE, LOCAL_STORE_ID, OTHER)
        explored = explore_store(
            ranked, ranked[0], LOCAL_STORE_ID, local_slot_full=local_slot_full, share=0.1, draw=draw, pick=pick
        )
        return None if explored is None else explored.store_id

    def test_a_draw_over_the_share_keeps_the_best(self) -> None:
        assert self._explore(draw=0.1) is None

    def test_a_draw_under_the_share_takes_another_candidate(self) -> None:
        assert self._explore(draw=0.05, pick=0.0) == LOCAL_STORE_ID
        assert self._explore(draw=0.05, pick=0.99) == OTHER

    def test_never_a_full_local_slot(self) -> None:
        assert self._explore(draw=0.0, pick=0.0, local_slot_full=True) == OTHER

    def test_no_other_candidate_keeps_the_best(self) -> None:
        ranked = _ranked(REMOTE)
        assert (
            explore_store(ranked, ranked[0], LOCAL_STORE_ID, local_slot_full=False, share=1.0, draw=0.0, pick=0.0)
            is None
        )


class TestPreferLocalBuild:
    @staticmethod
    def _pick(*, wait: float | None, remote_seconds: float | None) -> StoreId | None:
        transfer = {} if remote_seconds is None else {REMOTE: remote_seconds}
        picked = pick_store(
            _ranked(REMOTE, OTHER),
            LOCAL_STORE_ID,
            local_slot_full=True,
            prefer_local=PreferLocal(wait_seconds=wait, transfer_seconds=transfer, weight=10.0),
        )
        return None if picked is None else picked.store_id

    def test_a_free_local_slot_wins_over_a_better_ranked_remote(self) -> None:
        picked = pick_store(
            _ranked(REMOTE, LOCAL_STORE_ID),
            LOCAL_STORE_ID,
            local_slot_full=False,
            prefer_local=PreferLocal(wait_seconds=None, transfer_seconds={}, weight=10.0),
        )

        assert picked is not None
        assert picked.store_id == LOCAL_STORE_ID

    def test_waits_while_the_wait_is_cheaper_than_the_weighted_delegation(self) -> None:
        assert self._pick(wait=19.0, remote_seconds=2.0) is None

    def test_delegates_once_the_wait_passes_the_weighted_delegation(self) -> None:
        assert self._pick(wait=21.0, remote_seconds=2.0) == REMOTE

    def test_waits_when_the_wait_is_unknown(self) -> None:
        """Nix waits for a local slot however long; with no estimate, so does pynixd."""
        assert self._pick(wait=None, remote_seconds=2.0) is None

    def test_waits_when_the_delegation_is_unmeasured(self) -> None:
        assert self._pick(wait=500.0, remote_seconds=None) is None

    def test_a_client_on_this_machine_waits_however_long(self) -> None:
        """`derivation-building-goal.cc:606`: Nix waits for the local slot. nixidae/nixidae#1259."""
        picked = pick_store(
            _ranked(REMOTE, OTHER),
            LOCAL_STORE_ID,
            local_slot_full=True,
            prefer_local=PreferLocal(
                wait_seconds=3600.0, transfer_seconds={REMOTE: 1.0}, weight=10.0, wait_for_local=True
            ),
        )

        assert picked is None


def _request(number: int, env: dict[str, str] | None = None) -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/{number:032d}-trivial.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh", env=env or {}),
        build_mode=BuildMode.NORMAL,
    )


def _remote_alone() -> FixedRanking:
    """The ranking when the local store is under pressure: the remote alone."""
    return FixedRanking.of(FakeStore(REMOTE))


async def _assign(env: dict[str, str]) -> Spawned:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1, env))
    spawned = Spawned()
    await fake_placer(queue, _remote_alone(), spawned).assign([queue.by_id[build_id]], {})
    return spawned


class TestALocalStoreUnderPressure:
    """Measured on dynhetz, 2026-10-21: PSI cpu some at 71 % put the local
    store under `min_schedule_score`. A `preferLocalBuild` derivation then
    waited for a local slot that no running build would free, and the
    deployed pynixd hung every `writeText`."""

    @pytest.mark.anyio
    async def test_still_takes_a_prefer_local_build(self) -> None:
        """Nix ignores load for `preferLocalBuild`: a free slot is enough."""
        spawned = await _assign({"preferLocalBuild": "1"})

        assert spawned.stores == [LOCAL_STORE_ID]

    @pytest.mark.anyio
    async def test_leaves_other_builds_to_the_ranking(self) -> None:
        spawned = await _assign({})

        assert spawned.stores == [REMOTE]


class BrokenEstimate:
    """The estimate raises for one build, as a database or a bad derivation can."""

    def __init__(self, broken: BuildId) -> None:
        self.broken = broken

    async def transfer_seconds(self, build: QueuedBuild) -> dict[StoreId, float]:
        if build.build_id == self.broken:
            raise RuntimeError("an estimate that raises")
        return {}


class TestOneBrokenBuild:
    """`Scheduler.start` caught a failure of the whole pass, and the next
    pass met the same build first. So one such build held every other
    build pending for good. nixidae/nixidae#1003."""

    @pytest.mark.anyio
    async def test_fails_alone_and_the_pass_goes_on(self) -> None:
        queue = BuildQueue()
        broken, broken_future = await queue.enqueue(_request(1))
        healthy, _ = await queue.enqueue(_request(2))
        spawned = Spawned()
        placer = fake_placer(queue, _remote_alone(), spawned, estimator=BrokenEstimate(broken))

        await placer.assign([queue.by_id[broken], queue.by_id[healthy]], {})

        assert spawned.stores == [REMOTE]
        assert queue.by_id[healthy].is_building
        assert "an estimate that raises" in str(broken_future.result().result.error_msg)


def _blocked_alone(blocked: BuildId) -> FixedRanking:
    """The first build's builders are all under `min_schedule_score`; the second's are not."""
    remote = RankedStore(REMOTE, 0.0, FakeStore(REMOTE))
    return FixedRanking(lambda build: [] if build.build_id == blocked else [remote])


@pytest.mark.anyio
async def test_a_build_whose_builders_are_full_does_not_hold_back_the_next() -> None:
    """Full is full: the first build waits, and the queue is not FIFO behind it.
    User's design, 2026-10-22."""
    queue = BuildQueue()
    blocked, _ = await queue.enqueue(_request(1))
    other, _ = await queue.enqueue(_request(2))
    spawned = Spawned()

    waiting = await fake_placer(queue, _blocked_alone(blocked), spawned).assign(
        [queue.by_id[blocked], queue.by_id[other]], {}
    )

    assert spawned.stores == [REMOTE]
    assert queue.by_id[other].is_building
    assert [build.build_id for build in waiting] == [blocked]


class WakingScheduler:
    """Enough of `Scheduler` for `schedule` and `on_sample`: the loop around the placer, not placement."""

    def __init__(self, queue: BuildQueue, blocked: BuildId) -> None:
        self.queue = queue
        self.placer = fake_placer(queue, _blocked_alone(blocked), Spawned())
        self.stores: dict[StoreId, FakeStore] = {}
        self.builds_waiting = False
        self.triggered = 0

    def _filter_schedulable(self, pending: list[QueuedBuild]) -> tuple[list[QueuedBuild], dict[StoreId, int]]:
        return pending, {}

    def _update_store_metrics(self) -> None:
        return

    def trigger(self) -> None:
        self.triggered += 1


@pytest.mark.anyio
async def test_a_sample_wakes_a_build_that_waits_for_a_store() -> None:
    """A store that comes back by its figures alone triggered no pass, so
    the build waited for an unrelated event. Found by the handover test,
    2026-10-22. nixidae/nixidae#1016."""
    queue = BuildQueue()
    blocked, _ = await queue.enqueue(_request(1))
    scheduler = WakingScheduler(queue, blocked)

    await Scheduler.schedule(cast("Any", scheduler))
    Scheduler.on_sample(cast("Any", scheduler), SystemHealth())

    assert scheduler.builds_waiting
    assert scheduler.triggered == 1

    await queue.fail(blocked, "gone")
    await Scheduler.schedule(cast("Any", scheduler))
    Scheduler.on_sample(cast("Any", scheduler), SystemHealth())

    assert not scheduler.builds_waiting
    assert scheduler.triggered == 1, "with nothing waiting a sample costs no pass"


def _two_remotes() -> FixedRanking:
    """REMOTE first and OTHER second."""
    return FixedRanking.of(FakeStore(REMOTE), FakeStore(OTHER))


@pytest.mark.anyio
async def test_a_retry_goes_to_a_store_the_build_has_not_failed_on() -> None:
    """sg1-n1 failed one missing input three times running. nixidae/nixidae#1015."""
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1))
    queue.by_id[build_id].reset_for_retry(REMOTE, "getting status of ...: No such file or directory")
    spawned = Spawned()

    await fake_placer(queue, _two_remotes(), spawned).assign([queue.by_id[build_id]], {})

    assert spawned.stores == [OTHER]


GIB = 1024 * 1024 * 1024


def _with_memory(store_id: StoreId, available: int | None) -> RankedStore[FakeStore]:
    """A candidate whose monitor reports *available* bytes, or no monitor at all."""
    store = FakeStore(store_id)
    if available is not None:
        meminfo = MemInfo(mem_total=64 * GIB // 1024, mem_available=available // 1024)
        store.monitor = FakeMonitor(SystemHealth(meminfo=meminfo))
    return RankedStore(store_id, 0.0, store)


class TestTheMemoryFit:
    """A build's expected peak against each store's room. User's design, nixidae/nixidae#1187."""

    def test_leaves_out_a_store_short_of_memory(self) -> None:
        fit = fit_memory([_with_memory(REMOTE, 1 * GIB), _with_memory(OTHER, 8 * GIB)], 2 * GIB, {})

        assert [c.store_id for c in fit.stores] == [OTHER]
        assert fit.short == {REMOTE: "1024 MiB free for an expected peak of 2048 MiB"}
        assert fit.reason is None

    def test_counts_what_running_builds_reserve(self) -> None:
        fit = fit_memory([_with_memory(REMOTE, 4 * GIB), _with_memory(OTHER, 8 * GIB)], 2 * GIB, {REMOTE: 3 * GIB})

        assert [c.store_id for c in fit.stores] == [OTHER]

    def test_an_unmeasured_store_fits(self) -> None:
        fit = fit_memory([_with_memory(REMOTE, None), _with_memory(OTHER, 1 * GIB)], 2 * GIB, {})

        assert [c.store_id for c in fit.stores] == [REMOTE]

    def test_no_estimate_keeps_every_candidate(self) -> None:
        ranked = [_with_memory(REMOTE, 1 * GIB), _with_memory(OTHER, 8 * GIB)]

        assert fit_memory(ranked, None, {}).stores == ranked

    def test_with_no_fit_the_most_room_goes_first(self) -> None:
        """A build never waits on an estimate."""
        fit = fit_memory([_with_memory(REMOTE, 1 * GIB), _with_memory(OTHER, 3 * GIB)], 4 * GIB, {})

        assert [c.store_id for c in fit.stores] == [OTHER, REMOTE]
        assert fit.short == {}
        assert fit.reason == "no candidate has memory for an expected peak of 4096 MiB; the most room goes first"


@pytest.mark.anyio
async def test_a_second_large_build_goes_where_memory_is_left() -> None:
    """The first 3 GiB build reserves its peak on REMOTE, so the second leaves for OTHER."""
    queue = BuildQueue()
    first, _ = await queue.enqueue(_request(1))
    second, _ = await queue.enqueue(_request(2))
    for build_id in (first, second):
        queue.by_id[build_id].expected_peak_bytes = 3 * GIB
    # REMOTE first; each remote has 4 GiB available.
    ranked = [_with_memory(REMOTE, 4 * GIB), _with_memory(OTHER, 4 * GIB)]
    spawned = Spawned()

    await fake_placer(queue, FixedRanking(lambda _build: list(ranked)), spawned).assign(
        [queue.by_id[first], queue.by_id[second]], {}
    )

    assert spawned.stores == [REMOTE, OTHER]


@pytest.mark.parametrize(("draw", "expected"), [(0.0, OTHER), (0.5, REMOTE)])
@pytest.mark.anyio
async def test_an_exploration_draw_sends_the_build_past_the_best(draw: float, expected: StoreId) -> None:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1))
    spawned = Spawned()
    placer = fake_placer(
        queue, _two_remotes(), spawned, settings=StoreRankingSettings(explore_share=0.1), rng=FixedRng(draw)
    )

    await placer.assign([queue.by_id[build_id]], {})

    assert spawned.stores == [expected]


@pytest.mark.anyio
async def test_a_prefer_local_build_is_never_explored() -> None:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1, {"preferLocalBuild": "1"}))
    spawned = Spawned()
    placer = fake_placer(
        queue, _remote_alone(), spawned, settings=StoreRankingSettings(explore_share=1.0), rng=FixedRng(0.0)
    )

    await placer.assign([queue.by_id[build_id]], {})

    assert spawned.stores == [LOCAL_STORE_ID]


class RoundTripOnly:
    """A delegation's round trip is 0.3 s; the inputs' cost is measured when *measured*."""

    def __init__(self, *, measured: bool) -> None:
        self.measured = measured

    async def transfer_seconds(self, build: QueuedBuild) -> dict[StoreId, float]:
        if self.measured:
            build.inputs_measured.add(REMOTE)
        return {REMOTE: 0.3}


async def _assign_prefer_local(*, measured: bool, from_this_machine: bool = False) -> Spawned:
    """The local slot of `-j1` is held by a build expected to end in 100 s."""
    queue = BuildQueue()
    running_id, _ = await queue.enqueue(_request(2))
    running = queue.by_id[running_id]
    running.assigned_store_id = LOCAL_STORE_ID
    running.worker = anyio.CancelScope()
    running.started_at = time.monotonic()
    running.expected_duration = 100_000
    client = ClientConn(w=cast("NixWriter", None))
    client.on_this_machine = from_this_machine
    build_id, _ = await queue.enqueue(_request(1, {"preferLocalBuild": "1"}), options=options(1), askers=[client])
    spawned = Spawned()
    placer = fake_placer(queue, _remote_alone(), spawned, estimator=RoundTripOnly(measured=measured))

    await placer.assign([queue.by_id[build_id]], {LOCAL_STORE_ID: 1})

    return spawned


class TestAPreferLocalBuildNeverSentToTheStore:
    """dynhetz, 2026-10-22: home-manager's `man-paths` had built locally 11
    times and never on sg1-n3. Its input bytes there were unknown, and a
    round trip alone priced the delegation under a second. It went, and
    sent 9.3 GB in 648 s for a 0.3 s build."""

    @pytest.mark.anyio
    async def test_waits_while_its_inputs_there_are_unmeasured(self) -> None:
        spawned = await _assign_prefer_local(measured=False)

        assert spawned.stores == []

    @pytest.mark.anyio
    async def test_goes_once_they_are_measured_and_the_wait_is_dearer(self) -> None:
        spawned = await _assign_prefer_local(measured=True)

        assert spawned.stores == [REMOTE]

    @pytest.mark.anyio
    async def test_a_client_on_this_machine_keeps_it_waiting_for_the_local_slot(self) -> None:
        spawned = await _assign_prefer_local(measured=True, from_this_machine=True)

        assert spawned.stores == []


class LocalShortOfDisk(FixedRanking):
    """The remote alone, and the local store excluded for disk."""

    def rank_stores(self, build: QueuedBuild, *args: Any, **kwargs: Any) -> RankedStores[FakeStore]:
        ranked = super().rank_stores(build, *args, **kwargs)
        return RankedStores(list(ranked), [ExcludedStore(LOCAL_STORE_ID, SHORT_OF_DISK)])


@pytest.mark.anyio
async def test_a_prefer_local_build_leaves_a_local_store_short_of_disk() -> None:
    queue = BuildQueue()
    client = ClientConn(w=cast("NixWriter", None))
    client.on_this_machine = True
    build_id, _ = await queue.enqueue(_request(1, {"preferLocalBuild": "1"}), askers=[client])
    spawned = Spawned()
    ranking = LocalShortOfDisk.of(FakeStore(REMOTE))

    await fake_placer(queue, ranking, spawned).assign([queue.by_id[build_id]], {})

    assert spawned.stores == [REMOTE]


class ValidityStore:
    """A remote that holds *held*, or that does not answer."""

    def __init__(self, held: set[str], *, broken: bool = False) -> None:
        self.store_id = REMOTE
        self.held = held
        self.broken = broken

    async def execute(self, request: QueryValidPathsRequest) -> QueryValidPathsResponse:
        if self.broken:
            raise ConnectionResetError("builder went away")
        return QueryValidPathsResponse(paths={path for path in request.paths if str(path) in self.held})


SIZES = {StorePath("/nix/store/00000000000000000000000000000001-a"): 10 * MB}
SIZES[StorePath("/nix/store/00000000000000000000000000000002-b")] = 30 * MB


class TestTheMissingInputBytes:
    """The bytes a delegation sends are the input closure the remote lacks,
    as `QueryValidPaths` answers it."""

    @pytest.mark.anyio
    async def test_are_what_the_store_lacks(self) -> None:
        store = ValidityStore({"/nix/store/00000000000000000000000000000001-a"})

        missing = await missing_input_bytes(store, {}, SIZES)

        assert missing == 30 * MB

    @pytest.mark.anyio
    async def test_are_unknown_when_the_store_does_not_answer(self) -> None:
        store = ValidityStore(set(), broken=True)

        assert await missing_input_bytes(store, {}, SIZES) is None

    @pytest.mark.anyio
    async def test_are_what_the_store_answers_and_not_its_daemon_client(self) -> None:
        """A store with no daemon: its client refuses, and its own database answers."""
        client = ValidityStore(set(), broken=True)
        store = ValidityStore({"/nix/store/00000000000000000000000000000002-b"})

        missing = await missing_input_bytes(client, {REMOTE: store}, SIZES)

        assert missing == 10 * MB
