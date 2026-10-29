"""Where a build goes: the local store ranks with the others, and delegation has a measured cost.

nixidae/nixidae#1001, and `preferLocalBuild` from nixidae/nixidae#947.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, BuildId, StoreId
from pynixd.allocator import (
    BuildAllocator,
    PreferLocal,
    RankedStore,
    TelemetryStoreRanker,
    explore_store,
    pick_store,
    score_store,
)
from pynixd.build_queue import BuildQueue
from pynixd.build_stats import ExpectedBytes, StoreEstimate, StoreLink, slot_wait_seconds, transfer_seconds
from pynixd.config import PlacementTraceSettings, StoreRankingSettings
from pynixd.placement_trace import PlacementTrace
from pynixd.psi import SystemHealth
from pynixd.scheduler import Scheduler
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode, StorePath

if TYPE_CHECKING:
    from pynixd.build_queue import QueuedBuild

REMOTE = StoreId("sg1-n1")
OTHER = StoreId("sg1-n2")
MB = 1_000_000


class FakeStore:
    def __init__(self, store_id: StoreId) -> None:
        self.store_id = store_id
        self.cpu_util: object | None = None
        self.monitor = None
        self.in_flight = 0
        self.score_penalty = 0
        self.priority = 1.0
        self.is_healthy = True
        self.draining = False
        self.no_schedule = False

    def supports_derivation(self, platform: str, features: set[str]) -> bool:
        del platform, features
        return True


class FakeBuild:
    request = SimpleNamespace(derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"))

    @staticmethod
    def is_blacklisted(store_id: StoreId) -> bool:
        del store_id
        return False


def _ranked(*store_ids: StoreId) -> list[RankedStore]:
    return [RankedStore(store_id, 0.0, cast("Any", FakeStore(store_id))) for store_id in store_ids]


def _rank(transfer: dict[StoreId, float] | None = None) -> list[StoreId]:
    local = FakeStore(LOCAL_STORE_ID)
    stores = {LOCAL_STORE_ID: local, REMOTE: FakeStore(REMOTE)}
    ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=StoreRankingSettings())))
    allocator = BuildAllocator(cast("Any", stores), cast("Any", local), ranker)
    ranked = allocator.rank_stores(cast("Any", FakeBuild()), {}, transfer_seconds=transfer)
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
    local.cpu_util = SimpleNamespace(utilization=14.0)
    remote = FakeStore(REMOTE)
    remote.cpu_util = SimpleNamespace(utilization=1.0)
    settings = StoreRankingSettings(time_penalty_per_second=50.0, cpu_reference_seconds=5.0)
    ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=settings)))
    allocator = BuildAllocator(cast("Any", {LOCAL_STORE_ID: local, REMOTE: remote}), cast("Any", local), ranker)
    estimates = {
        LOCAL_STORE_ID: StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=318),
        REMOTE: StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=230, setup_seconds=setup),
    }

    ranked = allocator.rank_stores(cast("Any", FakeBuild()), {}, estimates=estimates, expected_build_seconds=0.318)

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
        remote.cpu_util = SimpleNamespace(utilization=utilization)
        stores = {LOCAL_STORE_ID: local, REMOTE: remote}
        ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=StoreRankingSettings())))
        ranked = BuildAllocator(cast("Any", stores), cast("Any", local), ranker).rank_stores(
            cast("Any", FakeBuild()), {}, running_builds={REMOTE: busy}, transfer_seconds=transfer
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
        local.cpu_util = SimpleNamespace(utilization=0.0)
        local.in_flight = 9
        ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=StoreRankingSettings())))
        ranked = BuildAllocator(cast("Any", {LOCAL_STORE_ID: local}), cast("Any", local), ranker).rank_stores(
            cast("Any", FakeBuild()), {}, running_builds={LOCAL_STORE_ID: 0}
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
        seen.cpu_util = SimpleNamespace(utilization=100.0)
        stores = {LOCAL_STORE_ID: local, REMOTE: FakeStore(REMOTE), OTHER: seen}
        ranker = TelemetryStoreRanker(
            cast("Any", SimpleNamespace(ranking=StoreRankingSettings(min_schedule_score=min_score)))
        )
        ranked = BuildAllocator(cast("Any", stores), cast("Any", local), ranker).rank_stores(
            cast("Any", FakeBuild()), {}, running_builds={OTHER: busy}
        )
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
        local.cpu_util = SimpleNamespace(utilization=local_cpu)
        remote = FakeStore(REMOTE)
        remote.cpu_util = SimpleNamespace(utilization=0.0)
        settings = StoreRankingSettings(time_penalty_per_second=time_penalty, cpu_reference_seconds=cpu_reference)
        ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=settings)))
        ranked = BuildAllocator(cast("Any", {LOCAL_STORE_ID: local, REMOTE: remote}), cast("Any", local), ranker)
        estimates = {
            LOCAL_STORE_ID: StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=200),
            REMOTE: StoreEstimate(inputs_bytes=0, pull_bytes=0, build_ms=600),
        }
        return [
            candidate.store_id
            for candidate in ranked.rank_stores(
                cast("Any", FakeBuild()),
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
        busy.cpu_util = SimpleNamespace(utilization=100.0)
        settings = StoreRankingSettings(cpu_reference_seconds=5.0)

        terms = score_store(cast("Any", busy), settings, 1, 0, None, expected_build_seconds=0.1)

        assert terms.cpu_scale == pytest.approx(0.02)
        assert terms.load_score == pytest.approx(-50.0)
        assert not terms.passed

    def test_a_short_build_barely_feels_the_herd(self) -> None:
        """A burst of 20 trivial probes spread to sg1 once eight sat locally. nixidae/nixidae#1125."""
        store = FakeStore(REMOTE)
        store.cpu_util = SimpleNamespace(utilization=0.0)
        settings = StoreRankingSettings(cpu_reference_seconds=5.0)

        alone = score_store(cast("Any", store), settings, 0, 0, None, expected_build_seconds=0.1)
        herded = score_store(cast("Any", store), settings, 0, 1, None, expected_build_seconds=0.1)

        assert herded.herd_points == pytest.approx(-100.0)
        assert herded.final_score - alone.final_score == pytest.approx(-2.0)


class TestPerCoreLoad:
    """A build costs one core's share of the CPU points. nixidae/nixidae#1125:
    at 50 and 100 points a build, a 16-core store took one build per pass."""

    @staticmethod
    def _store(cores: float | None) -> FakeStore:
        store = FakeStore(REMOTE)
        store.cpu_util = SimpleNamespace(utilization=0.0)
        store.monitor = cast("Any", SimpleNamespace(health=SimpleNamespace(cores=cores, psi=None)))
        return store

    def test_a_large_store_takes_a_build_per_core(self) -> None:
        settings = StoreRankingSettings(per_core_load=True)

        terms = score_store(cast("Any", self._store(16.0)), settings, 8, 4, None)

        assert terms.concurrency_points == pytest.approx(-50.0)
        assert terms.herd_points == pytest.approx(-25.0)
        assert terms.passed

    def test_a_store_of_unknown_cores_keeps_the_fixed_prices(self) -> None:
        settings = StoreRankingSettings(per_core_load=True)

        terms = score_store(cast("Any", self._store(None)), settings, 1, 1, None)

        assert terms.concurrency_points == pytest.approx(-settings.concurrency_penalty)
        assert terms.herd_points == pytest.approx(-settings.thundering_herd_penalty)


class TestLocalWinsTies:
    @staticmethod
    def _rank(*, local_wins_ties: bool) -> list[StoreId]:
        local = FakeStore(LOCAL_STORE_ID)
        stores = {LOCAL_STORE_ID: local, REMOTE: FakeStore(REMOTE)}
        settings = StoreRankingSettings(local_wins_ties=local_wins_ties)
        ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=settings)))
        allocator = BuildAllocator(cast("Any", stores), cast("Any", local), ranker)
        return [candidate.store_id for candidate in allocator.rank_stores(cast("Any", FakeBuild()), {})]

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


class LocalStore(FakeStore):
    """The local store, which the ranker dropped: its score fell under `min_schedule_score`."""


class AssignScheduler:
    """Enough of `Scheduler` for `_assign_to_stores`, with a ranker that
    returns the remote alone, as it does when the local store is under
    pressure."""

    def __init__(self, queue: BuildQueue) -> None:
        self.queue = queue
        self.local_store = LocalStore(LOCAL_STORE_ID)
        remote = RankedStore(REMOTE, 0.0, cast("Any", FakeStore(REMOTE)))
        self.allocator = SimpleNamespace(rank_stores=lambda *_a, **_k: [remote])
        self.ranker = SimpleNamespace(settings=StoreRankingSettings())
        self.rng: FixedRng | None = None
        self.stores: dict[StoreId, object] = {}
        self.assigned: list[StoreId] = []

    _assign_one = Scheduler._assign_one  # noqa: SLF001 -- the assignment is the unit
    _place = Scheduler._place  # noqa: SLF001 -- the assignment is the unit
    _trace_decision = Scheduler._trace_decision  # noqa: SLF001 -- a trace with no database returns at once
    trace = PlacementTrace(None, PlacementTraceSettings())
    _client_forbids_local = Scheduler._client_forbids_local  # noqa: SLF001 -- part of the assignment

    def _has_compatible_store(self, *_args: Any) -> bool:
        return True

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


def _request(number: int, env: dict[str, str] | None = None) -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/{number:032d}-trivial.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh", env=env or {}),
        build_mode=BuildMode.NORMAL,
    )


async def _assign(env: dict[str, str]) -> AssignScheduler:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1, env))
    scheduler = AssignScheduler(queue)
    await Scheduler._assign_to_stores(cast("Any", scheduler), [queue.by_id[build_id]], {})  # noqa: SLF001 -- the assignment is the unit
    return scheduler


class TestALocalStoreUnderPressure:
    """Measured on dynhetz, 2026-10-21: PSI cpu some at 71 % put the local
    store under `min_schedule_score`. A `preferLocalBuild` derivation then
    waited for a local slot that no running build would free, and the
    deployed pynixd hung every `writeText`."""

    @pytest.mark.anyio
    async def test_still_takes_a_prefer_local_build(self) -> None:
        """Nix ignores load for `preferLocalBuild`: a free slot is enough."""
        scheduler = await _assign({"preferLocalBuild": "1"})

        assert scheduler.assigned == [LOCAL_STORE_ID]

    @pytest.mark.anyio
    async def test_leaves_other_builds_to_the_ranking(self) -> None:
        scheduler = await _assign({})

        assert scheduler.assigned == [REMOTE]


class BrokenScheduler(AssignScheduler):
    """`_transfer_seconds` raises for one build, as a database or a bad derivation can."""

    def __init__(self, queue: BuildQueue, broken: BuildId) -> None:
        super().__init__(queue)
        self.broken = broken

    async def _transfer_seconds(self, *args: Any) -> dict[StoreId, float]:
        build: QueuedBuild = args[0]
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
        scheduler = BrokenScheduler(queue, broken)

        await Scheduler._assign_to_stores(  # noqa: SLF001 -- the assignment is the unit
            cast("Any", scheduler), [queue.by_id[broken], queue.by_id[healthy]], {}
        )

        assert scheduler.assigned == [REMOTE]
        assert queue.by_id[healthy].is_building
        assert "an estimate that raises" in str(broken_future.result().result.error_msg)


class OnePressuredPoolScheduler(AssignScheduler):
    """The first build's builders are all under `min_schedule_score`; the second's are not."""

    def __init__(self, queue: BuildQueue, blocked: BuildId) -> None:
        super().__init__(queue)
        remote = RankedStore(REMOTE, 0.0, cast("Any", FakeStore(REMOTE)))
        self.allocator = SimpleNamespace(
            rank_stores=lambda build, *_a, **_k: [] if build.build_id == blocked else [remote]
        )


@pytest.mark.anyio
async def test_a_build_whose_builders_are_full_does_not_hold_back_the_next() -> None:
    """Full is full: the first build waits, and the queue is not FIFO behind it.
    User's design, 2026-10-22."""
    queue = BuildQueue()
    blocked, _ = await queue.enqueue(_request(1))
    other, _ = await queue.enqueue(_request(2))
    scheduler = OnePressuredPoolScheduler(queue, blocked)

    waiting = await Scheduler._assign_to_stores(  # noqa: SLF001 -- the assignment is the unit
        cast("Any", scheduler), [queue.by_id[blocked], queue.by_id[other]], {}
    )

    assert scheduler.assigned == [REMOTE]
    assert queue.by_id[other].is_building
    assert [build.build_id for build in waiting] == [blocked]


class WakingScheduler(OnePressuredPoolScheduler):
    """Enough of `Scheduler` for `schedule` and `on_sample`."""

    def __init__(self, queue: BuildQueue, blocked: BuildId) -> None:
        super().__init__(queue, blocked)
        self.builds_waiting = False
        self.triggered = 0
        self.pass_id = 0

    def _filter_schedulable(self, pending: list[QueuedBuild]) -> tuple[list[QueuedBuild], dict[Any, int]]:
        return pending, {}

    def _update_store_metrics(self) -> None:
        return

    def trigger(self) -> None:
        self.triggered += 1

    _assign_to_stores = Scheduler._assign_to_stores  # noqa: SLF001 -- part of the pass


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


class TwoRemotesScheduler(AssignScheduler):
    """The ranker puts REMOTE first and OTHER second."""

    def __init__(self, queue: BuildQueue) -> None:
        super().__init__(queue)
        ranked = [
            RankedStore(REMOTE, 0.0, cast("Any", FakeStore(REMOTE))),
            RankedStore(OTHER, 0.0, cast("Any", FakeStore(OTHER))),
        ]
        self.allocator = SimpleNamespace(rank_stores=lambda *_a, **_k: list(ranked))


@pytest.mark.anyio
async def test_a_retry_goes_to_a_store_the_build_has_not_failed_on() -> None:
    """sg1-n1 failed one missing input three times running. nixidae/nixidae#1015."""
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1))
    queue.by_id[build_id].reset_for_retry(REMOTE, "getting status of ...: No such file or directory")
    scheduler = TwoRemotesScheduler(queue)

    await Scheduler._assign_to_stores(cast("Any", scheduler), [queue.by_id[build_id]], {})  # noqa: SLF001 -- the assignment is the unit

    assert scheduler.assigned == [OTHER]


class FixedRng:
    """Every draw returns `value`."""

    def __init__(self, value: float) -> None:
        self.value = value

    def random(self) -> float:
        return self.value


@pytest.mark.parametrize(("draw", "expected"), [(0.0, OTHER), (0.5, REMOTE)])
@pytest.mark.anyio
async def test_an_exploration_draw_sends_the_build_past_the_best(draw: float, expected: StoreId) -> None:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1))
    scheduler = TwoRemotesScheduler(queue)
    scheduler.ranker = SimpleNamespace(settings=StoreRankingSettings(explore_share=0.1))
    scheduler.rng = FixedRng(draw)

    await Scheduler._assign_to_stores(cast("Any", scheduler), [queue.by_id[build_id]], {})  # noqa: SLF001 -- the assignment is the unit

    assert scheduler.assigned == [expected]


@pytest.mark.anyio
async def test_a_prefer_local_build_is_never_explored() -> None:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1, {"preferLocalBuild": "1"}))
    scheduler = AssignScheduler(queue)
    scheduler.ranker = SimpleNamespace(settings=StoreRankingSettings(explore_share=1.0))
    scheduler.rng = FixedRng(0.0)

    await Scheduler._assign_to_stores(cast("Any", scheduler), [queue.by_id[build_id]], {})  # noqa: SLF001 -- the assignment is the unit

    assert scheduler.assigned == [LOCAL_STORE_ID]


class BusyLocalScheduler(AssignScheduler):
    """The local slot is full for 100 s, and a delegation's round trip is 0.3 s."""

    def __init__(self, queue: BuildQueue, *, measured: bool) -> None:
        super().__init__(queue)
        self.measured = measured

    async def _transfer_seconds(self, *args: Any) -> dict[StoreId, float]:
        build: QueuedBuild = args[0]
        if self.measured:
            build.inputs_measured.add(REMOTE)
        return {REMOTE: 0.3}

    def _local_slot_is_full(self, *_args: Any) -> bool:
        return True

    def _local_wait_seconds(self) -> float | None:
        return 100.0


async def _assign_prefer_local(*, measured: bool) -> BusyLocalScheduler:
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1, {"preferLocalBuild": "1"}))
    scheduler = BusyLocalScheduler(queue, measured=measured)
    await Scheduler._assign_to_stores(cast("Any", scheduler), [queue.by_id[build_id]], {})  # noqa: SLF001 -- the assignment is the unit
    return scheduler


class TestAPreferLocalBuildNeverSentToTheStore:
    """dynhetz, 2026-10-22: home-manager's `man-paths` had built locally 11
    times and never on sg1-n3. Its input bytes there were unknown, and a
    round trip alone priced the delegation under a second. It went, and
    sent 9.3 GB in 648 s for a 0.3 s build."""

    @pytest.mark.anyio
    async def test_waits_while_its_inputs_there_are_unmeasured(self) -> None:
        scheduler = await _assign_prefer_local(measured=False)

        assert scheduler.assigned == []

    @pytest.mark.anyio
    async def test_goes_once_they_are_measured_and_the_wait_is_dearer(self) -> None:
        scheduler = await _assign_prefer_local(measured=True)

        assert scheduler.assigned == [REMOTE]


class ValidityStore:
    """A remote that holds *held*, or that does not answer."""

    def __init__(self, held: set[str], *, broken: bool = False) -> None:
        self.store_id = REMOTE
        self.held = held
        self.broken = broken

    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        if self.broken:
            raise ConnectionResetError("builder went away")
        return SimpleNamespace(paths={path for path in request.paths if str(path) in self.held})


SIZES = {StorePath("/nix/store/00000000000000000000000000000001-a"): 10 * MB}
SIZES[StorePath("/nix/store/00000000000000000000000000000002-b")] = 30 * MB


class TestTheMissingInputBytes:
    """The bytes a delegation sends are the input closure the remote lacks,
    as `QueryValidPaths` answers it."""

    @pytest.mark.anyio
    async def test_are_what_the_store_lacks(self) -> None:
        store = ValidityStore({"/nix/store/00000000000000000000000000000001-a"})

        missing = await Scheduler._missing_input_bytes(cast("Any", None), cast("Any", store), SIZES)  # noqa: SLF001 -- the estimate is the unit

        assert missing == 30 * MB

    @pytest.mark.anyio
    async def test_are_unknown_when_the_store_does_not_answer(self) -> None:
        store = ValidityStore(set(), broken=True)

        assert await Scheduler._missing_input_bytes(cast("Any", None), cast("Any", store), SIZES) is None  # noqa: SLF001 -- the estimate is the unit
