"""Where a build goes: the local store ranks with the others, and delegation has a measured cost.

nixidae/nixidae#1001, and `preferLocalBuild` from nixidae/nixidae#947.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, BuildId, StoreId
from pynixd.allocator import BuildAllocator, PreferLocal, RankedStore, TelemetryStoreRanker, pick_store
from pynixd.build_queue import BuildQueue
from pynixd.build_stats import ExpectedBytes, StoreLink, slot_wait_seconds, transfer_seconds
from pynixd.config import StoreRankingSettings
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
    def test_adds_both_directions_and_the_round_trip(self):
        link = StoreLink(send_bytes_per_s=10 * MB, receive_bytes_per_s=20 * MB, dispatch_ms=500)

        assert transfer_seconds(ExpectedBytes(inputs=100 * MB, pull=40 * MB), link) == pytest.approx(12.5)

    def test_an_unmeasured_store_has_no_cost(self):
        assert transfer_seconds(ExpectedBytes(inputs=100 * MB), StoreLink()) is None

    def test_a_known_round_trip_alone_is_a_cost(self):
        """A tiny build whose bytes were never measured still pays the dispatch."""
        assert transfer_seconds(ExpectedBytes(), StoreLink(dispatch_ms=300)) == pytest.approx(0.3)


class TestTheLocalSlotWait:
    def test_is_the_soonest_end(self):
        assert slot_wait_seconds([(10_000, 100.0), (60_000, 100.0)], now=104.0) == pytest.approx(6.0)

    def test_a_build_past_its_estimate_ends_now(self):
        assert slot_wait_seconds([(1_000, 100.0)], now=200.0) == 0.0

    def test_no_estimate_is_no_answer(self):
        assert slot_wait_seconds([(None, 100.0)], now=101.0) is None


class TestTheRanking:
    def test_the_local_store_is_a_candidate(self):
        assert LOCAL_STORE_ID in _rank()

    def test_a_remote_wins_a_tie(self):
        """The local store was only a fallback, and an unmeasured world ranks as it did."""
        assert _rank() == [REMOTE, LOCAL_STORE_ID]

    def test_a_measured_transfer_puts_the_local_store_first(self):
        assert _rank({REMOTE: 5.0}) == [LOCAL_STORE_ID, REMOTE]


class TestAStoreNobodyCanMonitor:
    """A builder that refuses SFTP and exec still builds, after every store
    pynixd can see. nixidae/nixidae#1016."""

    @staticmethod
    def _rank(*, busy: int, min_score: float = -10_000.0) -> list[StoreId]:
        local = FakeStore(LOCAL_STORE_ID)
        local.no_schedule = True
        seen = FakeStore(OTHER)
        seen.cpu_util = SimpleNamespace(utilization=100.0)
        seen.in_flight = busy
        stores = {LOCAL_STORE_ID: local, REMOTE: FakeStore(REMOTE), OTHER: seen}
        ranker = TelemetryStoreRanker(
            cast("Any", SimpleNamespace(ranking=StoreRankingSettings(min_schedule_score=min_score)))
        )
        ranked = BuildAllocator(cast("Any", stores), cast("Any", local), ranker).rank_stores(
            cast("Any", FakeBuild()), {}
        )
        return [candidate.store_id for candidate in ranked]

    def test_ranks_under_a_fully_busy_monitored_store(self):
        assert self._rank(busy=3) == [OTHER, REMOTE]

    def test_still_gets_builds_under_the_minimum_score(self):
        """The monitored store falls under the minimum; the blind one stays."""
        assert self._rank(busy=3, min_score=-100.0) == [REMOTE]


class TestThePick:
    def test_takes_the_best_ranked(self):
        picked = pick_store(_ranked(REMOTE, LOCAL_STORE_ID), LOCAL_STORE_ID, local_slot_full=False)

        assert picked is not None
        assert picked.store_id == REMOTE

    def test_a_full_local_slot_passes_to_the_next_store(self):
        picked = pick_store(_ranked(LOCAL_STORE_ID, REMOTE), LOCAL_STORE_ID, local_slot_full=True)

        assert picked is not None
        assert picked.store_id == REMOTE


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

    def test_a_free_local_slot_wins_over_a_better_ranked_remote(self):
        picked = pick_store(
            _ranked(REMOTE, LOCAL_STORE_ID),
            LOCAL_STORE_ID,
            local_slot_full=False,
            prefer_local=PreferLocal(wait_seconds=None, transfer_seconds={}, weight=10.0),
        )

        assert picked is not None
        assert picked.store_id == LOCAL_STORE_ID

    def test_waits_while_the_wait_is_cheaper_than_the_weighted_delegation(self):
        assert self._pick(wait=19.0, remote_seconds=2.0) is None

    def test_delegates_once_the_wait_passes_the_weighted_delegation(self):
        assert self._pick(wait=21.0, remote_seconds=2.0) == REMOTE

    def test_waits_when_the_wait_is_unknown(self):
        """Nix waits for a local slot however long; with no estimate, so does pynixd."""
        assert self._pick(wait=None, remote_seconds=2.0) is None

    def test_waits_when_the_delegation_is_unmeasured(self):
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
        self.stores: dict[StoreId, object] = {}
        self.assigned: list[StoreId] = []

    _assign_one = Scheduler._assign_one  # noqa: SLF001 -- the assignment is the unit
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
    await Scheduler._assign_to_stores(cast("Any", scheduler), [queue.by_id[build_id]], {}, {})  # noqa: SLF001 -- the assignment is the unit
    return scheduler


class TestALocalStoreUnderPressure:
    """Measured on dynhetz, 2026-10-21: PSI cpu some at 71 % put the local
    store under `min_schedule_score`. A `preferLocalBuild` derivation then
    waited for a local slot that no running build would free, and the
    deployed pynixd hung every `writeText`."""

    @pytest.mark.anyio
    async def test_still_takes_a_prefer_local_build(self):
        """Nix ignores load for `preferLocalBuild`: a free slot is enough."""
        scheduler = await _assign({"preferLocalBuild": "1"})

        assert scheduler.assigned == [LOCAL_STORE_ID]

    @pytest.mark.anyio
    async def test_leaves_other_builds_to_the_ranking(self):
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
    async def test_fails_alone_and_the_pass_goes_on(self):
        queue = BuildQueue()
        broken, broken_future = await queue.enqueue(_request(1))
        healthy, _ = await queue.enqueue(_request(2))
        scheduler = BrokenScheduler(queue, broken)

        await Scheduler._assign_to_stores(  # noqa: SLF001 -- the assignment is the unit
            cast("Any", scheduler), [queue.by_id[broken], queue.by_id[healthy]], {}, {}
        )

        assert scheduler.assigned == [REMOTE]
        assert queue.by_id[healthy].is_building
        assert "an estimate that raises" in str(broken_future.result().result.error_msg)


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
async def test_a_retry_goes_to_a_store_the_build_has_not_failed_on():
    """sg1-n1 failed one missing input three times running. nixidae/nixidae#1015."""
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(_request(1))
    queue.by_id[build_id].reset_for_retry(REMOTE, "getting status of ...: No such file or directory")
    scheduler = TwoRemotesScheduler(queue)

    await Scheduler._assign_to_stores(cast("Any", scheduler), [queue.by_id[build_id]], {}, {})  # noqa: SLF001 -- the assignment is the unit

    assert scheduler.assigned == [OTHER]
