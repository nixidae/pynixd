"""Each placement decision is in the store database, with every term behind it.

nixidae/nixidae#1070.
"""

from __future__ import annotations

import sqlite3
import time
from contextlib import closing
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import aiosqlite
import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.allocator import BuildAllocator, TelemetryStoreRanker, score_store
from pynixd.build_stats import BuildEstimateSource, BuildObservation, DerivationIdentity, StoreEstimate, StoreLink
from pynixd.config import PlacementTraceSettings, PynixdSettings, StoreRankingSettings
from pynixd.db_migrations import PLACEMENT_CANDIDATE_TABLE, PLACEMENT_DECISION_TABLE
from pynixd.local_store_db import LocalStoreDB
from pynixd.placement_trace import CandidateTrace, Outcome, PlacementDecision, PlacementTrace
from pynixd.store_layout import StoreLayout
from tests.unit.test_store_choice import REMOTE, FakeBuild, FakeStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

MB = 1_000_000
HELLO = DerivationIdentity(name="hello", system="x86_64-linux", shape="0" * 16, keys=("name",), features=())


def _monitored(store_id: StoreId, *, utilization: float, cpu_psi: float = 0.0, io_psi: float = 0.0) -> FakeStore:
    store = FakeStore(store_id)
    store.cpu_util = SimpleNamespace(utilization=utilization)
    psi = SimpleNamespace(cpu=SimpleNamespace(some_avg10=cpu_psi), io=SimpleNamespace(some_avg10=io_psi))
    store.monitor = cast("Any", SimpleNamespace(health=SimpleNamespace(psi=psi, cores=None)))
    return store


class TestTheScore:
    def test_the_terms_add_up_to_the_final_score(self) -> None:
        store = _monitored(REMOTE, utilization=40.0, cpu_psi=1.0, io_psi=2.0)
        store.score_penalty = 5
        store.priority = 2.0

        terms = score_store(cast("Any", store), StoreRankingSettings(), 1, 1, 3.0)

        assert terms.cpu_idle_points == pytest.approx(60.0)
        # PSI is a percent: 1 % and 2 % of a 50-point penalty.
        assert terms.cpu_psi_points == pytest.approx(-0.5)
        assert terms.io_psi_points == pytest.approx(-1.0)
        assert terms.concurrency_points == pytest.approx(-50.0)
        assert terms.herd_points == pytest.approx(-100.0)
        assert terms.load_score == pytest.approx(60 - 0.5 - 1.0 - 50 - 100 - 5)
        assert terms.transfer_points == pytest.approx(-30.0)
        assert terms.final_score == pytest.approx((terms.load_score - 30.0) * 2.0)
        assert not terms.passed

    def test_an_unmonitored_store_passes_with_its_penalty(self) -> None:
        terms = score_store(cast("Any", FakeStore(REMOTE)), StoreRankingSettings(), 0, 0, None)

        assert terms.unmonitored_points == -1000.0
        assert terms.passed

    def test_a_store_under_the_threshold_is_left_out_with_its_terms(self) -> None:
        busy = _monitored(REMOTE, utilization=100.0, cpu_psi=50.0)
        ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=StoreRankingSettings())))

        ranked = ranker.rank_stores(cast("Any", FakeBuild()), [cast("Any", busy)], {})

        assert list(ranked) == []
        assert [(e.store_id, e.reason) for e in ranked.excluded] == [(REMOTE, "score under min_schedule_score")]
        assert ranked.excluded[0].breakdown is not None

    def test_the_allocator_names_why_a_store_is_no_candidate(self) -> None:
        local = FakeStore(LOCAL_STORE_ID)
        sick = FakeStore(StoreId("sick"))
        sick.is_healthy = False
        draining = FakeStore(StoreId("draining"))
        draining.draining = True
        stores = {LOCAL_STORE_ID: local, sick.store_id: sick, draining.store_id: draining}
        ranker = TelemetryStoreRanker(cast("Any", SimpleNamespace(ranking=StoreRankingSettings())))
        allocator = BuildAllocator(cast("Any", stores), cast("Any", local), ranker)

        ranked = allocator.rank_stores(cast("Any", FakeBuild()), {})

        assert [c.store_id for c in ranked] == [LOCAL_STORE_ID]
        assert {e.store_id: e.reason for e in ranked.excluded} == {"sick": "unhealthy", "draining": "draining"}


class TestTheEstimate:
    def test_each_phase_and_the_total(self) -> None:
        estimate = StoreEstimate(
            inputs_bytes=100 * MB,
            pull_bytes=40 * MB,
            link=StoreLink(send_bytes_per_s=10 * MB, receive_bytes_per_s=20 * MB, dispatch_ms=500),
            build_ms=30_000,
        )

        assert estimate.ship_seconds == pytest.approx(10.0)
        assert estimate.pull_seconds == pytest.approx(2.0)
        assert estimate.total_seconds(wait_seconds=4.0) == pytest.approx(4 + 10 + 0.5 + 30 + 2)

    def test_nothing_to_send_costs_nothing_on_an_unmeasured_link(self) -> None:
        assert StoreEstimate(inputs_bytes=0).ship_seconds == 0.0
        assert StoreEstimate(inputs_bytes=MB).ship_seconds is None


def _nix_database(tmp_path: Path) -> None:
    db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute("CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL)")


@pytest.fixture
async def db(tmp_path: Path) -> AsyncIterator[LocalStoreDB]:
    _nix_database(tmp_path)
    async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as opened:
        assert opened.schema.usable, opened.schema.reason
        yield opened


def _decision(
    build_id: str = "b1", *, outcome: Outcome = Outcome.ASSIGNED, at_ms: int | None = None
) -> PlacementDecision:
    decision = PlacementDecision(
        pass_id=7,
        build_id=build_id,
        attempt=0,
        drv_path="/nix/store/x-hello.drv",
        name="hello",
        system="x86_64-linux",
        shape="0" * 16,
        queued_ms=12,
        expected_build_ms=30_000,
        outcome=outcome,
        chosen_store_id=str(REMOTE) if outcome is Outcome.ASSIGNED else None,
        candidates=[
            CandidateTrace(
                store_id=str(REMOTE),
                rank=0,
                chosen=outcome is Outcome.ASSIGNED,
                roomy=True,
                breakdown=score_store(cast("Any", FakeStore(REMOTE)), StoreRankingSettings(), 0, 0, 2.0),
                estimate=StoreEstimate(
                    inputs_bytes=100 * MB,
                    link=StoreLink(send_bytes_per_s=10 * MB),
                    build_ms=30_000,
                    build_source=BuildEstimateSource.SHAPE_ON_STORE,
                ),
                slot_wait_seconds=0.0,
            ),
            CandidateTrace(store_id=str(LOCAL_STORE_ID), reason="short of disk"),
        ],
    )
    if at_ms is not None:
        decision.decided_at_ms = at_ms
    return decision


async def _rows(db: LocalStoreDB, query: str) -> list[aiosqlite.Row]:
    async with db.execute(query) as cursor:
        return list(await cursor.fetchall())


@pytest.mark.anyio
class TestTheTables:
    async def test_a_decision_holds_each_store_with_its_terms_and_estimate(self, db: LocalStoreDB) -> None:
        trace = PlacementTrace(db, PlacementTraceSettings(enabled=True))
        trace.record(_decision())

        assert await trace.flush() == 1

        decisions = await _rows(db, f"SELECT buildId, outcome, chosenStoreId, passId FROM {PLACEMENT_DECISION_TABLE}")
        assert [tuple(r) for r in decisions] == [("b1", "assigned", str(REMOTE), 7)]
        stores = await _rows(
            db,
            f"SELECT storeId, rank, reason, unmonitoredPoints, transferPoints, shipMs, buildMs, buildSource, totalMs"
            f" FROM {PLACEMENT_CANDIDATE_TABLE} ORDER BY storeId",
        )
        assert [tuple(r) for r in stores] == [
            (str(LOCAL_STORE_ID), None, "short of disk", None, None, None, None, None, None),
            (str(REMOTE), 0, None, -1000.0, -20.0, 10_000, 30_000, "shape-on-store", 40_000),
        ]

    async def test_the_observation_links_to_a_decision_still_in_the_buffer(self, db: LocalStoreDB) -> None:
        """A short build ends before the flush that writes its decision."""
        trace = PlacementTrace(db, PlacementTraceSettings(enabled=True))
        decision = _decision()
        trace.record(decision)
        observation_id = await db.record_build_observation(HELLO, BuildObservation(store_id=str(REMOTE), status=0))
        assert observation_id is not None
        trace.observed(decision, observation_id)

        await trace.flush()

        assert [tuple(r) for r in await _rows(db, f"SELECT observationId FROM {PLACEMENT_DECISION_TABLE}")] == [
            (observation_id,)
        ]

    async def test_the_sweep_takes_old_rows_in_chunks_and_keeps_young_ones(self, db: LocalStoreDB) -> None:
        settings = PlacementTraceSettings(enabled=True, max_age=3600.0, sweep_chunk=3)
        trace = PlacementTrace(db, settings)
        now_ms = int(time.time() * 1000)
        for index in range(7):
            trace.record(_decision(f"old{index}", at_ms=now_ms - 7200_000))
        for index in range(2):
            trace.record(_decision(f"new{index}", at_ms=now_ms))
        await trace.flush()

        assert await trace.sweep(now_ms) == 7

        young = await _rows(db, f"SELECT buildId FROM {PLACEMENT_DECISION_TABLE} ORDER BY id")
        assert [r[0] for r in young] == ["new0", "new1"]
        left = await _rows(db, f"SELECT count(*) FROM {PLACEMENT_CANDIDATE_TABLE}")
        assert left[0][0] == 4

    async def test_a_full_buffer_drops_and_counts(self, db: LocalStoreDB) -> None:
        trace = PlacementTrace(db, PlacementTraceSettings(enabled=True, buffer_limit=2))
        for index in range(5):
            trace.record(_decision(f"b{index}"))

        assert trace.dropped == 3
        assert await trace.flush() == 2

    async def test_a_disabled_trace_records_nothing(self, db: LocalStoreDB) -> None:
        trace = PlacementTrace(db, PlacementTraceSettings(enabled=False))
        trace.record(_decision())

        assert await trace.flush() == 0


class TestARepeat:
    def test_a_waiting_build_repeats_only_after_the_interval(self) -> None:
        trace = PlacementTrace(None, PlacementTraceSettings(repeat_interval=30.0))
        waiting = _decision(outcome=Outcome.WAITING)
        waiting.reason = "every candidate is at its build limit"

        assert trace.due(None, waiting)
        assert not trace.due((waiting.key, time.monotonic()), waiting)
        assert trace.due((waiting.key, time.monotonic() - 31), waiting)

    def test_a_new_reason_is_due_at_once(self) -> None:
        trace = PlacementTrace(None, PlacementTraceSettings())
        before = _decision(outcome=Outcome.WAITING)
        after = _decision(outcome=Outcome.WAITING)
        after.reason = "no store is a candidate now"

        assert trace.due((before.key, time.monotonic()), after)


class TestTheSetting:
    def test_off_by_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PYNIXD_PLACEMENT_TRACE", raising=False)

        assert not PynixdSettings().placement_trace.enabled

    def test_the_environment_turns_it_on(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The test session turns it on this way (`tests/_conftest/config.py`)."""
        monkeypatch.setenv("PYNIXD_PLACEMENT_TRACE", '{"enabled": true}')

        assert PynixdSettings().placement_trace.enabled
