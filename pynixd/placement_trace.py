"""A record of each placement decision, in pynixd's tables of the store database.

Each decision names every store the scheduler weighed and every store it left
out, with each term of each score, the reason a store was no candidate, and
what each phase of the build was expected to cost there. After the build,
`observationId` points at what the build cost, in `PynixdBuildObservation`.
nixidae/nixidae#1070.

A scheduling pass never waits on SQLite: it adds to a buffer, and `run`
writes the buffer in one transaction per flush. `run` also deletes the rows
older than `max_age`, the oldest first, in chunks of one transaction each: a
long delete would hold the write lock of the store database that nix-daemon
shares.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING

import aiosqlite
import anyio
import structlog

from .db_migrations import PLACEMENT_CANDIDATE_TABLE, PLACEMENT_DECISION_TABLE

if TYPE_CHECKING:
    from .allocator import ScoreBreakdown
    from .build_stats import StoreEstimate
    from .config import PlacementTraceSettings
    from .local_store_db import LocalStoreDB

log = structlog.get_logger(__name__)


class Outcome(StrEnum):
    ASSIGNED = "assigned"
    WAITING = "waiting"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CandidateTrace:
    """One store as one decision saw it."""

    store_id: str
    rank: int | None = None
    """The position in the final order, 0 first; `None` for a store left out."""
    reason: str | None = None
    """Why the store was left out, or a note on a candidate."""
    chosen: bool = False
    roomy: bool | None = None
    """Whether the store had a free build slot; `None` where nobody asked."""
    failed_here: int = 0
    breakdown: ScoreBreakdown | None = None
    estimate: StoreEstimate | None = None
    slot_wait_seconds: float | None = None


@dataclass
class PlacementDecision:
    """One decision of `Scheduler._assign_one` for one build."""

    pass_id: int
    build_id: str
    attempt: int
    drv_path: str
    name: str
    system: str
    shape: str
    queued_ms: int
    expected_build_ms: int | None
    prefer_local: bool = False
    local_can_build: bool = False
    local_slot_full: bool | None = None
    local_wait_seconds: float | None = None
    weights: str = "{}"
    """The ranking settings as JSON: the multiplier behind each score term."""
    outcome: Outcome = Outcome.WAITING
    reason: str | None = None
    chosen_store_id: str | None = None
    candidates: list[CandidateTrace] = field(default_factory=list)
    decided_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    row_id: int | None = None
    """The row this decision got; the flush sets it."""

    @property
    def key(self) -> tuple[Outcome, str | None, str | None]:
        """What a repeat of the decision must change to be written at once."""
        return (self.outcome, self.reason, self.chosen_store_id)


@dataclass(frozen=True)
class _Observed:
    decision: PlacementDecision
    observation_id: int


INSERT_DECISION = f"""
INSERT INTO {PLACEMENT_DECISION_TABLE}
(decidedAtMs, passId, buildId, attempt, drvPath, name, system, shape, outcome, reason, chosenStoreId,
 queuedMs, expectedBuildMs, preferLocal, localCanBuild, localSlotFull, localWaitMs, weights)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

INSERT_CANDIDATE = f"""
INSERT OR REPLACE INTO {PLACEMENT_CANDIDATE_TABLE}
(decisionId, storeId, rank, reason, chosen, roomy, failedHere,
 cpuUtilization, cpuIdlePoints, cpuPsi, cpuPsiPoints, ioPsi, ioPsiPoints, unmonitoredPoints,
 inFlight, concurrencyPoints, assignedThisPass, herdPoints, scorePenalty, loadScore, threshold, passed,
 transferSeconds, transferPoints, priority, finalScore,
 inputsBytes, pullBytes, sendBytesPerS, receiveBytesPerS, dispatchMs, buildMs, buildSource,
 shipMs, pullMs, slotWaitMs, totalMs)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

SET_OBSERVATION = f"UPDATE {PLACEMENT_DECISION_TABLE} SET observationId = ? WHERE id = ?"

# The oldest rows first: ids grow with time, so the scan stops at the chunk.
SWEEP_CHUNK_END = f"""
SELECT max(id), count(*) FROM (
    SELECT id FROM {PLACEMENT_DECISION_TABLE} WHERE decidedAtMs < ? ORDER BY id LIMIT ?
)
"""
SWEEP_CANDIDATES = f"DELETE FROM {PLACEMENT_CANDIDATE_TABLE} WHERE decisionId <= ?"
SWEEP_DECISIONS = f"DELETE FROM {PLACEMENT_DECISION_TABLE} WHERE id <= ?"


def _ms(seconds: float | None) -> int | None:
    return round(seconds * 1000) if seconds is not None else None


def _flag(value: bool | None) -> int | None:
    return int(value) if value is not None else None


def decision_row(decision: PlacementDecision) -> tuple[object, ...]:
    return (
        decision.decided_at_ms,
        decision.pass_id,
        decision.build_id,
        decision.attempt,
        decision.drv_path,
        decision.name,
        decision.system,
        decision.shape,
        str(decision.outcome),
        decision.reason,
        decision.chosen_store_id,
        decision.queued_ms,
        decision.expected_build_ms,
        int(decision.prefer_local),
        int(decision.local_can_build),
        _flag(decision.local_slot_full),
        _ms(decision.local_wait_seconds),
        decision.weights,
    )


def candidate_row(decision_id: int, candidate: CandidateTrace) -> tuple[object, ...]:
    terms = candidate.breakdown
    estimate = candidate.estimate
    link = estimate.link if estimate is not None else None
    return (
        decision_id,
        candidate.store_id,
        candidate.rank,
        candidate.reason,
        int(candidate.chosen),
        _flag(candidate.roomy),
        candidate.failed_here,
        *(
            (
                terms.cpu_utilization,
                terms.cpu_idle_points,
                terms.cpu_psi,
                terms.cpu_psi_points,
                terms.io_psi,
                terms.io_psi_points,
                terms.unmonitored_points,
                terms.in_flight,
                terms.concurrency_points,
                terms.assigned_this_pass,
                terms.herd_points,
                terms.score_penalty,
                terms.load_score,
                terms.threshold,
                int(terms.passed),
                terms.transfer_seconds,
                terms.transfer_points,
                terms.priority,
                terms.final_score,
            )
            if terms is not None
            else (None,) * 19
        ),
        estimate.inputs_bytes if estimate is not None else None,
        estimate.pull_bytes if estimate is not None else None,
        link.send_bytes_per_s if link is not None else None,
        link.receive_bytes_per_s if link is not None else None,
        link.dispatch_ms if link is not None else None,
        estimate.build_ms if estimate is not None else None,
        str(estimate.build_source) if estimate is not None and estimate.build_source is not None else None,
        _ms(estimate.ship_seconds) if estimate is not None else None,
        _ms(estimate.pull_seconds) if estimate is not None else None,
        _ms(candidate.slot_wait_seconds),
        _ms(estimate.total_seconds(candidate.slot_wait_seconds)) if estimate is not None else None,
    )


class PlacementTrace:
    """The buffer of decisions, and the loop that writes and sweeps it."""

    def __init__(self, db: LocalStoreDB | None, settings: PlacementTraceSettings) -> None:
        self.db = db
        self.settings = settings
        self._buffer: list[PlacementDecision | _Observed] = []
        self.dropped = 0
        """Decisions the full buffer refused since the last flush."""
        self._last_sweep = time.monotonic()

    @property
    def enabled(self) -> bool:
        db = self.db
        return self.settings.enabled and db is not None and db.active and not db.read_only and db.schema.usable

    def due(self, previous: tuple[tuple[object, ...], float] | None, decision: PlacementDecision) -> bool:
        """Whether *decision* differs from *previous*, or repeats it after `repeat_interval`."""
        if previous is None or decision.outcome is not Outcome.WAITING:
            return True
        key, at = previous
        return key != decision.key or time.monotonic() - at >= self.settings.repeat_interval

    def record(self, decision: PlacementDecision) -> None:
        if not self.enabled:
            return
        if len(self._buffer) >= self.settings.buffer_limit:
            self.dropped += 1
            return
        self._buffer.append(decision)

    def observed(self, decision: PlacementDecision, observation_id: int) -> None:
        """Link *decision* to the observation of its build.

        Through the buffer: a build can end before the flush that writes
        its decision, and the link must follow the insert.
        """
        if self.enabled:
            self._buffer.append(_Observed(decision, observation_id))

    async def flush(self) -> int:
        """Write the buffer in one transaction; answer the decisions written."""
        db = self.db
        if not self._buffer or db is None or not self.enabled:
            return 0
        pending, self._buffer = self._buffer, []
        dropped, self.dropped = self.dropped, 0
        if dropped:
            log.warning("placement_trace_dropped", dropped=dropped, limit=self.settings.buffer_limit)
        written = 0
        try:
            async with db.acquire_conn() as conn:
                candidates: list[tuple[object, ...]] = []
                for item in pending:
                    if isinstance(item, _Observed):
                        if item.decision.row_id is not None:
                            await conn.execute(SET_OBSERVATION, (item.observation_id, item.decision.row_id))
                        continue
                    cursor = await conn.execute(INSERT_DECISION, decision_row(item))
                    item.row_id = cursor.lastrowid
                    if item.row_id is not None:
                        candidates += [candidate_row(item.row_id, c) for c in item.candidates]
                    written += 1
                await conn.executemany(INSERT_CANDIDATE, candidates)
                await conn.commit()
        except aiosqlite.Error:
            log.warning("placement_trace_flush_failed", decisions=written, exc_info=True)
            for item in pending:
                if isinstance(item, PlacementDecision):
                    item.row_id = None
            return 0
        return written

    async def sweep(self, now_ms: int | None = None) -> int:
        """Delete the decisions older than `max_age`, chunk by chunk; answer how many."""
        db = self.db
        if db is None or not self.enabled:
            return 0
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        cutoff = now_ms - int(self.settings.max_age * 1000)
        removed = 0
        try:
            while True:
                async with db.acquire_conn() as conn:
                    async with conn.execute(SWEEP_CHUNK_END, (cutoff, self.settings.sweep_chunk)) as cursor:
                        row = await cursor.fetchone()
                    if row is None or row[0] is None:
                        break
                    end, count = int(row[0]), int(row[1])
                    await conn.execute(SWEEP_CANDIDATES, (end,))
                    await conn.execute(SWEEP_DECISIONS, (end,))
                    await conn.commit()
                removed += count
                if count < self.settings.sweep_chunk:
                    break
                # The daemon's own writers get the lock between two chunks.
                await anyio.sleep(0)
        except aiosqlite.Error:
            log.warning("placement_trace_sweep_failed", removed=removed, exc_info=True)
        if removed:
            log.debug("placement_trace_swept", removed=removed)
        return removed

    async def run(self) -> None:
        """Flush on each `flush_interval`, and sweep on each `sweep_interval`, until cancelled."""
        try:
            while True:
                await anyio.sleep(self.settings.flush_interval)
                await self.flush()
                if time.monotonic() - self._last_sweep >= self.settings.sweep_interval:
                    self._last_sweep = time.monotonic()
                    await self.sweep()
        finally:
            with anyio.CancelScope(shield=True):
                await self.flush()
