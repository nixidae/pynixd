"""The statements over the scheduler's placement trace: `PynixdPlacementDecision` and `PynixdPlacementCandidate`."""

from __future__ import annotations

from dataclasses import dataclass

from ..db_migrations import PLACEMENT_CANDIDATE_TABLE, PLACEMENT_DECISION_TABLE


@dataclass(frozen=True, slots=True)
class PlacementSql:
    insert_decision: str
    insert_candidate: str
    set_observation: str
    sweep_chunk_end: str
    """The oldest rows first: ids grow with time, so the scan stops at the chunk."""
    sweep_candidates: str
    sweep_decisions: str


SQLITE = PlacementSql(
    insert_decision=f"""
INSERT INTO {PLACEMENT_DECISION_TABLE}
(decidedAtMs, passId, buildId, attempt, drvPath, name, system, shape, outcome, reason, chosenStoreId,
 queuedMs, expectedBuildMs, preferLocal, localCanBuild, localSlotFull, localWaitMs, weights)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
    insert_candidate=f"""
INSERT OR REPLACE INTO {PLACEMENT_CANDIDATE_TABLE}
(decisionId, storeId, rank, reason, chosen, roomy, failedHere,
 cpuUtilization, cpuIdlePoints, cpuPsi, cpuPsiPoints, ioPsi, ioPsiPoints, unmonitoredPoints,
 inFlight, concurrencyPoints, assignedThisPass, herdPoints, scorePenalty, loadScore, threshold, passed,
 transferSeconds, transferPoints, priority, finalScore,
 inputsBytes, pullBytes, sendBytesPerS, receiveBytesPerS, dispatchMs, buildMs, buildSource,
 shipMs, pullMs, slotWaitMs, totalMs)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
    set_observation=f"UPDATE {PLACEMENT_DECISION_TABLE} SET observationId = ? WHERE id = ?",
    sweep_chunk_end=f"""
SELECT max(id), count(*) FROM (
    SELECT id FROM {PLACEMENT_DECISION_TABLE} WHERE decidedAtMs < ? ORDER BY id LIMIT ?
)
""",
    sweep_candidates=f"DELETE FROM {PLACEMENT_CANDIDATE_TABLE} WHERE decisionId <= ?",
    sweep_decisions=f"DELETE FROM {PLACEMENT_DECISION_TABLE} WHERE id <= ?",
)
