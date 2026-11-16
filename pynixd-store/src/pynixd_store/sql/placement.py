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


POSTGRES = PlacementSql(
    insert_decision=f"""
INSERT INTO {PLACEMENT_DECISION_TABLE}
(decidedAtMs, passId, buildId, attempt, drvPath, name, system, shape, outcome, reason, chosenStoreId,
 queuedMs, expectedBuildMs, preferLocal, localCanBuild, localSlotFull, localWaitMs, weights)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
RETURNING id
""",
    # SQLite's `INSERT OR REPLACE`: the row of a second write takes every column of the new one.
    insert_candidate=f"""
INSERT INTO {PLACEMENT_CANDIDATE_TABLE}
(decisionId, storeId, rank, reason, chosen, roomy, failedHere,
 cpuUtilization, cpuIdlePoints, cpuPsi, cpuPsiPoints, ioPsi, ioPsiPoints, unmonitoredPoints,
 inFlight, concurrencyPoints, assignedThisPass, herdPoints, scorePenalty, loadScore, threshold, passed,
 transferSeconds, transferPoints, priority, finalScore,
 inputsBytes, pullBytes, sendBytesPerS, receiveBytesPerS, dispatchMs, buildMs, buildSource,
 shipMs, pullMs, slotWaitMs, totalMs)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (decisionId, storeId) DO UPDATE SET
 rank = excluded.rank, reason = excluded.reason, chosen = excluded.chosen, roomy = excluded.roomy,
 failedHere = excluded.failedHere, cpuUtilization = excluded.cpuUtilization,
 cpuIdlePoints = excluded.cpuIdlePoints, cpuPsi = excluded.cpuPsi, cpuPsiPoints = excluded.cpuPsiPoints,
 ioPsi = excluded.ioPsi, ioPsiPoints = excluded.ioPsiPoints, unmonitoredPoints = excluded.unmonitoredPoints,
 inFlight = excluded.inFlight, concurrencyPoints = excluded.concurrencyPoints,
 assignedThisPass = excluded.assignedThisPass, herdPoints = excluded.herdPoints,
 scorePenalty = excluded.scorePenalty, loadScore = excluded.loadScore, threshold = excluded.threshold,
 passed = excluded.passed, transferSeconds = excluded.transferSeconds, transferPoints = excluded.transferPoints,
 priority = excluded.priority, finalScore = excluded.finalScore, inputsBytes = excluded.inputsBytes,
 pullBytes = excluded.pullBytes, sendBytesPerS = excluded.sendBytesPerS,
 receiveBytesPerS = excluded.receiveBytesPerS, dispatchMs = excluded.dispatchMs, buildMs = excluded.buildMs,
 buildSource = excluded.buildSource, shipMs = excluded.shipMs, pullMs = excluded.pullMs,
 slotWaitMs = excluded.slotWaitMs, totalMs = excluded.totalMs
""",
    set_observation=f"UPDATE {PLACEMENT_DECISION_TABLE} SET observationId = %s WHERE id = %s",
    sweep_chunk_end=f"""
SELECT max(id), count(*) FROM (
    SELECT id FROM {PLACEMENT_DECISION_TABLE} WHERE decidedAtMs < %s ORDER BY id LIMIT %s
) AS chunk
""",
    sweep_candidates=f"DELETE FROM {PLACEMENT_CANDIDATE_TABLE} WHERE decisionId <= %s",
    sweep_decisions=f"DELETE FROM {PLACEMENT_DECISION_TABLE} WHERE id <= %s",
)
