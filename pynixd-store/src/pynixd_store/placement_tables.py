"""pynixd's placement trace tables in the store database: `PynixdPlacementDecision` and `PynixdPlacementCandidate`.

The scheduler's trace (`pynixd/placement_trace.py`) builds the rows; this
module holds the SQL and the transactions. A caller never holds a connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from .db_migrations import PLACEMENT_CANDIDATE_TABLE, PLACEMENT_DECISION_TABLE

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .local_store_db import LocalStoreDB

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


class Decision(Protocol):
    """A decision the trace holds: the write sets its row id, and an observation reads it."""

    row_id: int | None


@dataclass(frozen=True, slots=True)
class DecisionWrite:
    """One decision row, and its candidate rows without their leading `decisionId`."""

    decision: Decision
    row: tuple[object, ...]
    candidates: list[tuple[object, ...]]


@dataclass(frozen=True, slots=True)
class ObservationWrite:
    """The link of a decision to the observation of its build; nothing when the decision has no row."""

    decision: Decision
    observation_id: int


class PlacementTables:
    def __init__(self, db: LocalStoreDB) -> None:
        self._db = db

    async def write(self, writes: Sequence[DecisionWrite | ObservationWrite]) -> int:
        """Write *writes* in order, in one transaction; answer the decisions written.

        A decision's row id is set as it is inserted, so a link later in the
        same list finds it. A refused transaction raises `DatabaseError`, and
        the caller clears the ids it set.
        """
        written = 0
        async with self._db.acquire_conn() as conn:
            candidates: list[tuple[object, ...]] = []
            for item in writes:
                if isinstance(item, ObservationWrite):
                    if item.decision.row_id is not None:
                        await conn.execute(SET_OBSERVATION, (item.observation_id, item.decision.row_id))
                    continue
                cursor = await conn.execute(INSERT_DECISION, item.row)
                row_id = item.decision.row_id = cursor.lastrowid
                if row_id is not None:
                    candidates += [(row_id, *candidate) for candidate in item.candidates]
                written += 1
            await conn.executemany(INSERT_CANDIDATE, candidates)
            await conn.commit()
        return written

    async def sweep_chunk(self, cutoff_ms: int, chunk: int) -> int:
        """Delete the oldest decisions before *cutoff_ms*, at most *chunk*, with their candidates; answer how many."""
        async with self._db.acquire_conn() as conn:
            async with conn.execute(SWEEP_CHUNK_END, (cutoff_ms, chunk)) as cursor:
                row = await cursor.fetchone()
            if row is None or row[0] is None:
                return 0
            end, count = int(row[0]), int(row[1])
            await conn.execute(SWEEP_CANDIDATES, (end,))
            await conn.execute(SWEEP_DECISIONS, (end,))
            await conn.commit()
        return count
