"""pynixd's placement trace tables in the store database: `PynixdPlacementDecision` and `PynixdPlacementCandidate`.

The scheduler's trace (`pynixd/placement_trace.py`) builds the rows; this
module holds the transactions, and `sql/placement.py` the statements. A caller
never holds a connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .local_store_db import LocalStoreDB


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
        async with self._db.transaction() as session:
            candidates: list[tuple[object, ...]] = []
            for item in writes:
                if isinstance(item, ObservationWrite):
                    if item.decision.row_id is not None:
                        await session.execute(
                            session.sql.placement.set_observation, (item.observation_id, item.decision.row_id)
                        )
                    continue
                row_id = item.decision.row_id = await session.insert(session.sql.placement.insert_decision, item.row)
                candidates += [(row_id, *candidate) for candidate in item.candidates]
                written += 1
            await session.executemany(session.sql.placement.insert_candidate, candidates)
        return written

    async def sweep_chunk(self, cutoff_ms: int, chunk: int) -> int:
        """Delete the oldest decisions before *cutoff_ms*, at most *chunk*, with their candidates; answer how many."""
        async with self._db.transaction() as session:
            row = await session.fetchone(session.sql.placement.sweep_chunk_end, (cutoff_ms, chunk))
            if row is None or row[0] is None:
                return 0
            end, count = int(row[0]), int(row[1])
            await session.execute(session.sql.placement.sweep_candidates, (end,))
            await session.execute(session.sql.placement.sweep_decisions, (end,))
        return count
