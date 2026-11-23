"""The builder machines the provisioner started, in the store database: `PynixdProvisionedInstance`. nixidae/nixidae#1198.

`pynixd/provisioning/` decides and calls the provider; this module holds the
transactions, and `sql/provisioning.py` the statements. A row is written
before the provider is asked, so a pynixd that dies between the two finds the
row at its next start and ends the machine. A join token is stored as its
SHA-256 only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .local_store_db import LocalStoreDB

PENDING = "pending"
RUNNING = "running"
ENDED = "ended"


@dataclass(frozen=True, slots=True)
class InstanceRow:
    id: str
    pool: str
    provider: str
    provider_id: str | None
    """The provider's own name for the machine, once it answered the launch."""
    state: str
    token_sha256: str
    launched_at_ms: int
    registered_at_ms: int | None
    store_id: str | None

    @classmethod
    def from_row(cls, row: Sequence[object]) -> InstanceRow:
        id_, pool, provider, provider_id, state, token, launched, registered, store = row
        return cls(
            id=str(id_),
            pool=str(pool),
            provider=str(provider),
            provider_id=None if provider_id is None else str(provider_id),
            state=str(state),
            token_sha256=str(token),
            launched_at_ms=int(str(launched)),
            registered_at_ms=None if registered is None else int(str(registered)),
            store_id=None if store is None else str(store),
        )


class ProvisioningTables:
    def __init__(self, db: LocalStoreDB) -> None:
        self._db = db

    async def record_launch(self, id_: str, pool: str, provider: str, token_sha256: str, at_ms: int) -> None:
        async with self._db.transaction() as session:
            await session.execute(session.sql.provisioning.insert, (id_, pool, provider, PENDING, token_sha256, at_ms))

    async def set_provider_id(self, id_: str, provider_id: str) -> None:
        async with self._db.transaction() as session:
            await session.execute(session.sql.provisioning.set_provider_id, (provider_id, id_))

    async def registered(self, id_: str, store_id: str, at_ms: int) -> bool:
        """Mark *id_* running under *store_id*; False when it is ended, unknown or registered already.

        A join token admits one registration: a builder that reconnects is
        refused, and the provisioner ends its instance.
        """
        async with self._db.transaction() as session:
            changed = await session.execute(session.sql.provisioning.set_registered, (RUNNING, at_ms, store_id, id_))
        return changed > 0

    async def ended(self, id_: str, reason: str, at_ms: int) -> None:
        async with self._db.transaction() as session:
            await session.execute(session.sql.provisioning.set_ended, (ENDED, at_ms, reason, id_))

    async def live(self) -> list[InstanceRow]:
        """Every instance not ended, oldest first.

        A plain read, not a write transaction: the write lock waits behind
        every writer of `db.sqlite`, and under a burst of builds it fails at
        the 30 s busy timeout (nixidae/nixidae#1582).
        """
        async with self._db.connection() as session:
            rows = await session.fetchall(session.sql.provisioning.live)
        return [InstanceRow.from_row(row) for row in rows]

    async def by_token(self, token_sha256: str) -> InstanceRow | None:
        """The live instance whose join token hashes to *token_sha256*."""
        async with self._db.connection() as session:
            row = await session.fetchone(session.sql.provisioning.live_by_token, (token_sha256,))
        return None if row is None else InstanceRow.from_row(row)
