"""The statements over `PynixdProvisionedInstance`, the builder machines the provisioner started. nixidae/nixidae#1198."""

from __future__ import annotations

from dataclasses import dataclass

from ..db_migrations import PROVISIONED_INSTANCE_TABLE as T

_LIVE_COLUMNS = "id, pool, provider, providerId, state, tokenSha256, launchedAtMs, registeredAtMs, storeId"


@dataclass(frozen=True, slots=True)
class ProvisioningSql:
    insert: str
    set_provider_id: str
    set_registered: str
    set_ended: str
    live: str
    """Every row not ended, oldest first."""
    live_by_token: str


SQLITE = ProvisioningSql(
    insert=f"INSERT INTO {T} (id, pool, provider, state, tokenSha256, launchedAtMs) VALUES (?, ?, ?, ?, ?, ?)",
    set_provider_id=f"UPDATE {T} SET providerId = ? WHERE id = ?",
    set_registered=f"UPDATE {T} SET state = ?, registeredAtMs = ?, storeId = ? WHERE id = ? AND endedAtMs IS NULL",
    set_ended=f"UPDATE {T} SET state = ?, endedAtMs = ?, endReason = ? WHERE id = ? AND endedAtMs IS NULL",
    live=f"SELECT {_LIVE_COLUMNS} FROM {T} WHERE endedAtMs IS NULL ORDER BY launchedAtMs, id",
    live_by_token=f"SELECT {_LIVE_COLUMNS} FROM {T} WHERE tokenSha256 = ? AND endedAtMs IS NULL",
)

POSTGRES = ProvisioningSql(
    insert=f"INSERT INTO {T} (id, pool, provider, state, tokenSha256, launchedAtMs) VALUES (%s, %s, %s, %s, %s, %s)",
    set_provider_id=f"UPDATE {T} SET providerId = %s WHERE id = %s",
    set_registered=f"UPDATE {T} SET state = %s, registeredAtMs = %s, storeId = %s WHERE id = %s AND endedAtMs IS NULL",
    set_ended=f"UPDATE {T} SET state = %s, endedAtMs = %s, endReason = %s WHERE id = %s AND endedAtMs IS NULL",
    live=f"SELECT {_LIVE_COLUMNS} FROM {T} WHERE endedAtMs IS NULL ORDER BY launchedAtMs, id",
    live_by_token=f"SELECT {_LIVE_COLUMNS} FROM {T} WHERE tokenSha256 = %s AND endedAtMs IS NULL",
)
