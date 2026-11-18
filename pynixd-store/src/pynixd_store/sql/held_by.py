"""The statements over `PynixdHeldBy`: the paths a cold tier or a stable substituter holds. nixidae/nixidae#1417."""

from __future__ import annotations

from dataclasses import dataclass

from ..db_migrations import HELD_BY_TABLE as T


@dataclass(frozen=True, slots=True)
class HeldBySql:
    record: str
    """`store`, `paths` (a path set), `at`: one row per path; a second answer moves its time."""
    held: str
    """`paths`, `stores` (path sets): the paths that any of the stores holds."""
    forget: str
    """`store`, `paths` (a path set): the rows of paths the store no longer holds."""


SQLITE = HeldBySql(
    record=f"""
INSERT INTO {T} (store, path, recordedAt) SELECT :store, value, :at FROM json_each(:paths)
WHERE true  -- SQLite parses `ON CONFLICT` after a bare `FROM` as a join constraint
ON CONFLICT (store, path) DO UPDATE SET recordedAt = excluded.recordedAt
""",
    held=f"""
SELECT DISTINCT path FROM {T}
WHERE store IN (SELECT value FROM json_each(:stores)) AND path IN (SELECT value FROM json_each(:paths))
""",
    forget=f"DELETE FROM {T} WHERE store = :store AND path IN (SELECT value FROM json_each(:paths))",
)

POSTGRES = HeldBySql(
    record=f"""
INSERT INTO {T} (store, path, recordedAt) SELECT %(store)s, p, %(at)s FROM unnest(%(paths)s::text[]) AS p
ON CONFLICT (store, path) DO UPDATE SET recordedAt = excluded.recordedAt
""",
    held=f"SELECT DISTINCT path FROM {T} WHERE path = ANY(%(paths)s) AND store = ANY(%(stores)s)",
    forget=f"DELETE FROM {T} WHERE store = %(store)s AND path = ANY(%(paths)s)",
)
