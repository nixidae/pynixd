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
    uncovered: str
    """`after`, `stores` (a path set): `(id, path)` of each valid path above `after` none of the stores holds,
    by `id`. Nix's `ValidPaths.id` is `AUTOINCREMENT`, so a path registered later has a larger id."""
    uncovered_of: str
    """`paths`, `stores` (path sets): the valid paths of `paths` none of the stores holds, by `id`."""


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
    uncovered=f"""
SELECT v.id, v.path FROM ValidPaths v
WHERE v.id > :after
AND NOT EXISTS (SELECT 1 FROM {T} h WHERE h.path = v.path AND h.store IN (SELECT value FROM json_each(:stores)))
ORDER BY v.id
""",
    uncovered_of=f"""
SELECT v.path FROM ValidPaths v
WHERE v.path IN (SELECT value FROM json_each(:paths))
AND NOT EXISTS (SELECT 1 FROM {T} h WHERE h.path = v.path AND h.store IN (SELECT value FROM json_each(:stores)))
ORDER BY v.id
""",
)

POSTGRES = HeldBySql(
    record=f"""
INSERT INTO {T} (store, path, recordedAt) SELECT %(store)s, p, %(at)s FROM unnest(%(paths)s::text[]) AS p
ON CONFLICT (store, path) DO UPDATE SET recordedAt = excluded.recordedAt
""",
    held=f"SELECT DISTINCT path FROM {T} WHERE path = ANY(%(paths)s) AND store = ANY(%(stores)s)",
    forget=f"DELETE FROM {T} WHERE store = %(store)s AND path = ANY(%(paths)s)",
    uncovered=f"""
SELECT v.id, v.path FROM ValidPaths v
WHERE v.id > %(after)s
AND NOT EXISTS (SELECT 1 FROM {T} h WHERE h.path = v.path AND h.store = ANY(%(stores)s))
ORDER BY v.id
""",
    uncovered_of=f"""
SELECT v.path FROM ValidPaths v
WHERE v.path = ANY(%(paths)s)
AND NOT EXISTS (SELECT 1 FROM {T} h WHERE h.path = v.path AND h.store = ANY(%(stores)s))
ORDER BY v.id
""",
)
