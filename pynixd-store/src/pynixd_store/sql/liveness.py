"""The statements of the liveness mirror (`liveness.py`) and of the roots report: `PynixdGCRoot`, `PynixdLive`, `PynixdLivenessStreak`."""

from __future__ import annotations

from dataclasses import dataclass

from ..db_migrations import LIVENESS_ROOT_TABLE, LIVENESS_STREAK_TABLE, LIVENESS_TABLE


@dataclass(frozen=True, slots=True)
class LivenessSql:
    live_set: str
    fingerprint: str
    roots: str
    delete_root: str
    upsert_root: str
    create_live_now: str
    fill_live_now: str
    delete_left: str
    insert_joined: str
    newest_epoch: str
    live_paths: str
    record_streak: str
    read_streak: str
    create_root_hits: str
    fill_root_hits: str
    root_full_counts: str
    """One root's closure over the same edges `live_set` walks: references, derivers, and deriver references.
    The report runs it once per distinct seed set."""
    mark_root_hits: str
    root_exclusive_counts: str
    drop_root_hits: str


_SQLITE_ROOT_CLOSURE_OF_SEEDS = """
    WITH RECURSIVE closure(id) AS (
        SELECT vp.id FROM ValidPaths vp WHERE vp.path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer
        UNION
        SELECT deriver_vp.id FROM closure c
        JOIN ValidPaths current_vp ON c.id = current_vp.id
        JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path
        WHERE current_vp.deriver IS NOT NULL
        UNION
        SELECT r.reference FROM closure c
        JOIN ValidPaths current_vp ON c.id = current_vp.id
        JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path
        JOIN Refs r ON deriver_vp.id = r.referrer
        WHERE current_vp.deriver IS NOT NULL
    )
    SELECT id FROM closure
"""

SQLITE = LivenessSql(
    live_set=f"""
WITH RECURSIVE closure(id) AS (
    SELECT vp.id FROM ValidPaths vp
    WHERE vp.path IN (
        SELECT target FROM {LIVENESS_ROOT_TABLE}
        UNION
        SELECT value FROM json_each(?)
    )
    UNION
    SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer
    UNION
    SELECT deriver_vp.id FROM closure c
    JOIN ValidPaths current_vp ON c.id = current_vp.id
    JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path
    WHERE current_vp.deriver IS NOT NULL
    UNION
    SELECT r.reference FROM closure c
    JOIN ValidPaths current_vp ON c.id = current_vp.id
    JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path
    JOIN Refs r ON deriver_vp.id = r.referrer
    WHERE current_vp.deriver IS NOT NULL
)
SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
""",
    fingerprint="SELECT COUNT(*), COALESCE(MAX(id), 0) FROM ValidPaths",
    roots=f"SELECT link, target, kind FROM {LIVENESS_ROOT_TABLE}",
    delete_root=f"DELETE FROM {LIVENESS_ROOT_TABLE} WHERE link = ?",
    upsert_root=f"INSERT OR REPLACE INTO {LIVENESS_ROOT_TABLE} (link, target, kind) VALUES (?, ?, ?)",
    create_live_now="CREATE TEMP TABLE live_now(path TEXT PRIMARY KEY)",
    fill_live_now="INSERT INTO live_now SELECT value FROM json_each(?)",
    delete_left=f"DELETE FROM {LIVENESS_TABLE} WHERE path NOT IN (SELECT path FROM live_now)",
    insert_joined=f"INSERT OR IGNORE INTO {LIVENESS_TABLE} (path, epoch) SELECT path, ? FROM live_now",
    newest_epoch=f"SELECT COALESCE(MAX(epoch), -1) FROM {LIVENESS_TABLE}",
    live_paths=f"SELECT path FROM {LIVENESS_TABLE}",
    record_streak=f"""
INSERT INTO {LIVENESS_STREAK_TABLE} (id, agreements, divergences, checks, live, updatedAt)
VALUES (1, ?, ?, 1, ?, unixepoch())
ON CONFLICT (id) DO UPDATE SET
    agreements = CASE WHEN ? THEN {LIVENESS_STREAK_TABLE}.agreements + 1 ELSE 0 END,
    divergences = {LIVENESS_STREAK_TABLE}.divergences + CASE WHEN ? THEN 0 ELSE 1 END,
    checks = {LIVENESS_STREAK_TABLE}.checks + 1,
    live = excluded.live,
    updatedAt = unixepoch()
""",
    read_streak=f"SELECT agreements, divergences, checks, live, updatedAt FROM {LIVENESS_STREAK_TABLE} WHERE id = 1",
    create_root_hits="CREATE TEMP TABLE _pynixd_root_hits(id INTEGER PRIMARY KEY, n INTEGER NOT NULL DEFAULT 0)",
    fill_root_hits="INSERT INTO _pynixd_root_hits(id) SELECT id FROM ValidPaths",
    root_full_counts=(
        f"SELECT COUNT(*), COALESCE(SUM(narSize), 0) FROM ValidPaths WHERE id IN ({_SQLITE_ROOT_CLOSURE_OF_SEEDS})"
    ),
    mark_root_hits=f"UPDATE _pynixd_root_hits SET n = n + ? WHERE id IN ({_SQLITE_ROOT_CLOSURE_OF_SEEDS})",
    root_exclusive_counts=(
        f"SELECT COUNT(*), COALESCE(SUM(v.narSize), 0) FROM ValidPaths v "
        f"JOIN _pynixd_root_hits h ON h.id = v.id "
        f"WHERE h.n = 1 AND v.id IN ({_SQLITE_ROOT_CLOSURE_OF_SEEDS})"
    ),
    drop_root_hits="DROP TABLE _pynixd_root_hits",
)
