"""The statements of the liveness mirror (`liveness.py`) and of the roots report: `PynixdGCRoot`, `PynixdLive`, `PynixdLivenessStreak`."""

from __future__ import annotations

from dataclasses import dataclass

from ..db_migrations import LIVENESS_ROOT_TABLE, LIVENESS_STREAK_TABLE, LIVENESS_TABLE

type Fragment = str
"""A piece of a statement, not one: `test_postgres_statements` parses each `str` field alone."""


@dataclass(frozen=True, slots=True)
class LivenessSql:
    live_set: str
    """The closure of the roots table plus a parameter of living roots, over `Refs` and the `{edges}` the
    settings add: Nix's `markAlive` closure (`gc.cc:598`, `computeFSClosure` in `misc.cc:48`)."""
    closure: str
    """`live_set` of the parameter's paths alone: a collection's own roots, with no roots table."""
    deriver_edge: Fragment
    """`keep-derivations`: a path keeps its valid deriver."""
    output_edge: Fragment
    """`keep-outputs`: a derivation keeps its valid outputs, as `DerivationOutputs` records them."""
    build_trace_edge: Fragment
    """`keep-outputs`: a derivation keeps the valid outputs its build trace records (`BuildTraceV3`)."""
    has_build_trace: str
    """Whether the database has `BuildTraceV3`: a store that never enabled `ca-derivations` may not."""
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
    """One root's closure over references and derivers: `live_set` at Nix's default settings.
    The report runs it once per distinct seed set."""
    mark_root_hits: str
    root_exclusive_counts: str
    drop_root_hits: str

    def edges(self, *, keep_derivations: bool, keep_outputs: bool, build_trace: bool) -> str:
        """The `{edges}` of `live_set` and `closure` for these settings; *build_trace* when `BuildTraceV3` exists."""
        edges = self.deriver_edge if keep_derivations else ""
        if keep_outputs:
            edges += self.output_edge
            if build_trace:
                edges += self.build_trace_edge
        return edges


_SQLITE_CLOSURE = """
WITH RECURSIVE closure(id) AS (
    SELECT vp.id FROM ValidPaths vp
    WHERE vp.path IN ({seeds})
    UNION
    SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer{{edges}}
)
SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
"""

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
    live_set=_SQLITE_CLOSURE.format(
        seeds=f"SELECT target FROM {LIVENESS_ROOT_TABLE} UNION SELECT value FROM json_each(?)"
    ),
    closure=_SQLITE_CLOSURE.format(seeds="SELECT value FROM json_each(?)"),
    deriver_edge="""
    UNION
    SELECT deriver_vp.id FROM closure c
    JOIN ValidPaths current_vp ON c.id = current_vp.id
    JOIN ValidPaths deriver_vp ON current_vp.deriver = deriver_vp.path""",
    output_edge="""
    UNION
    SELECT output_vp.id FROM closure c
    JOIN DerivationOutputs d ON d.drv = c.id
    JOIN ValidPaths output_vp ON output_vp.path = d.path""",
    # `drvPath` is the derivation's base name. `rtrim` by every character of the path but `/` leaves its directory.
    build_trace_edge="""
    UNION
    SELECT output_vp.id FROM closure c
    JOIN ValidPaths drv_vp ON drv_vp.id = c.id
    JOIN BuildTraceV3 b ON b.drvPath = substr(drv_vp.path, length(rtrim(drv_vp.path, replace(drv_vp.path, '/', ''))) + 1)
    JOIN ValidPaths output_vp ON output_vp.path = b.outputPath
    WHERE substr(drv_vp.path, -4) = '.drv'""",
    has_build_trace="SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'BuildTraceV3'",
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


# PostgreSQL takes one recursive term, where SQLite's walk has three: the references, the deriver, and the
# deriver's references. One `LATERAL` union holds the three.
_POSTGRES_STEP = """SELECT step.id FROM closure c
    JOIN ValidPaths current_vp ON current_vp.id = c.id
    CROSS JOIN LATERAL (
        SELECT r.reference AS id FROM Refs r WHERE r.referrer = c.id
        UNION ALL
        SELECT deriver_vp.id FROM ValidPaths deriver_vp WHERE deriver_vp.path = current_vp.deriver
        UNION ALL
        SELECT r.reference FROM ValidPaths deriver_vp JOIN Refs r ON deriver_vp.id = r.referrer
        WHERE deriver_vp.path = current_vp.deriver
    ) AS step"""

_POSTGRES_ROOT_CLOSURE_OF_SEEDS = f"""
    WITH RECURSIVE closure(id) AS (
        SELECT vp.id FROM ValidPaths vp WHERE vp.path = ANY(%s)
        UNION
        {_POSTGRES_STEP}
    )
    SELECT id FROM closure
"""

_POSTGRES_CLOSURE = """
WITH RECURSIVE closure(id) AS (
    SELECT vp.id FROM ValidPaths vp
    WHERE vp.path IN ({seeds})
    UNION
    SELECT step.id FROM closure c
    JOIN ValidPaths current_vp ON current_vp.id = c.id
    CROSS JOIN LATERAL (
        SELECT r.reference AS id FROM Refs r WHERE r.referrer = c.id{{edges}}
    ) AS step
)
SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
"""

# `SUM` of a BIGINT is `numeric`; the casts keep each count an integer, as SQLite answers it.
POSTGRES = LivenessSql(
    live_set=_POSTGRES_CLOSURE.format(
        seeds=f"SELECT target FROM {LIVENESS_ROOT_TABLE} UNION SELECT unnest(%s::text[])"
    ),
    closure=_POSTGRES_CLOSURE.format(seeds="SELECT unnest(%s::text[])"),
    deriver_edge="""
        UNION ALL
        SELECT deriver_vp.id FROM ValidPaths deriver_vp WHERE deriver_vp.path = current_vp.deriver""",
    output_edge="""
        UNION ALL
        SELECT output_vp.id FROM DerivationOutputs d JOIN ValidPaths output_vp ON output_vp.path = d.path
        WHERE d.drv = c.id""",
    # No `LIKE`: a `%` in a statement with parameters is a placeholder to psycopg.
    build_trace_edge="""
        UNION ALL
        SELECT output_vp.id FROM BuildTraceV3 b JOIN ValidPaths output_vp ON output_vp.path = b.outputPath
        WHERE right(current_vp.path, 4) = '.drv' AND b.drvPath = substring(current_vp.path FROM '[^/]*$')""",
    has_build_trace="SELECT CASE WHEN to_regclass('buildtracev3') IS NULL THEN 0 ELSE 1 END",
    fingerprint="SELECT COUNT(*), COALESCE(MAX(id), 0) FROM ValidPaths",
    roots=f"SELECT link, target, kind FROM {LIVENESS_ROOT_TABLE}",
    delete_root=f"DELETE FROM {LIVENESS_ROOT_TABLE} WHERE link = %s",
    upsert_root=(
        f"INSERT INTO {LIVENESS_ROOT_TABLE} (link, target, kind) VALUES (%s, %s, %s) "
        "ON CONFLICT (link) DO UPDATE SET target = excluded.target, kind = excluded.kind"
    ),
    create_live_now="CREATE TEMP TABLE live_now(id BIGINT PRIMARY KEY)",
    fill_live_now="INSERT INTO live_now SELECT id FROM ValidPaths WHERE path = ANY(%s)",
    delete_left=f"DELETE FROM {LIVENESS_TABLE} WHERE pathId NOT IN (SELECT id FROM live_now)",
    insert_joined=(
        f"INSERT INTO {LIVENESS_TABLE} (pathId, epoch) SELECT id, %s::bigint FROM live_now ON CONFLICT DO NOTHING"
    ),
    newest_epoch=f"SELECT COALESCE(MAX(epoch), -1) FROM {LIVENESS_TABLE}",
    live_paths=f"SELECT v.path FROM {LIVENESS_TABLE} l JOIN ValidPaths v ON v.id = l.pathId",
    record_streak=f"""
INSERT INTO {LIVENESS_STREAK_TABLE} (id, agreements, divergences, checks, live, updatedAt)
VALUES (1, %s, %s, 1, %s, extract(epoch FROM now())::bigint)
ON CONFLICT (id) DO UPDATE SET
    agreements = CASE WHEN %s::bigint <> 0 THEN {LIVENESS_STREAK_TABLE}.agreements + 1 ELSE 0 END,
    divergences = {LIVENESS_STREAK_TABLE}.divergences + CASE WHEN %s::bigint <> 0 THEN 0 ELSE 1 END,
    checks = {LIVENESS_STREAK_TABLE}.checks + 1,
    live = excluded.live,
    updatedAt = extract(epoch FROM now())::bigint
""",
    read_streak=f"SELECT agreements, divergences, checks, live, updatedAt FROM {LIVENESS_STREAK_TABLE} WHERE id = 1",
    create_root_hits="CREATE TEMP TABLE _pynixd_root_hits(id BIGINT PRIMARY KEY, n BIGINT NOT NULL DEFAULT 0)",
    fill_root_hits="INSERT INTO _pynixd_root_hits(id) SELECT id FROM ValidPaths",
    root_full_counts=(
        f"SELECT COUNT(*), COALESCE(SUM(narSize), 0)::bigint FROM ValidPaths WHERE id IN ({_POSTGRES_ROOT_CLOSURE_OF_SEEDS})"
    ),
    mark_root_hits=f"UPDATE _pynixd_root_hits SET n = n + %s WHERE id IN ({_POSTGRES_ROOT_CLOSURE_OF_SEEDS})",
    root_exclusive_counts=(
        f"SELECT COUNT(*), COALESCE(SUM(v.narSize), 0)::bigint FROM ValidPaths v "
        f"JOIN _pynixd_root_hits h ON h.id = v.id "
        f"WHERE h.n = 1 AND v.id IN ({_POSTGRES_ROOT_CLOSURE_OF_SEEDS})"
    ),
    drop_root_hits="DROP TABLE _pynixd_root_hits",
)
