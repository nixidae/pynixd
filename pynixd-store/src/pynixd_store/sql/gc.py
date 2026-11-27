"""The statements of the collector's plan (`gc_sql.SqlPlan`): set algebra over Nix's tables into TEMP tables."""

from __future__ import annotations

from dataclasses import dataclass

from ..db_migrations import HELD_BY_TABLE, PATH_ACCESS_TABLE


@dataclass(frozen=True, slots=True)
class GcSql:
    schema: tuple[str, ...]
    load_ids: str
    """Formatted with the `{table}` to fill: `gc_live`, `gc_veto` or `gc_batch`."""
    load_dead: str
    plan: str
    """The stale dead seeds and everything that refers to them, kept to the dead.
    A NULL cutoff takes every dead path (a pressed disk). A referrer of a dead path
    is dead unless the mirror is wrong, so the filter only guards that case."""
    counts: str
    weighed: str
    """The weight `by_weight` computes, in the same operations and the same
    order, so the two agree to the bit: size and age each normalised against
    the candidates, blended by pressure, path string breaking ties. `before`
    is what the rows ahead of this one free, which is what
    `take_until_below_target` tests before it takes a row, times the bound's ratio."""
    clear_batch: str
    batch_count: str
    batch_referrers: str
    escaped_seeds: str
    """A seed escapes the plan when anything outside the plan refers to it,
    transitively. Walking down from the batch's outside referrers marks every
    seed one of them reaches."""
    weights_of: str
    batch_edges: str
    load_covered: str
    """The valid paths `PynixdHeldBy` records under any store of a path set."""
    drop_uncovered: str
    """Drops each uncovered planned path and everything it references: Nix refuses to delete a
    reference while its referrer stays (`gc.cc:653`). What remains is still closed under referrers."""
    planned_counts: str


SQLITE = GcSql(
    schema=(
        "CREATE TEMP TABLE gc_live(id INTEGER PRIMARY KEY)",
        "CREATE TEMP TABLE gc_veto(id INTEGER PRIMARY KEY)",
        "CREATE TEMP TABLE gc_dead(id INTEGER PRIMARY KEY, path TEXT NOT NULL, size INTEGER NOT NULL, seen INTEGER)",
        "CREATE TEMP TABLE gc_planned(id INTEGER PRIMARY KEY)",
        "CREATE TEMP TABLE gc_batch(id INTEGER PRIMARY KEY)",
        "CREATE TEMP TABLE gc_covered(id INTEGER PRIMARY KEY)",
    ),
    load_ids="INSERT OR IGNORE INTO {table} SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))",
    load_dead=f"""
INSERT INTO gc_dead
SELECT v.id, v.path, COALESCE(v.narSize, 0), COALESCE(a.lastReferencedAt, v.registrationTime)
FROM ValidPaths v LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
WHERE v.id NOT IN (SELECT id FROM gc_live)
""",
    plan="""
INSERT INTO gc_planned
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_dead WHERE :cutoff IS NULL OR seen < :cutoff
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
)
SELECT id FROM closure WHERE id IN (SELECT id FROM gc_dead)
""",
    counts=f"""
SELECT
    (SELECT count(*) FROM ValidPaths),
    (SELECT count(*) FROM gc_live),
    (SELECT count(*) FROM ValidPaths v LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
        WHERE :cutoff IS NOT NULL AND COALESCE(a.lastReferencedAt, v.registrationTime) < :cutoff),
    (SELECT count(*) FROM gc_planned),
    (SELECT count(*) FROM gc_planned WHERE id IN (SELECT id FROM gc_veto)),
    (SELECT COALESCE(sum(size), 0) FROM gc_dead WHERE id NOT IN (SELECT id FROM gc_veto))
""",
    weighed="""
WITH w AS (
    SELECT d.path, d.size, CASE WHEN d.seen IS NULL THEN 0 ELSE max(0, :now - d.seen) END AS age
    FROM gc_planned p JOIN gc_dead d USING (id)
    WHERE p.id NOT IN (SELECT id FROM gc_veto)
), n AS (
    SELECT max(size) AS big, max(age) AS old FROM w
), scored AS (
    SELECT w.path, w.size, w.age,
        :pressure * (CASE WHEN n.big THEN 1.0 * w.size / n.big ELSE 0.0 END)
        + (1.0 - :pressure) * (CASE WHEN n.old THEN 1.0 * w.age / n.old ELSE 0.0 END) AS score
    FROM w, n
), ordered AS (
    SELECT path, size, age, score,
        COALESCE(sum(size) OVER (ORDER BY score DESC, path ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
            AS before
    FROM scored
)
SELECT path, size, age FROM ordered
WHERE :total IS NULL OR (:used - :ratio * before) / :total > :target
ORDER BY score DESC, path
LIMIT :limit
""",
    clear_batch="DELETE FROM gc_batch",
    batch_count="SELECT count(*) FROM gc_batch",
    batch_referrers="""
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_batch
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
)
SELECT v.path, c.id IN (SELECT id FROM gc_planned) FROM closure c JOIN ValidPaths v ON v.id = c.id
""",
    escaped_seeds="""
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_batch
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
), tainted(id) AS (
    SELECT id FROM closure WHERE id NOT IN (SELECT id FROM gc_planned)
    UNION
    SELECT r.reference FROM tainted t JOIN Refs r ON r.referrer = t.id WHERE r.reference IN (SELECT id FROM closure)
)
SELECT v.path FROM tainted t JOIN gc_batch b ON b.id = t.id JOIN ValidPaths v ON v.id = t.id
""",
    weights_of="""
SELECT path, size, CASE WHEN seen IS NULL THEN 0 ELSE max(0, ?1 - seen) END FROM gc_dead
WHERE path IN (SELECT value FROM json_each(?2))
""",
    batch_edges="""
SELECT a.path, b.path FROM Refs r
JOIN gc_batch ra ON ra.id = r.referrer JOIN gc_batch rb ON rb.id = r.reference
JOIN ValidPaths a ON a.id = r.referrer JOIN ValidPaths b ON b.id = r.reference
WHERE r.referrer != r.reference
""",
    load_covered=f"""
INSERT OR IGNORE INTO gc_covered
SELECT v.id FROM ValidPaths v JOIN {HELD_BY_TABLE} h ON h.path = v.path
WHERE h.store IN (SELECT value FROM json_each(?))
""",
    drop_uncovered="""
DELETE FROM gc_planned WHERE id IN (
    WITH RECURSIVE keep(id) AS (
        SELECT id FROM gc_planned WHERE id NOT IN (SELECT id FROM gc_covered)
        UNION
        SELECT r.reference FROM keep k JOIN Refs r ON r.referrer = k.id
    )
    SELECT id FROM keep
)
""",
    planned_counts="""
SELECT (SELECT count(*) FROM gc_planned), (SELECT count(*) FROM gc_planned WHERE id IN (SELECT id FROM gc_veto))
""",
)


# The weight is `double precision` throughout, as SQLite's REAL and Python's float: a `numeric` score orders
# ties another way. The window sum is cast back to BIGINT, and `NULLIF` turns the caller's `-1` into no limit.
POSTGRES = GcSql(
    schema=(
        "CREATE TEMP TABLE gc_live(id BIGINT PRIMARY KEY)",
        "CREATE TEMP TABLE gc_veto(id BIGINT PRIMARY KEY)",
        'CREATE TEMP TABLE gc_dead(id BIGINT PRIMARY KEY, path TEXT COLLATE "C" NOT NULL, size BIGINT NOT NULL, seen BIGINT)',
        "CREATE TEMP TABLE gc_planned(id BIGINT PRIMARY KEY)",
        "CREATE TEMP TABLE gc_batch(id BIGINT PRIMARY KEY)",
        "CREATE TEMP TABLE gc_covered(id BIGINT PRIMARY KEY)",
    ),
    load_ids=("INSERT INTO {table} SELECT id FROM ValidPaths WHERE path = ANY(%s) ON CONFLICT DO NOTHING"),
    load_dead=f"""
INSERT INTO gc_dead
SELECT v.id, v.path, COALESCE(v.narSize, 0), COALESCE(a.lastReferencedAt, v.registrationTime)
FROM ValidPaths v LEFT JOIN {PATH_ACCESS_TABLE} a ON a.pathId = v.id
WHERE v.id NOT IN (SELECT id FROM gc_live)
""",
    plan="""
INSERT INTO gc_planned
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_dead WHERE %(cutoff)s::bigint IS NULL OR seen < %(cutoff)s::bigint
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
)
SELECT id FROM closure WHERE id IN (SELECT id FROM gc_dead)
""",
    counts=f"""
SELECT
    (SELECT count(*) FROM ValidPaths),
    (SELECT count(*) FROM gc_live),
    (SELECT count(*) FROM ValidPaths v LEFT JOIN {PATH_ACCESS_TABLE} a ON a.pathId = v.id
        WHERE %(cutoff)s::bigint IS NOT NULL AND COALESCE(a.lastReferencedAt, v.registrationTime) < %(cutoff)s::bigint),
    (SELECT count(*) FROM gc_planned),
    (SELECT count(*) FROM gc_planned WHERE id IN (SELECT id FROM gc_veto)),
    (SELECT COALESCE(sum(size), 0) FROM gc_dead WHERE id NOT IN (SELECT id FROM gc_veto))
""",
    weighed="""
WITH w AS (
    SELECT d.path, d.size, CASE WHEN d.seen IS NULL THEN 0 ELSE greatest(0, %(now)s::bigint - d.seen) END AS age
    FROM gc_planned p JOIN gc_dead d USING (id)
    WHERE p.id NOT IN (SELECT id FROM gc_veto)
), n AS (
    SELECT max(size) AS big, max(age) AS old FROM w
), scored AS (
    SELECT w.path, w.size, w.age,
        %(pressure)s::float8 * (CASE WHEN n.big <> 0 THEN w.size::float8 / n.big ELSE 0.0::float8 END)
        + (1.0::float8 - %(pressure)s::float8) * (CASE WHEN n.old <> 0 THEN w.age::float8 / n.old ELSE 0.0::float8 END)
        AS score
    FROM w, n
), ordered AS (
    SELECT path, size, age, score,
        COALESCE(sum(size) OVER (ORDER BY score DESC, path ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)::bigint
            AS before
    FROM scored
)
SELECT path, size, age FROM ordered
WHERE %(total)s::bigint IS NULL
    OR (%(used)s::bigint - %(ratio)s::float8 * before) / %(total)s::bigint > %(target)s::float8
ORDER BY score DESC, path
LIMIT NULLIF(%(limit)s::bigint, -1)
""",
    clear_batch="DELETE FROM gc_batch",
    batch_count="SELECT count(*) FROM gc_batch",
    batch_referrers="""
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_batch
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
)
SELECT v.path, c.id IN (SELECT id FROM gc_planned) FROM closure c JOIN ValidPaths v ON v.id = c.id
""",
    escaped_seeds="""
WITH RECURSIVE closure(id) AS (
    SELECT id FROM gc_batch
    UNION
    SELECT r.referrer FROM closure c JOIN Refs r ON r.reference = c.id
), tainted(id) AS (
    SELECT id FROM closure WHERE id NOT IN (SELECT id FROM gc_planned)
    UNION
    SELECT r.reference FROM tainted t JOIN Refs r ON r.referrer = t.id WHERE r.reference IN (SELECT id FROM closure)
)
SELECT v.path FROM tainted t JOIN gc_batch b ON b.id = t.id JOIN ValidPaths v ON v.id = t.id
""",
    weights_of="""
SELECT path, size, CASE WHEN seen IS NULL THEN 0 ELSE greatest(0, %s::bigint - seen) END FROM gc_dead
WHERE path = ANY(%s)
""",
    batch_edges="""
SELECT a.path, b.path FROM Refs r
JOIN gc_batch ra ON ra.id = r.referrer JOIN gc_batch rb ON rb.id = r.reference
JOIN ValidPaths a ON a.id = r.referrer JOIN ValidPaths b ON b.id = r.reference
WHERE r.referrer != r.reference
""",
    load_covered=f"""
INSERT INTO gc_covered
SELECT v.id FROM ValidPaths v JOIN {HELD_BY_TABLE} h ON h.path = v.path
WHERE h.store = ANY(%s)
ON CONFLICT DO NOTHING
""",
    drop_uncovered="""
DELETE FROM gc_planned WHERE id IN (
    WITH RECURSIVE keep(id) AS (
        SELECT id FROM gc_planned WHERE id NOT IN (SELECT id FROM gc_covered)
        UNION
        SELECT r.reference FROM keep k JOIN Refs r ON r.referrer = k.id
    )
    SELECT id FROM keep
)
""",
    planned_counts="""
SELECT (SELECT count(*) FROM gc_planned), (SELECT count(*) FROM gc_planned WHERE id IN (SELECT id FROM gc_veto))
""",
)
