"""The statements over `PynixdPathAccess`: when traffic last named a path, and the closures that date it."""

from __future__ import annotations

from dataclasses import dataclass

from ..db_migrations import PATH_ACCESS_TABLE


@dataclass(frozen=True, slots=True)
class PathAccessSql:
    touch: str
    """The touched paths, and only them: the flush expands each queued seed
    over its closure first, so this writes the union. Unmarked paths
    resolve their age from `registrationTime`, so nothing needs the
    closure's testimony twice."""
    runtime_closure: str
    """The runtime closure of a seed set: the seeds and everything reachable
    through references, never crossing a derivation boundary. A `.drv`
    file's references are its build inputs, so walking through one would
    refresh a build closure from a mere observation -- the pushed
    derivations stay heads. Store derivation paths always end in `.drv`,
    so the suffix is the boundary. The parameter is a JSON array of store
    paths. Issue #65."""
    build_closure: str
    """The build closure of a seed set: the seeds and everything reachable
    through references, derivations included. The scheduler seeds this
    with the derivation it decided to build plus the inputs the request
    names, so the walk covers the whole build closure whether or not the
    store has registered every layer. Same array parameter. Issue #65."""
    referrer_closure: str
    """The referrers of each seed, transitively, seeds included: the mirror of
    `runtime_closure` walking `Refs` the other way. A delete set travels
    with its referrers (`gc.cc:653`), so the planner closes its set under
    this."""
    weights: str
    unreferenced_since: str
    prune: str
    """The join the tables of pynixd get by living in Nix's own database."""


_SQLITE_REFERRERS_OF_SEEDS = """
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.referrer
        FROM closure c JOIN Refs r ON c.id = r.reference
    )
    SELECT id FROM closure
"""

SQLITE = PathAccessSql(
    touch=f"""
INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt)
SELECT path, unixepoch() FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
ON CONFLICT (path) DO UPDATE SET lastReferencedAt = excluded.lastReferencedAt
""",
    runtime_closure="""
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.reference FROM closure c
        JOIN Refs r ON c.id = r.referrer
        JOIN ValidPaths cvp ON cvp.id = c.id
        WHERE cvp.path NOT LIKE '%.drv'
    )
    SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
""",
    build_closure="""
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
        UNION
        SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer
    )
    SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
""",
    referrer_closure=f"""
SELECT path FROM ValidPaths WHERE id IN ({_SQLITE_REFERRERS_OF_SEEDS})
""",
    weights=f"""
SELECT v.path, v.narSize, COALESCE(a.lastReferencedAt, v.registrationTime) FROM ValidPaths v
LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
WHERE v.path IN (SELECT value FROM json_each(?))
""",
    unreferenced_since=f"""
SELECT v.path FROM ValidPaths v
LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
WHERE COALESCE(a.lastReferencedAt, v.registrationTime) < ?
""",
    prune=f"""
DELETE FROM {PATH_ACCESS_TABLE}
WHERE path NOT IN (SELECT path FROM ValidPaths)
""",
)


_POSTGRES_REFERRERS_OF_SEEDS = """
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path = ANY(%s)
        UNION
        SELECT r.referrer
        FROM closure c JOIN Refs r ON c.id = r.reference
    )
    SELECT id FROM closure
"""

POSTGRES = PathAccessSql(
    # `ORDER BY`: the upsert locks its rows in this order, so two instances that touch one chunk do not deadlock.
    touch=f"""
INSERT INTO {PATH_ACCESS_TABLE} (path, lastReferencedAt)
SELECT path, extract(epoch FROM now())::bigint FROM ValidPaths
WHERE path = ANY(%s)
ORDER BY path
ON CONFLICT (path) DO UPDATE SET lastReferencedAt = excluded.lastReferencedAt
""",
    runtime_closure="""
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path = ANY(%s)
        UNION
        SELECT r.reference FROM closure c
        JOIN Refs r ON c.id = r.referrer
        JOIN ValidPaths cvp ON cvp.id = c.id
        WHERE cvp.path NOT LIKE '%%.drv'
    )
    SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
""",
    build_closure="""
    WITH RECURSIVE closure(id) AS (
        SELECT id FROM ValidPaths WHERE path = ANY(%s)
        UNION
        SELECT r.reference FROM closure c JOIN Refs r ON c.id = r.referrer
    )
    SELECT path FROM ValidPaths WHERE id IN (SELECT id FROM closure)
""",
    referrer_closure=f"""
SELECT path FROM ValidPaths WHERE id IN ({_POSTGRES_REFERRERS_OF_SEEDS})
""",
    weights=f"""
SELECT v.path, v.narSize, COALESCE(a.lastReferencedAt, v.registrationTime) FROM ValidPaths v
LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
WHERE v.path = ANY(%s)
""",
    unreferenced_since=f"""
SELECT v.path FROM ValidPaths v
LEFT JOIN {PATH_ACCESS_TABLE} a ON a.path = v.path
WHERE COALESCE(a.lastReferencedAt, v.registrationTime) < %s
""",
    prune=f"""
DELETE FROM {PATH_ACCESS_TABLE}
WHERE path NOT IN (SELECT path FROM ValidPaths)
""",
)
