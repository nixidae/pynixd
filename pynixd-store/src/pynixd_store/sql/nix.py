"""The statements over Nix's own tables: `ValidPaths`, `Refs`, `DerivationOutputs` and the realisations."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class NixSql:
    """The statements of Nix's `LocalStore`, for a `ValidPaths` with or without `provenance`.

    Determinate Nix adds the column with its `provenance` experimental feature, in the migration
    `20241024-provenance` (`local-store.cc:679` of v3.24.0). Without it each read answers `NULL` in its place, so
    a row has one shape: the eight columns of Nix, then provenance. `update_path_info` leaves provenance alone, as
    Nix's does (`local-store.cc:388`).
    """

    provenance: bool
    is_valid_path: str
    any_valid_path: str
    path_info_with_refs: str
    all_valid_paths: str
    referrers: str
    valid_derivers: str
    derivation_outputs: str
    realisation_by_hash: str
    realisation_by_path: str
    register_build_trace_by_path: str
    update_build_trace_by_path: str
    register_realisation_by_hash: str
    update_realisation_by_hash: str
    valid_paths: str
    """One row, as `closure`."""
    path_from_hash_part: str
    path_from_nar_hash: str
    closure: str
    """One row: Python's `sqlite3` takes the GIL again for each row it steps to, and in a busy process a closure of
    2000 paths then takes 255 ms, where the query alone takes 4 ms (nixidae/nixidae#1594)."""
    closure_sizes: str
    """One row, as `closure`: each path, a colon and its NAR size."""
    closure_with_info: str
    path_infos_batch: str
    references_batch: str
    derivation_output_map_batch: str
    register_valid_path: str
    update_path_info: str
    path_info_for_update: str
    invalidate_path: str
    add_reference: str
    add_derivation_output: str
    valid_path_id: str
    valid_path_ids: str
    """The path and the id of each valid path of a `path_set`."""


_SQLITE_PATH_INFO_WITH_REFS = """
SELECT v.path, v.deriver, v.hash, v.registrationTime, v.narSize, v.ultimate, v.sigs, v.ca, {provenance}, vp.path
FROM ValidPaths v
LEFT JOIN Refs r ON r.referrer = v.id
LEFT JOIN ValidPaths vp ON vp.id = r.reference
WHERE v.path = ?
"""

_SQLITE_CLOSURE_WITH_INFO = """
WITH RECURSIVE closure(id) AS (
    SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
    UNION
    SELECT r.reference
    FROM closure c
    JOIN Refs r ON c.id = r.referrer
)
SELECT vp.path, vp.deriver, vp.hash, vp.registrationTime, vp.narSize,
       vp.ultimate, vp.sigs, vp.ca, {provenance},
       (SELECT group_concat(ref_vp.path, ' ')
        FROM Refs r
        JOIN ValidPaths ref_vp ON r.reference = ref_vp.id
        WHERE r.referrer = vp.id)
FROM closure c
JOIN ValidPaths vp ON c.id = vp.id
ORDER BY vp.id ASC
"""

_SQLITE_PATH_INFOS_BATCH = """
SELECT vp.path, vp.deriver, vp.hash, registrationTime, narSize,
       vp.ultimate, vp.sigs, vp.ca, {provenance}
FROM ValidPaths vp
WHERE vp.path IN (SELECT value FROM json_each(?))
"""

# Nix's own statements for a registration, `local-store.cc:331-348` of 2.34,
# the same on master. Every write to Nix's tables uses one of these.
_SQLITE_REGISTER_VALID_PATH = "insert into ValidPaths (path, hash, registrationTime, deriver, narSize, ultimate, sigs, ca{provenance}) values (?, ?, ?, ?, ?, ?, ?, ?{value});"


def sqlite(*, provenance: bool) -> NixSql:
    column = "{table}.provenance" if provenance else "NULL"
    return NixSql(
        provenance=provenance,
        is_valid_path="SELECT 1 FROM ValidPaths WHERE path = ? LIMIT 1",
        any_valid_path="SELECT 1 FROM ValidPaths LIMIT 1",
        path_info_with_refs=_SQLITE_PATH_INFO_WITH_REFS.format(provenance=column.format(table="v")),
        all_valid_paths="SELECT path FROM ValidPaths",
        # Nix's own statement, `local-store.cc:364`. A path the store does not hold
        # has no id, so it has no referrers, and a path that refers to itself is one.
        referrers="select path from Refs join ValidPaths on referrer = id where reference = (select id from ValidPaths where path = ?)",
        # Nix's own statement, `local-store.cc:371`. A deriver counts while it is
        # valid, whether or not the output it names is.
        valid_derivers="select v.id, v.path from DerivationOutputs d join ValidPaths v on d.drv = v.id where d.path = ?",
        # `local-store.cc:372`, with the drv's id looked up in the same statement. No
        # row is a drv that is not valid, which Nix refuses (`queryValidPathId`).
        derivation_outputs=(
            "select v.id, d.id, d.path from ValidPaths v left join DerivationOutputs d on d.drv = v.id where v.path = ?"
        ),
        # Nix's own statements for a realisation. Nix 2.34 and 2.35 key `Realisations`
        # by the hash of the derivation (`local-store.cc:377` of 2.34); Nix master keys
        # `BuildTraceV3` by its path, without the store directory (`local-store.cc:407`).
        realisation_by_hash=(
            "select Output.path, Realisations.signatures from Realisations "
            "inner join ValidPaths as Output on Output.id = Realisations.outputPath "
            "where drvPath = ? and outputName = ?"
        ),
        realisation_by_path="select outputPath, signatures from BuildTraceV3 where drvPath = ? and outputName = ?",
        # `registerDrvOutput` (`local-store.cc:664`). The feature picks the table, as
        # the reads above do. `BuildTraceV3.outputPath` is the whole path as text; Nix
        # 2.34's `Realisations.outputPath` is a `ValidPaths` id, filled by the
        # subquery (`local-store.cc:358`). Both bind the signatures as a space-joined
        # string, empty when there are none (no NULL flag, unlike `ValidPaths.sigs`).
        register_build_trace_by_path=(
            "insert into BuildTraceV3 (drvPath, outputName, outputPath, signatures) values (?, ?, ?, ?)"
        ),
        update_build_trace_by_path="update BuildTraceV3 set signatures = ? where drvPath = ? and outputName = ?",
        register_realisation_by_hash=(
            "insert into Realisations (drvPath, outputName, outputPath, signatures) "
            "values (?, ?, (select id from ValidPaths where path = ?), ?)"
        ),
        update_realisation_by_hash="update Realisations set signatures = ? where drvPath = ? and outputName = ?",
        valid_paths="""
SELECT group_concat(path, ' ') FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
""",
        path_from_hash_part="""
SELECT path FROM ValidPaths WHERE path >= ? AND path < ? LIMIT 1
""",
        path_from_nar_hash="SELECT path FROM ValidPaths WHERE hash = ? LIMIT 1",
        closure="""
WITH RECURSIVE closure(id) AS (
    SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
    UNION
    SELECT r.reference
    FROM closure c
    JOIN Refs r ON c.id = r.referrer
)
SELECT group_concat(vp.path, ' ') FROM closure c
JOIN ValidPaths vp ON c.id = vp.id
""",
        closure_sizes="""
WITH RECURSIVE closure(id) AS (
    SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
    UNION
    SELECT r.reference
    FROM closure c
    JOIN Refs r ON c.id = r.referrer
)
SELECT group_concat(vp.path || ':' || coalesce(vp.narSize, 0), ' ') FROM closure c
JOIN ValidPaths vp ON c.id = vp.id
""",
        closure_with_info=_SQLITE_CLOSURE_WITH_INFO.format(provenance=column.format(table="vp")),
        path_infos_batch=_SQLITE_PATH_INFOS_BATCH.format(provenance=column.format(table="vp")),
        references_batch="""
SELECT vp_referrer.path, vp_ref.path
FROM Refs r
JOIN ValidPaths vp_referrer ON r.referrer = vp_referrer.id
JOIN ValidPaths vp_ref ON r.reference = vp_ref.id
WHERE vp_referrer.path IN (SELECT value FROM json_each(?))
""",
        derivation_output_map_batch="""
SELECT vp_drv.path, do.id, do.path
FROM DerivationOutputs do
JOIN ValidPaths vp_drv ON do.drv = vp_drv.id
WHERE vp_drv.path IN (SELECT value FROM json_each(?))
""",
        register_valid_path=_SQLITE_REGISTER_VALID_PATH.format(
            provenance=", provenance" if provenance else "", value=", ?" if provenance else ""
        ),
        update_path_info="update ValidPaths set narSize = ?, hash = ?, ultimate = ?, sigs = ?, ca = ? where path = ?;",
        # The fields `updatePathInfo` writes back, read for `addSignatures`: it changes only `sigs` and keeps the rest.
        path_info_for_update="select narSize, hash, ultimate, sigs, ca from ValidPaths where path = ?;",
        # `LocalStore::invalidatePath` (`local-store.cc:1016`). The `Refs` foreign keys
        # cascade the referrer's entries away, so the connection must enforce them.
        invalidate_path="delete from ValidPaths where path = ?;",
        add_reference="insert or replace into Refs (referrer, reference) values (?, ?);",
        add_derivation_output="insert or replace into DerivationOutputs (drv, id, path) values (?, ?, ?);",
        valid_path_id="select id from ValidPaths where path = ?;",
        valid_path_ids="select path, id from ValidPaths where path in (select value from json_each(?));",
    )


SQLITE = sqlite(provenance=False)


_POSTGRES_CLOSURE = """
WITH RECURSIVE closure(id) AS (
    SELECT id FROM ValidPaths WHERE path = ANY(%s)
    UNION
    SELECT r.reference
    FROM closure c
    JOIN Refs r ON c.id = r.referrer
)
"""

# `postgres_schema` has `provenance` and `BuildTraceV3`, and no `Realisations`: a PostgreSQL store keys its
# realisations by path. The `*_by_hash` statements name the table that is not there, and fail if a caller asks.
POSTGRES = NixSql(
    provenance=True,
    is_valid_path="SELECT 1 FROM ValidPaths WHERE path = %s LIMIT 1",
    any_valid_path="SELECT 1 FROM ValidPaths LIMIT 1",
    path_info_with_refs="""
SELECT v.path, v.deriver, v.hash, v.registrationTime, v.narSize, v.ultimate, v.sigs, v.ca, v.provenance, vp.path
FROM ValidPaths v
LEFT JOIN Refs r ON r.referrer = v.id
LEFT JOIN ValidPaths vp ON vp.id = r.reference
WHERE v.path = %s
""",
    all_valid_paths="SELECT path FROM ValidPaths",
    referrers=(
        "select path from Refs join ValidPaths on referrer = id where reference = (select id from ValidPaths where path = %s)"
    ),
    valid_derivers="select v.id, v.path from DerivationOutputs d join ValidPaths v on d.drv = v.id where d.path = %s",
    derivation_outputs=(
        "select v.id, d.id, d.path from ValidPaths v left join DerivationOutputs d on d.drv = v.id where v.path = %s"
    ),
    realisation_by_hash=(
        "select Output.path, Realisations.signatures from Realisations "
        "inner join ValidPaths as Output on Output.id = Realisations.outputPath "
        "where drvPath = %s and outputName = %s"
    ),
    realisation_by_path="select outputPath, signatures from BuildTraceV3 where drvPath = %s and outputName = %s",
    register_build_trace_by_path=(
        "insert into BuildTraceV3 (drvPath, outputName, outputPath, signatures) values (%s, %s, %s, %s)"
    ),
    update_build_trace_by_path="update BuildTraceV3 set signatures = %s where drvPath = %s and outputName = %s",
    register_realisation_by_hash=(
        "insert into Realisations (drvPath, outputName, outputPath, signatures) "
        "values (%s, %s, (select id from ValidPaths where path = %s), %s)"
    ),
    update_realisation_by_hash="update Realisations set signatures = %s where drvPath = %s and outputName = %s",
    valid_paths="""
SELECT array_agg(path) FROM ValidPaths WHERE path = ANY(%s)
""",
    path_from_hash_part="""
SELECT path FROM ValidPaths WHERE path >= %s AND path < %s LIMIT 1
""",
    path_from_nar_hash="SELECT path FROM ValidPaths WHERE hash = %s LIMIT 1",
    closure=_POSTGRES_CLOSURE
    + """SELECT array_agg(vp.path) FROM closure c
JOIN ValidPaths vp ON c.id = vp.id
""",
    closure_sizes=_POSTGRES_CLOSURE
    + """SELECT array_agg(vp.path || ':' || coalesce(vp.narSize, 0)) FROM closure c
JOIN ValidPaths vp ON c.id = vp.id
""",
    # `array_agg` in no order, as SQLite's `group_concat`: the reader takes the references as a set.
    closure_with_info=_POSTGRES_CLOSURE
    + """SELECT vp.path, vp.deriver, vp.hash, vp.registrationTime, vp.narSize,
       vp.ultimate, vp.sigs, vp.ca, vp.provenance,
       (SELECT array_agg(ref_vp.path)
        FROM Refs r
        JOIN ValidPaths ref_vp ON r.reference = ref_vp.id
        WHERE r.referrer = vp.id)
FROM closure c
JOIN ValidPaths vp ON c.id = vp.id
ORDER BY vp.id ASC
""",
    path_infos_batch="""
SELECT vp.path, vp.deriver, vp.hash, registrationTime, narSize,
       vp.ultimate, vp.sigs, vp.ca, vp.provenance
FROM ValidPaths vp
WHERE vp.path = ANY(%s)
""",
    references_batch="""
SELECT vp_referrer.path, vp_ref.path
FROM Refs r
JOIN ValidPaths vp_referrer ON r.referrer = vp_referrer.id
JOIN ValidPaths vp_ref ON r.reference = vp_ref.id
WHERE vp_referrer.path = ANY(%s)
""",
    # `dout`, not SQLite's `do`: PostgreSQL reserves `DO`.
    derivation_output_map_batch="""
SELECT vp_drv.path, dout.id, dout.path
FROM DerivationOutputs dout
JOIN ValidPaths vp_drv ON dout.drv = vp_drv.id
WHERE vp_drv.path = ANY(%s)
""",
    register_valid_path=(
        "insert into ValidPaths (path, hash, registrationTime, deriver, narSize, ultimate, sigs, ca, provenance) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s) returning id"
    ),
    update_path_info="update ValidPaths set narSize = %s, hash = %s, ultimate = %s, sigs = %s, ca = %s where path = %s",
    path_info_for_update="select narSize, hash, ultimate, sigs, ca from ValidPaths where path = %s",
    invalidate_path="delete from ValidPaths where path = %s",
    # SQLite's `insert or replace`: a `Refs` row holds only its key, and a `DerivationOutputs` row takes the new path.
    add_reference="insert into Refs (referrer, reference) values (%s, %s) on conflict do nothing",
    add_derivation_output=(
        "insert into DerivationOutputs (drv, id, path) values (%s, %s, %s) "
        "on conflict (drv, id) do update set path = excluded.path"
    ),
    valid_path_id="select id from ValidPaths where path = %s",
    valid_path_ids="select path, id from ValidPaths where path = any(%s)",
)
