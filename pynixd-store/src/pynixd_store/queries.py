"""SQLite queries for the local store database.

Extracted from the old operation modules to break the dependency on the legacy
operation classes.  These are pure string constants used by
LocalDBStore for SQLite fast-path queries.
"""

from __future__ import annotations

from dataclasses import dataclass

IS_VALID_PATH = "SELECT 1 FROM ValidPaths WHERE path = ? LIMIT 1"

_QUERY_PATH_INFO_WITH_REFS = """
SELECT v.path, v.deriver, v.hash, v.registrationTime, v.narSize, v.ultimate, v.sigs, v.ca, {provenance}, vp.path
FROM ValidPaths v
LEFT JOIN Refs r ON r.referrer = v.id
LEFT JOIN ValidPaths vp ON vp.id = r.reference
WHERE v.path = ?
"""

QUERY_ALL_VALID_PATHS = "SELECT path FROM ValidPaths"

# Nix's own statement, `local-store.cc:364`. A path the store does not hold
# has no id, so it has no referrers, and a path that refers to itself is one.
QUERY_REFERRERS = "select path from Refs join ValidPaths on referrer = id where reference = (select id from ValidPaths where path = ?)"

# Nix's own statement, `local-store.cc:371`. A deriver counts while it is
# valid, whether or not the output it names is.
QUERY_VALID_DERIVERS = "select v.id, v.path from DerivationOutputs d join ValidPaths v on d.drv = v.id where d.path = ?"

# `local-store.cc:372`, with the drv's id looked up in the same statement. No
# row is a drv that is not valid, which Nix refuses (`queryValidPathId`).
QUERY_DERIVATION_OUTPUTS = (
    "select v.id, d.id, d.path from ValidPaths v left join DerivationOutputs d on d.drv = v.id where v.path = ?"
)

# Nix's own statements for a realisation. Nix 2.34 and 2.35 key `Realisations`
# by the hash of the derivation (`local-store.cc:377` of 2.34); Nix master keys
# `BuildTraceV3` by its path, without the store directory (`local-store.cc:407`).
QUERY_REALISATION_BY_HASH = (
    "select Output.path, Realisations.signatures from Realisations "
    "inner join ValidPaths as Output on Output.id = Realisations.outputPath "
    "where drvPath = ? and outputName = ?"
)
QUERY_REALISATION_BY_PATH = "select outputPath, signatures from BuildTraceV3 where drvPath = ? and outputName = ?"

# `registerDrvOutput` (`local-store.cc:664`). The feature picks the table, as
# the reads above do. `BuildTraceV3.outputPath` is the whole path as text; Nix
# 2.34's `Realisations.outputPath` is a `ValidPaths` id, filled by the
# subquery (`local-store.cc:358`). Both bind the signatures as a space-joined
# string, empty when there are none (no NULL flag, unlike `ValidPaths.sigs`).
REGISTER_BUILD_TRACE_BY_PATH = (
    "insert into BuildTraceV3 (drvPath, outputName, outputPath, signatures) values (?, ?, ?, ?)"
)
UPDATE_BUILD_TRACE_BY_PATH = "update BuildTraceV3 set signatures = ? where drvPath = ? and outputName = ?"
REGISTER_REALISATION_BY_HASH = (
    "insert into Realisations (drvPath, outputName, outputPath, signatures) "
    "values (?, ?, (select id from ValidPaths where path = ?), ?)"
)
UPDATE_REALISATION_BY_HASH = "update Realisations set signatures = ? where drvPath = ? and outputName = ?"

QUERY_VALID_PATHS = """
SELECT path FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
"""

QUERY_PATH_FROM_HASH_PART = """
SELECT path FROM ValidPaths WHERE path >= ? AND path < ? LIMIT 1
"""

QUERY_PATH_FROM_NAR_HASH = "SELECT path FROM ValidPaths WHERE hash = ? LIMIT 1"

QUERY_CLOSURE = """
WITH RECURSIVE closure(id) AS (
    SELECT id FROM ValidPaths WHERE path IN (SELECT value FROM json_each(?))
    UNION
    SELECT r.reference
    FROM closure c
    JOIN Refs r ON c.id = r.referrer
)
SELECT vp.path FROM closure c
JOIN ValidPaths vp ON c.id = vp.id
"""

_QUERY_CLOSURE_WITH_INFO = """
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

_QUERY_PATH_INFOS_BATCH = """
SELECT vp.path, vp.deriver, vp.hash, registrationTime, narSize,
       vp.ultimate, vp.sigs, vp.ca, {provenance}
FROM ValidPaths vp
WHERE vp.path IN (SELECT value FROM json_each(?))
"""

QUERY_REFERENCES_BATCH = """
SELECT vp_referrer.path, vp_ref.path
FROM Refs r
JOIN ValidPaths vp_referrer ON r.referrer = vp_referrer.id
JOIN ValidPaths vp_ref ON r.reference = vp_ref.id
WHERE vp_referrer.path IN (SELECT value FROM json_each(?))
"""

QUERY_DERIVATION_OUTPUT_MAP_BATCH = """
SELECT vp_drv.path, do.id, do.path
FROM DerivationOutputs do
JOIN ValidPaths vp_drv ON do.drv = vp_drv.id
WHERE vp_drv.path IN (SELECT value FROM json_each(?))
"""

# Nix's own statements for a registration, `local-store.cc:331-348` of 2.34,
# the same on master. Every write to Nix's tables uses one of these.
_REGISTER_VALID_PATH = "insert into ValidPaths (path, hash, registrationTime, deriver, narSize, ultimate, sigs, ca{provenance}) values (?, ?, ?, ?, ?, ?, ?, ?{value});"
UPDATE_PATH_INFO = "update ValidPaths set narSize = ?, hash = ?, ultimate = ?, sigs = ?, ca = ? where path = ?;"
# The fields `updatePathInfo` writes back, read for `addSignatures`: it changes only `sigs` and keeps the rest.
QUERY_PATH_INFO_FOR_UPDATE = "select narSize, hash, ultimate, sigs, ca from ValidPaths where path = ?;"
# `LocalStore::invalidatePath` (`local-store.cc:1016`). The `Refs` foreign keys
# cascade the referrer's entries away, so the connection must enforce them.
INVALIDATE_PATH = "delete from ValidPaths where path = ?;"
ADD_REFERENCE = "insert or replace into Refs (referrer, reference) values (?, ?);"
ADD_DERIVATION_OUTPUT = "insert or replace into DerivationOutputs (drv, id, path) values (?, ?, ?);"
QUERY_VALID_PATH_ID = "select id from ValidPaths where path = ?;"


@dataclass(frozen=True, slots=True)
class PathInfoStatements:
    """The statements that read or write a whole `ValidPaths` row, for a table with or without `provenance`.

    Determinate Nix adds the column with its `provenance` experimental feature, in the migration
    `20241024-provenance` (`local-store.cc:679` of v3.24.0). Without it each read answers `NULL` in its place, so
    a row has one shape: the eight columns of Nix, then provenance. `UPDATE_PATH_INFO` leaves provenance alone, as
    Nix's does (`local-store.cc:388`).
    """

    provenance: bool
    path_info_with_refs: str
    closure_with_info: str
    path_infos_batch: str
    register_valid_path: str


def path_info_statements(*, provenance: bool) -> PathInfoStatements:
    column = "{table}.provenance" if provenance else "NULL"
    return PathInfoStatements(
        provenance=provenance,
        path_info_with_refs=_QUERY_PATH_INFO_WITH_REFS.format(provenance=column.format(table="v")),
        closure_with_info=_QUERY_CLOSURE_WITH_INFO.format(provenance=column.format(table="vp")),
        path_infos_batch=_QUERY_PATH_INFOS_BATCH.format(provenance=column.format(table="vp")),
        register_valid_path=_REGISTER_VALID_PATH.format(
            provenance=", provenance" if provenance else "", value=", ?" if provenance else ""
        ),
    )


WITHOUT_PROVENANCE = path_info_statements(provenance=False)
