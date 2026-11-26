"""The statements over pynixd's build history: `PynixdDerivationShape`, `PynixdBuildObservation` and its samples."""

from __future__ import annotations

from dataclasses import dataclass

from ..db_migrations import BUILD_OBSERVATION_SAMPLE_TABLE, BUILD_OBSERVATION_TABLE, DERIVATION_SHAPE_TABLE


@dataclass(frozen=True, slots=True)
class ObservationSql:
    insert_derivation_shape: str
    insert_build_observation: str
    insert_build_observation_sample: str
    prune_build_observation_samples: str
    """The samples of the rows `prune_build_observations` removes; it runs first, while the rows are there."""
    prune_build_observations: str
    """The rows of one identity and store past the newest `kept`, except its newest `BUILT` row: an
    estimate reads only those, so a run of failures must not take the last one."""
    build_ms_of_shape: str
    build_ms_of_shape_on_store: str
    build_ms_of_name: str
    memory_points_of_shape: str
    """The cores, peak and OOM kills of the identity's `BUILT` attempts and its killed ones, newest first."""
    memory_points_of_name: str
    cpu_of_shape: str
    """The CPU time and build time of the identity's `BUILT` builds, newest first."""
    cpu_of_name: str
    store_link: str
    inputs_bytes: str
    pull_bytes: str
    """Not the local store's rows: a local build pulls from itself and records 0 bytes. 40 local builds of a
    `qemu-root-image` made the median 0, and one remote pull of 8.6 GB took 322 s. nixidae/nixidae#1259."""


_SQLITE_PRUNED_OBSERVATIONS = f"""
    SELECT id FROM {BUILD_OBSERVATION_TABLE}
    WHERE name = :name AND system = :system AND shape = :shape AND storeId = :store AND id NOT IN (
        SELECT id FROM {BUILD_OBSERVATION_TABLE}
        WHERE name = :name AND system = :system AND shape = :shape AND storeId = :store
        ORDER BY id DESC LIMIT :kept
    ) AND id IS NOT (
        SELECT max(id) FROM {BUILD_OBSERVATION_TABLE}
        WHERE name = :name AND system = :system AND shape = :shape AND storeId = :store AND status = 0
    )
"""

SQLITE = ObservationSql(
    insert_derivation_shape=f"""
INSERT OR IGNORE INTO {DERIVATION_SHAPE_TABLE} (shape, keys, features) VALUES (?, ?, ?)
""",
    insert_build_observation=f"""
INSERT INTO {BUILD_OBSERVATION_TABLE}
(name, system, shape, storeId, status, queuedMs, inputsMs, inputsBytes, buildMs,
 cpuUserUs, cpuSystemUs, pullMs, pullBytes, resourceSource, peakMemoryBytes, ioReadBytes, ioWriteBytes,
 cpuStallUs, memoryStallUs, ioStallUs, hostResourceSource, hostCpuUserUs, hostCpuSystemUs, hostPeakMemoryBytes,
 cores, oomKills, finishedAt)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, unixepoch())
""",
    insert_build_observation_sample=f"""
INSERT INTO {BUILD_OBSERVATION_SAMPLE_TABLE}
(observationId, atMs, cpuUs, memoryBytes, ioReadBytes, ioWriteBytes) VALUES (?, ?, ?, ?, ?, ?)
""",
    prune_build_observation_samples=f"""
DELETE FROM {BUILD_OBSERVATION_SAMPLE_TABLE} WHERE observationId IN ({_SQLITE_PRUNED_OBSERVATIONS})
""",
    prune_build_observations=f"""
DELETE FROM {BUILD_OBSERVATION_TABLE} WHERE id IN ({_SQLITE_PRUNED_OBSERVATIONS})
""",
    build_ms_of_shape=f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT ?
""",
    build_ms_of_shape_on_store=f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND storeId = ? AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT ?
""",
    build_ms_of_name=f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT ?
""",
    memory_points_of_shape=f"""
SELECT cores, coalesce(hostPeakMemoryBytes, peakMemoryBytes), oomKills FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND (status = 0 OR oomKills > 0)
AND coalesce(hostPeakMemoryBytes, peakMemoryBytes) IS NOT NULL
ORDER BY id DESC LIMIT ?
""",
    memory_points_of_name=f"""
SELECT cores, coalesce(hostPeakMemoryBytes, peakMemoryBytes), oomKills FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND (status = 0 OR oomKills > 0)
AND coalesce(hostPeakMemoryBytes, peakMemoryBytes) IS NOT NULL
ORDER BY id DESC LIMIT ?
""",
    cpu_of_shape=f"""
SELECT cpuUserUs, cpuSystemUs, buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND status = 0 AND cpuUserUs IS NOT NULL AND buildMs > 0
ORDER BY id DESC LIMIT ?
""",
    cpu_of_name=f"""
SELECT cpuUserUs, cpuSystemUs, buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND status = 0 AND cpuUserUs IS NOT NULL AND buildMs > 0
ORDER BY id DESC LIMIT ?
""",
    store_link=f"""
SELECT status, inputsBytes, inputsMs, pullBytes, pullMs, buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE storeId = ?
ORDER BY id DESC LIMIT ?
""",
    inputs_bytes=f"""
SELECT inputsBytes FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND storeId = ? AND inputsBytes IS NOT NULL
ORDER BY id DESC LIMIT ?
""",
    pull_bytes=f"""
SELECT pullBytes FROM {BUILD_OBSERVATION_TABLE}
WHERE name = ? AND system = ? AND shape = ? AND storeId != ? AND pullBytes IS NOT NULL
ORDER BY id DESC LIMIT ?
""",
)


_POSTGRES_PRUNED_OBSERVATIONS = f"""
    SELECT id FROM {BUILD_OBSERVATION_TABLE}
    WHERE name = %(name)s AND system = %(system)s AND shape = %(shape)s AND storeId = %(store)s AND id NOT IN (
        SELECT id FROM {BUILD_OBSERVATION_TABLE}
        WHERE name = %(name)s AND system = %(system)s AND shape = %(shape)s AND storeId = %(store)s
        ORDER BY id DESC LIMIT %(kept)s
    ) AND id IS DISTINCT FROM (
        SELECT max(id) FROM {BUILD_OBSERVATION_TABLE}
        WHERE name = %(name)s AND system = %(system)s AND shape = %(shape)s AND storeId = %(store)s AND status = 0
    )
"""

POSTGRES = ObservationSql(
    insert_derivation_shape=f"""
INSERT INTO {DERIVATION_SHAPE_TABLE} (shape, keys, features) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
""",
    insert_build_observation=f"""
INSERT INTO {BUILD_OBSERVATION_TABLE}
(name, system, shape, storeId, status, queuedMs, inputsMs, inputsBytes, buildMs,
 cpuUserUs, cpuSystemUs, pullMs, pullBytes, resourceSource, peakMemoryBytes, ioReadBytes, ioWriteBytes,
 cpuStallUs, memoryStallUs, ioStallUs, hostResourceSource, hostCpuUserUs, hostCpuSystemUs, hostPeakMemoryBytes,
 cores, oomKills, finishedAt)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
 extract(epoch FROM now())::bigint)
RETURNING id
""",
    insert_build_observation_sample=f"""
INSERT INTO {BUILD_OBSERVATION_SAMPLE_TABLE}
(observationId, atMs, cpuUs, memoryBytes, ioReadBytes, ioWriteBytes) VALUES (%s, %s, %s, %s, %s, %s)
""",
    prune_build_observation_samples=f"""
DELETE FROM {BUILD_OBSERVATION_SAMPLE_TABLE} WHERE observationId IN ({_POSTGRES_PRUNED_OBSERVATIONS})
""",
    prune_build_observations=f"""
DELETE FROM {BUILD_OBSERVATION_TABLE} WHERE id IN ({_POSTGRES_PRUNED_OBSERVATIONS})
""",
    build_ms_of_shape=f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND shape = %s AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT %s
""",
    build_ms_of_shape_on_store=f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND shape = %s AND storeId = %s AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT %s
""",
    build_ms_of_name=f"""
SELECT buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND status = 0 AND buildMs IS NOT NULL
ORDER BY id DESC LIMIT %s
""",
    memory_points_of_shape=f"""
SELECT cores, coalesce(hostPeakMemoryBytes, peakMemoryBytes), oomKills FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND shape = %s AND (status = 0 OR oomKills > 0)
AND coalesce(hostPeakMemoryBytes, peakMemoryBytes) IS NOT NULL
ORDER BY id DESC LIMIT %s
""",
    memory_points_of_name=f"""
SELECT cores, coalesce(hostPeakMemoryBytes, peakMemoryBytes), oomKills FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND (status = 0 OR oomKills > 0)
AND coalesce(hostPeakMemoryBytes, peakMemoryBytes) IS NOT NULL
ORDER BY id DESC LIMIT %s
""",
    cpu_of_shape=f"""
SELECT cpuUserUs, cpuSystemUs, buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND shape = %s AND status = 0 AND cpuUserUs IS NOT NULL AND buildMs > 0
ORDER BY id DESC LIMIT %s
""",
    cpu_of_name=f"""
SELECT cpuUserUs, cpuSystemUs, buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND status = 0 AND cpuUserUs IS NOT NULL AND buildMs > 0
ORDER BY id DESC LIMIT %s
""",
    store_link=f"""
SELECT status, inputsBytes, inputsMs, pullBytes, pullMs, buildMs FROM {BUILD_OBSERVATION_TABLE}
WHERE storeId = %s
ORDER BY id DESC LIMIT %s
""",
    inputs_bytes=f"""
SELECT inputsBytes FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND shape = %s AND storeId = %s AND inputsBytes IS NOT NULL
ORDER BY id DESC LIMIT %s
""",
    pull_bytes=f"""
SELECT pullBytes FROM {BUILD_OBSERVATION_TABLE}
WHERE name = %s AND system = %s AND shape = %s AND storeId != %s AND pullBytes IS NOT NULL
ORDER BY id DESC LIMIT %s
""",
)
