"""The statements over pynixd's build history (`pynixd_store/sql/observations.py`) on both engines."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.build_resources import ResourceSample
from pynixd_store.build_stats import BuildEstimateSource, BuildObservation, DerivationIdentity
from pynixd_store.db_migrations import BUILD_OBSERVATION_SAMPLE_TABLE, BUILD_OBSERVATION_TABLE
from pynixd_store.local_store_db import OBSERVATIONS_KEPT

if TYPE_CHECKING:
    from pynixd_store.local_store_db import LocalStoreDB

HELLO = DerivationIdentity(name="hello", system="x86_64-linux", shape="s1", keys=("out",), features=())
RESHAPED = DerivationIdentity(name="hello", system="x86_64-linux", shape="s2", keys=("out",), features=())
BUILT = 0
FAILED = 1


def _built(store: str, build_ms: int) -> BuildObservation:
    return BuildObservation(store_id=store, status=BUILT, build_ms=build_ms)


async def _count(db: LocalStoreDB, table: str) -> int:
    row = await db.fetchone(f"SELECT count(*) FROM {table}")
    assert row is not None
    return int(row[0])


async def test_an_observation_and_its_samples_are_kept(store_db: LocalStoreDB) -> None:
    samples = (ResourceSample(at_ms=0, cpu_us=1), ResourceSample(at_ms=100, cpu_us=2, memory_bytes=1 << 33))
    first = await store_db.record_build_observation(
        HELLO, BuildObservation(store_id="local", status=BUILT, samples=samples)
    )
    second = await store_db.record_build_observation(HELLO, _built("local", 10))
    assert first is not None
    assert second is not None
    assert second > first
    assert await _count(store_db, BUILD_OBSERVATION_SAMPLE_TABLE) == 2


async def test_the_estimates_read_the_newest_built_rows(store_db: LocalStoreDB) -> None:
    for build_ms in (100, 200, 300):
        await store_db.record_build_observation(
            HELLO, BuildObservation(store_id="remote", status=BUILT, build_ms=build_ms, peak_memory_bytes=1 << 33)
        )
    await store_db.record_build_observation(HELLO, BuildObservation(store_id="remote", status=FAILED, build_ms=9))
    await store_db.record_build_observation(
        RESHAPED, BuildObservation(store_id="local", status=BUILT, build_ms=1000, host_peak_memory_bytes=1 << 34)
    )

    assert await store_db.expected_build_ms(HELLO) == 200
    assert await store_db.expected_peak_memory(HELLO) == 1 << 33
    unseen = DerivationIdentity(name="hello", system="x86_64-linux", shape="s3", keys=(), features=())
    assert await store_db.expected_build_ms(unseen) == 250
    assert await store_db.expected_peak_memory(RESHAPED) == 1 << 34
    assert await store_db.expected_build_ms_on(HELLO, "remote") == (200, BuildEstimateSource.SHAPE_ON_STORE)
    assert await store_db.expected_build_ms_on(HELLO, "local") == (200, BuildEstimateSource.SHAPE)
    assert await store_db.expected_build_ms_on(unseen, "local") == (250, BuildEstimateSource.NAME)


async def test_an_observation_keeps_its_cores(store_db: LocalStoreDB) -> None:
    row_id = await store_db.record_build_observation(HELLO, BuildObservation(store_id="local", status=BUILT, cores=12))
    assert await store_db.fetchone(f"SELECT cores FROM {BUILD_OBSERVATION_TABLE} WHERE id = {row_id}") == (12,)


async def test_the_link_and_the_bytes_read_the_store_s_rows(store_db: LocalStoreDB) -> None:
    remote = BuildObservation(
        store_id="remote", status=BUILT, build_ms=50, inputs_bytes=1000, inputs_ms=1000, pull_bytes=4000, pull_ms=2000
    )
    await store_db.record_build_observation(HELLO, remote)
    local = BuildObservation(store_id="local", status=BUILT, build_ms=7, pull_bytes=0, pull_ms=1)
    await store_db.record_build_observation(HELLO, local)

    link = await store_db.store_link("remote")
    assert (link.send_bytes_per_s, link.receive_bytes_per_s) == (1000.0, 2000.0)
    expected = await store_db.expected_bytes(HELLO, "remote", local_id="local")
    assert (expected.inputs, expected.pull) == (1000, 4000)


async def test_the_history_keeps_its_newest_rows_and_its_newest_built_row(store_db: LocalStoreDB) -> None:
    await store_db.record_build_observation(HELLO, _built("remote", 1))
    for _ in range(OBSERVATIONS_KEPT + 3):
        await store_db.record_build_observation(HELLO, BuildObservation(store_id="remote", status=FAILED))
    assert await _count(store_db, BUILD_OBSERVATION_TABLE) == OBSERVATIONS_KEPT + 1
    assert await store_db.expected_build_ms(HELLO) == 1
