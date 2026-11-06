"""`BuildResult.resources`: pynixd's record, only under its own feature (nixidae/nixidae#1187)."""

from __future__ import annotations

from typing import Any

from nix_daemon_protocol import PROTOCOL_VERSION, BuildResult, OptMicroseconds
from nix_daemon_protocol.build_resources import BuildResources, ResourceSample
from nix_daemon_protocol.constants import FEATURE_PYNIXD_BUILD_RESOURCES
from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.io import BytesReader, BytesWriter

RESOURCES = BuildResources(
    measured=1,
    source="cgroup",
    cpu_user_us=1,
    cpu_system_us=2,
    peak_memory_bytes=3,
    io_read_bytes=4,
    io_write_bytes=5,
    cpu_stall=OptMicroseconds(tag=1, value=6),
    io_stall=OptMicroseconds(tag=1, value=7),
    samples=[
        ResourceSample(at_ms=100, cpu_us=8, memory_bytes=9, io_read_bytes=10, io_write_bytes=11),
        ResourceSample(at_ms=200, cpu_us=12, memory_bytes=13, io_read_bytes=14, io_write_bytes=15),
    ],
    host_source="rusage",
    host_cpu_user_us=16,
    host_cpu_system_us=17,
    host_peak_memory_bytes=18,
)


def _result(**fields: Any) -> BuildResult:
    """A `BuildResult` whose gated fields are set: `PROTOCOL_VERSION` writes each, and `None` cannot be."""
    return BuildResult(
        times_built=0,
        is_non_deterministic=0,
        start_time=0,
        stop_time=0,
        built_outputs={},
        built_outputs_by_name={},
        **fields,
    )


async def _bytes(result: BuildResult, features: frozenset[str]) -> bytes:
    writer = BytesWriter("test")
    await result.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION, features=features))
    return writer.get_bytes()


async def test_a_peer_without_the_feature_reads_nix_bytes() -> None:
    """A Nix client names no pynixd feature, so the record must leave Nix's `BuildResult` unchanged."""
    assert await _bytes(_result(resources=RESOURCES), frozenset()) == await _bytes(_result(), frozenset())


async def test_the_record_reads_back_under_the_feature() -> None:
    features = frozenset({FEATURE_PYNIXD_BUILD_RESOURCES})
    raw = await _bytes(_result(status=3, resources=RESOURCES), features)
    read = await BuildResult.from_reader(
        ReadContext(reader=BytesReader(raw), version=PROTOCOL_VERSION, features=features)
    )

    assert read.status == 3
    assert read.resources == RESOURCES


async def test_a_record_without_a_host_cost_reads_back() -> None:
    """A build outside a VM has no host cost: one empty string stands for its three counters."""
    features = frozenset({FEATURE_PYNIXD_BUILD_RESOURCES})
    alone = RESOURCES.model_copy(
        update={"host_source": "", "host_cpu_user_us": 0, "host_cpu_system_us": 0, "host_peak_memory_bytes": 0}
    )
    raw = await _bytes(_result(resources=alone), features)
    read = await BuildResult.from_reader(
        ReadContext(reader=BytesReader(raw), version=PROTOCOL_VERSION, features=features)
    )

    assert read.resources == alone
    assert len(raw) == len(await _bytes(_result(resources=RESOURCES), features)) - 3 * 8 - 8


async def test_an_unmeasured_record_is_one_word() -> None:
    features = frozenset({FEATURE_PYNIXD_BUILD_RESOURCES})
    assert len(await _bytes(_result(), features)) == len(await _bytes(_result(), frozenset())) + 8
