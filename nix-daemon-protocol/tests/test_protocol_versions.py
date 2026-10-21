"""Fast compatibility-matrix tests for the daemon codec package."""

from __future__ import annotations

import pytest

from nix_daemon_protocol import (
    SUPPORTED_PROTOCOL_VERSIONS,
    AddBuildLogRequest,
    AddMultipleToStoreRequest,
    AddPermRootRequest,
    BuildPathsWithResultsRequest,
    BuildResult,
    OptMicroseconds,
    UnsupportedProtocolVersion,
    is_supported_protocol,
    proto,
)
from nix_daemon_protocol.context import WriteContext
from nix_daemon_protocol.io import BytesWriter
from nix_daemon_protocol.wire_ops import WireRequest  # noqa: TC001


@pytest.mark.parametrize("minor", range(32, 39))
def test_every_declared_protocol_version_is_supported(minor: int) -> None:
    """The compatibility contract is the contiguous 1.32 through 1.38 range."""
    assert is_supported_protocol(proto(1, minor))


def test_protocol_support_has_explicit_floor_and_ceiling() -> None:
    assert tuple(proto(1, minor) for minor in range(32, 39)) == SUPPORTED_PROTOCOL_VERSIONS
    assert not is_supported_protocol(proto(1, 31))
    assert not is_supported_protocol(proto(1, 39))


@pytest.mark.parametrize(
    ("wire_request", "first_supported_minor"),
    [
        (AddMultipleToStoreRequest(repair=0, dont_check_sigs=0), 32),
        (AddBuildLogRequest(path="example-log"), 32),
        (BuildPathsWithResultsRequest(derived_paths=[], build_mode=0), 34),
        (AddPermRootRequest(store_path="/nix/store/example-root", gc_root="/tmp/root"), 36),
    ],
)
async def test_operation_availability_respects_its_protocol_boundary(
    wire_request: WireRequest, first_supported_minor: int
) -> None:
    """Operations added after 1.32 cannot be sent to an older daemon."""
    writer = BytesWriter()
    if first_supported_minor > 32:
        with pytest.raises(UnsupportedProtocolVersion):
            await wire_request.to_writer(WriteContext(writer=writer, version=proto(1, first_supported_minor - 1)))

    await wire_request.to_writer(WriteContext(writer=writer, version=proto(1, first_supported_minor)))


async def test_build_result_cpu_fields_start_at_protocol_137() -> None:
    """Protocol 1.37 adds CPU timing fields to BuildResult."""
    result = BuildResult(
        status=0,
        error_msg="",
        times_built=0,
        is_non_deterministic=0,
        start_time=0,
        stop_time=0,
        built_outputs={},
        cpu_user=OptMicroseconds(tag=1, value=10),
        cpu_system=OptMicroseconds(tag=1, value=20),
    )
    before = BytesWriter()
    after = BytesWriter()

    await result.to_writer(WriteContext(writer=before, version=proto(1, 36)))
    await result.to_writer(WriteContext(writer=after, version=proto(1, 37)))

    assert after.get_bytes() != before.get_bytes()
    assert len(after.get_bytes()) > len(before.get_bytes())
