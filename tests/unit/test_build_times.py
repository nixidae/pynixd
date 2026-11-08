"""`with_build_times` fills only what a builder left at Nix's 0, so a fixed Nix's own values win."""

from __future__ import annotations

from pynixd.scheduler import with_build_times
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus


def _response(status: BuildResultStatus, times_built: int, start: int, stop: int) -> BuildDerivationResponse:
    return BuildDerivationResponse(
        result=BuildResult(
            status=status,
            error_msg="",
            times_built=times_built,
            is_non_deterministic=0,
            start_time=start,
            stop_time=stop,
            built_outputs={},
        )
    )


def test_a_build_nix_answered_with_zeros_gets_the_measured_times() -> None:
    result = with_build_times(_response(BuildResultStatus.BUILT, 0, 0, 0), 100, 160).result
    assert (result.times_built, result.start_time, result.stop_time) == (1, 100, 160)


def test_a_builder_that_fills_the_times_keeps_its_own() -> None:
    result = with_build_times(_response(BuildResultStatus.BUILT, 1, 90, 150), 100, 160).result
    assert (result.times_built, result.start_time, result.stop_time) == (1, 90, 150)


def test_a_result_that_built_nothing_keeps_its_zeros() -> None:
    for status in (BuildResultStatus.ALREADY_VALID, BuildResultStatus.INPUT_REJECTED, BuildResultStatus.MISC_FAILURE):
        result = with_build_times(_response(status, 0, 0, 0), 100, 160).result
        assert (result.times_built, result.start_time, result.stop_time) == (0, 0, 0), status
