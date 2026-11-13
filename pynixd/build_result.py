"""`BuildResult` values that every stage of the build pipeline makes and reads: below goals, the scheduler and execution."""

from __future__ import annotations

from .serde import BuildResult, BuildResultStatus


def failure(message: str, status: BuildResultStatus = BuildResultStatus.MISC_FAILURE) -> BuildResult:
    """Create a BuildResult representing a failure with the given message and status."""
    return BuildResult(
        status=status,
        error_msg=message,
        times_built=0,
        is_non_deterministic=0,
        start_time=0,
        stop_time=0,
        built_outputs={},
    )


def success(status: BuildResultStatus = BuildResultStatus.ALREADY_VALID) -> BuildResult:
    """Create a BuildResult representing a successful outcome."""
    return BuildResult(
        status=status,
        error_msg="",
        times_built=0,
        is_non_deterministic=0,
        start_time=0,
        stop_time=0,
        built_outputs={},
    )


def result_succeeded(result: BuildResult) -> bool:
    """Return True if the BuildResult status indicates success."""
    try:
        return BuildResultStatus(result.status).is_success
    except ValueError:
        return False
