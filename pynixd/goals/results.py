"""BuildResult helpers for goal implementations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..serde import BuildResult, BuildResultStatus

if TYPE_CHECKING:
    from ..store_path import StorePath


type DynamicPathMap = dict[tuple["StorePath | str", ...], "StorePath"]


@dataclass
class GoalResult:
    """Aggregated result of a goal execution, including build result and output metadata."""

    result: BuildResult
    resolved_outputs: dict[str, StorePath] = field(default_factory=dict)
    produced_paths: set[StorePath] = field(default_factory=set)
    dynamic_paths: DynamicPathMap = field(default_factory=dict)
    #: The derivation whose own build failed and started this failure. A goal
    #: that failed for itself names itself here. A goal that failed because an
    #: input failed names that input, or whatever the input named, so the whole
    #: chain points at the one build that really failed. `None` on a success.
    #: `BuildPathsWithResultsGoal` reads it to decide which root of a request
    #: carries the answer. Issue nixidae/nixidae#235.
    failing_derivation: StorePath | None = None
    #: Whether pynixd ended this build because the request that wanted it had
    #: already stopped. Such a result is not an answer: the client asked for
    #: the derivation, pynixd chose not to build it, and Nix reports nothing
    #: for the waitees that `Goal::amDone` drops. `_run_the_root_goals` reads
    #: it and leaves the place of that root empty. Issue nixidae/nixidae#325.
    abandoned: bool = False

    def copy(self) -> GoalResult:
        """Return a shallow copy of this GoalResult with independent collections."""
        return GoalResult(
            result=self.result,
            resolved_outputs=dict(self.resolved_outputs),
            produced_paths=set(self.produced_paths),
            dynamic_paths=dict(self.dynamic_paths),
            failing_derivation=self.failing_derivation,
            abandoned=self.abandoned,
        )

    def with_dynamic_outputs(self, drv_path: StorePath) -> GoalResult:
        """Return a copy with resolved outputs recorded as dynamic under *drv_path*."""
        result = self.copy()
        for output_name, output_path in self.resolved_outputs.items():
            result.dynamic_paths[(drv_path, output_name)] = output_path
        return result

    def with_single_output(self, output_name: str, path: StorePath) -> GoalResult:
        """Return a copy with exactly one resolved output and the given path added to produced paths."""
        result = self.copy()
        result.resolved_outputs = {output_name: path}
        result.produced_paths.add(path)
        return result


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


def goal_failure(message: str, status: BuildResultStatus = BuildResultStatus.MISC_FAILURE) -> GoalResult:
    """Create a GoalResult wrapping a failure BuildResult."""
    return GoalResult(result=failure(message, status))


def goal_success(status: BuildResultStatus = BuildResultStatus.ALREADY_VALID) -> GoalResult:
    """Create a GoalResult wrapping a success BuildResult."""
    return GoalResult(result=success(status))
