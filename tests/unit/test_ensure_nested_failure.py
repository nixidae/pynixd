"""What a client reads when the producer of a dynamic derivation fails.

`builtins.outputOf failingProducer.outPath "out"` gives a nested derived path.
The goal that makes the derivation fails, so no derivation arrives, and the
goal of the nested path has nothing to build.

`DerivationTrampolineGoal` at `derivation-trampoline-goal.cc:107` of Nix
answers "failed to obtain derivation of '<outer>'". pynixd answered
`pynixd: nested derived path did not produce out: <path>!out!out`, which names
an internal state and prints the separator that Nix does not print.

`dyn-drv/failing-outer.sh:48` reads the sentence of Nix. Refs nixidae/nixidae#214.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import GoalResult, goal_failure, result_succeeded
from pynixd.serde import BuildMode, BuildResultStatus

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine

PRODUCER = "/nix/store/00000000000000000000000000000001-failing-producer.drv"


class FakeOuterGoal:
    """The goal that makes the derivation, and it fails."""

    may_reach_a_root_goal = True
    """An ensure goal can reach a root goal of the request. Issue nixidae/nixidae#246."""

    def note_a_parent(self) -> None:
        """The goal of the nested path waits for this one."""

    async def subscribe_many(self, clients: list[Any]) -> None:
        del clients

    async def result(self) -> GoalResult:
        return goal_failure("Cannot build 'failing-producer.drv'.", BuildResultStatus.PERMANENT_FAILURE)


class FakeEngine:
    ctx = None

    async def get_ensure_derived_path_goal(self, dp: Any, build_mode: int, substituter_ids: Any) -> FakeOuterGoal:
        del dp, build_mode, substituter_ids
        return FakeOuterGoal()


def _goal() -> EnsureDerivedPathGoal:
    return EnsureDerivedPathGoal(
        engine=cast("GoalEngine", FakeEngine()),
        derived_path=DerivedPath(f"{PRODUCER}!out!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )


@pytest.mark.anyio
async def test_the_message_names_the_derivation_that_never_arrived() -> None:
    result = await _goal().result()

    assert not result_succeeded(result.result)
    assert str(result.result.error_msg) == f"failed to obtain derivation of '{PRODUCER}^out'"


@pytest.mark.anyio
async def test_the_status_says_a_dependency_failed() -> None:
    """`derivation-trampoline-goal.cc:105` gives `DependencyFailed`."""
    result = await _goal().result()

    assert result.result.status == BuildResultStatus.DEPENDENCY_FAILED
