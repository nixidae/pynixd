"""A pure derivation cannot depend on an impure one.

`DerivationResolutionGoal::init` at `derivation-resolution-goal.cc:67` reads
each input derivation and raises before it builds any of them. pynixd owns the
goal system, so pynixd owns the check.

`impure-derivations.sh:50` of the functional suite greps for the message, and
`inputAddressed` of `impure-derivations.nix` is the derivation that must be
refused.

Two derivations may depend on an impure one: an impure derivation, and a
fixed-output one. Neither takes its output path from the hash of its inputs.

Refs nixidae/nixidae#214.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.drv_parser import Derivation
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import GoalResult, goal_success
from pynixd.goals.substitute import SubstituteAttempt
from pynixd.serde import BuildMode, IsValidPathResponse, QueryRealisationRequest, QueryRealisationResponse
from pynixd.store_layout import StoreLayout
from pynixd.store_path import DrvOutput, StorePath

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine
    from pynixd.serde import BuildDerivationRequest

IMPURE_DRV = "/nix/store/00000000000000000000000000000001-impure.drv"
PURE_DRV = "/nix/store/00000000000000000000000000000002-input-addressed.drv"
FIXED_DRV = "/nix/store/00000000000000000000000000000003-fixed.drv"
IMPURE_ON_IMPURE_DRV = "/nix/store/00000000000000000000000000000004-impure-on-impure.drv"

PURE_OUT = "/nix/store/22222222222222222222222222222222-input-addressed"
FIXED_OUT = "/nix/store/33333333333333333333333333333333-fixed"


def _impure() -> Derivation:
    """`("out","","r:sha256","impure")` is the output that Nix writes."""
    return Derivation(
        outputs=[DrvOutput(hash_algo="r:sha256", hash_value="impure", output_name="out", path="")],
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", "echo hi > $out"],
        env={"out": "", "name": "impure"},
    )


def _consumer(name: str, drv_path: str, output: DrvOutput) -> Derivation:
    return Derivation(
        outputs=[output],
        input_drvs={StorePath(IMPURE_DRV): ["out"]},  # pyright: ignore[reportArgumentType] -- StorePath is a str
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", f"cat {drv_path} > $out"],
        env={"out": str(output.path), "name": name},
    )


_DERIVATIONS = {
    IMPURE_DRV: _impure,
    PURE_DRV: lambda: _consumer(
        "input-addressed",
        PURE_DRV,
        DrvOutput(hash_algo="", hash_value="", output_name="out", path=PURE_OUT),
    ),
    FIXED_DRV: lambda: _consumer(
        "fixed",
        FIXED_DRV,
        DrvOutput(hash_algo="r:sha256", hash_value="00" * 32, output_name="out", path=FIXED_OUT),
    ),
    IMPURE_ON_IMPURE_DRV: lambda: _consumer(
        "impure-on-impure",
        IMPURE_ON_IMPURE_DRV,
        DrvOutput(hash_algo="r:sha256", hash_value="impure", output_name="out", path=""),
    ),
}


class FakeLocalStore:
    def __init__(self) -> None:
        self.layout = StoreLayout.chroot(None)

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        make = _DERIVATIONS.get(str(drv_path))
        return make() if make is not None else None

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        """No output is valid and no output is realised, so each one builds."""
        del kwargs
        if isinstance(request, QueryRealisationRequest):
            return QueryRealisationResponse(realisations=[])
        return IsValidPathResponse(valid=False)


class FakeBuildGoal:
    may_reach_a_root_goal = False
    """A build goal reaches no root goal, so a caller keeps its place. Issue nixidae/nixidae#246."""

    def __init__(self, request: BuildDerivationRequest) -> None:
        self.request = request

    async def subscribe(self, client: Any) -> None:
        del client

    async def start(self) -> None:
        """`Goal.start` begins the build and does not wait. Issue nixidae/nixidae#246."""

    async def wait_until_it_reached_the_queue(self) -> None:
        """This fake needs no queue, so the build is on it at once."""

    async def result(self) -> GoalResult:
        return goal_success()


class FakeChildGoal:
    may_reach_a_root_goal = True
    """The goal of an input can reach a root goal of the request. Issue nixidae/nixidae#246."""

    def note_a_parent(self) -> None:
        """The goal of an input has a goal that waits for it."""

    async def subscribe_many(self, clients: list[Any]) -> None:
        del clients

    async def result(self) -> GoalResult:
        result = goal_success()
        result.resolved_outputs = {"out": StorePath(PURE_OUT)}
        return result.with_dynamic_outputs(StorePath(IMPURE_DRV))


class FakeSubstituteGoal:
    may_reach_a_root_goal = False
    """A substitute goal reaches no root goal. Issue nixidae/nixidae#246."""

    async def result(self) -> SubstituteAttempt:
        return SubstituteAttempt(found=False, result=goal_success())


class FakeContext:
    def __init__(self) -> None:
        self.local_store = FakeLocalStore()


class FakeEngine:
    def __init__(self) -> None:
        self.ctx = FakeContext()
        self.build_goals: list[FakeBuildGoal] = []

    async def get_build_derivation_goal(self, request: BuildDerivationRequest) -> FakeBuildGoal:
        goal = FakeBuildGoal(request)
        self.build_goals.append(goal)
        return goal

    async def get_ensure_derived_path_goal(self, dp: Any, build_mode: int, substituter_ids: Any) -> FakeChildGoal:
        del dp, build_mode, substituter_ids
        return FakeChildGoal()

    async def get_substitute_path_goal(self, path: StorePath, substituter_ids: tuple[str, ...]) -> Any:
        del path, substituter_ids
        return FakeSubstituteGoal()


async def _run(drv_path: str) -> tuple[FakeEngine, GoalResult]:
    engine = FakeEngine()
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{drv_path}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    return engine, await goal.result()


@pytest.mark.anyio
async def test_a_pure_derivation_with_an_impure_input_is_refused() -> None:
    engine, result = await _run(PURE_DRV)

    assert str(result.result.error_msg) == (f"pure derivation '{PURE_DRV}' depends on impure derivation '{IMPURE_DRV}'")
    assert engine.build_goals == []


@pytest.mark.anyio
async def test_a_fixed_output_derivation_may_depend_on_an_impure_one() -> None:
    """Its output path comes from the hash it names, and not from its inputs."""
    engine, _ = await _run(FIXED_DRV)

    assert len(engine.build_goals) == 1


@pytest.mark.anyio
async def test_an_impure_derivation_may_depend_on_an_impure_one() -> None:
    """`impureOnImpure` of `impure-derivations.nix` is this derivation."""
    engine, _ = await _run(IMPURE_ON_IMPURE_DRV)

    assert len(engine.build_goals) == 1
