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
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import GoalResult, goal_success
from pynixd.serde import BuildMode, IsValidPathResponse, QueryRealisationRequest, QueryRealisationResponse
from pynixd.trust import TrustPolicy
from pynixd_store.drv_parser import Derivation
from pynixd_store.store_layout import StoreLayout
from pynixd_store.store_path import DrvOutput, StorePath
from tests.goal_fakes import FakeChildGoal as SharedChildGoal, FakeGoalEngine
from tests.goal_group import result_of

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine

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
        input_drvs={StorePath(IMPURE_DRV): ["out"]},
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


class FakeChildGoal(SharedChildGoal):
    async def result(self) -> GoalResult:
        result = goal_success()
        result.resolved_outputs = {"out": StorePath(PURE_OUT)}
        return result.with_dynamic_outputs(StorePath(IMPURE_DRV))


class FakeContext:
    def __init__(self) -> None:
        self.local_store = FakeLocalStore()
        self.trust = TrustPolicy(experimental_features=("ca-derivations", "impure-derivations"))


class FakeEngine(FakeGoalEngine):
    child_goal = FakeChildGoal

    def __init__(self) -> None:
        super().__init__()
        self.ctx = FakeContext()


async def _run(drv_path: str) -> tuple[FakeEngine, GoalResult]:
    engine = FakeEngine()
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{drv_path}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    return engine, await result_of(goal)


@pytest.mark.anyio
async def test_a_pure_derivation_with_an_impure_input_is_refused() -> None:
    engine, result = await _run(PURE_DRV)

    message = f"pure derivation '{PURE_DRV}' depends on impure derivation '{IMPURE_DRV}'"
    assert str(result.result.error_msg) == message
    assert result.raised == message
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
