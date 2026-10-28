"""The path that `BuildDerivation` names, after pynixd resolves the derivation.

**The daemon reads the derivation on the disk, and not the one the wire
carries.** `DerivationBuildingGoal::queryPartialDerivationOutputMap` at
`derivation-building-goal.cc:1239` takes the store copy whenever `drvPath` is
valid there, and the client put the original derivation in the store when it
instantiated it. A resolved derivation sent under the original path therefore
answered every question about its outputs from the unresolved one.

Nix writes the resolved derivation to the store and builds that path, at
`derivation-resolution-goal.cc`. These tests state that pynixd does the same.

**Nix does that for some derivations only, and `Derivation::shouldResolve` at
`derivations.cc:1129` names them.** An input-addressed output belongs to the
hash of the derivation that names it, so the resolved derivation needs a
different path for that output, and `checkDerivationOutputs` at
`derivations.cc:1324` refuses the one it has: "derivation has incorrect output
..., should be ...". So an input-addressed derivation keeps its own path, and
a derivation whose output is deferred, floating or fixed content-addressed
gets the resolved one.

pynixd stored the resolved form of every derivation that had an input, and the
daemon refused each input-addressed one. `main:gc` of the functional suite is
where that showed.

Issue nixidae/nixidae#223.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.drv_parser import Derivation, parse_drv
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import GoalResult, goal_success
from pynixd.goals.substitute import SubstituteAttempt
from pynixd.serde import BuildMode, IsValidPathResponse, QueryRealisationRequest, QueryRealisationResponse
from pynixd.store_layout import StoreLayout
from pynixd.store_path import DrvOutput, StorePath

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine
    from pynixd.serde import BuildDerivationRequest

BASE_DRV = "/nix/store/00000000000000000000000000000001-base.drv"
TOP_DRV = "/nix/store/00000000000000000000000000000002-top.drv"
LEAF_DRV = "/nix/store/00000000000000000000000000000003-leaf.drv"
DEFER_DRV = "/nix/store/00000000000000000000000000000004-defer.drv"
BASE_OUT = "/nix/store/44444444444444444444444444444444-base"
TOP_OUT = "/nix/store/55555555555555555555555555555555-top"
LEAF_OUT = "/nix/store/66666666666666666666666666666666-leaf"
STORED = "/nix/store/99999999999999999999999999999999-defer.drv"


def _base() -> Derivation:
    """An input-addressed input, whose output is in the store already."""
    return Derivation(
        outputs=[DrvOutput(hash_algo="", hash_value="", output_name="out", path=BASE_OUT)],
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", "echo base > $out"],
        env={"out": BASE_OUT, "name": "base"},
    )


def _defer() -> Derivation:
    """A derivation whose output is deferred, so Nix resolves it and stores it.

    Three empty strings in the ATerm. The path of the output is not known
    until the inputs are built, so nothing in the derivation names it and the
    resolved form may name what it likes.
    """
    return Derivation(
        outputs=[DrvOutput(hash_algo="", hash_value="", output_name="out", path="")],
        input_drvs={StorePath(BASE_DRV): ["out"]},
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", f"cat {BASE_OUT} > $out"],
        env={"out": "", "name": "defer"},
    )


def _top() -> Derivation:
    """An input-addressed derivation with an input derivation.

    Its output path is the hash of this derivation, so the resolved form of it
    cannot carry that path. It keeps its own path.
    """
    return Derivation(
        outputs=[DrvOutput(hash_algo="", hash_value="", output_name="out", path=TOP_OUT)],
        input_drvs={StorePath(BASE_DRV): ["out"]},
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", f"cat {BASE_OUT} > $out"],
        env={"out": TOP_OUT, "name": "top"},
    )


def _leaf() -> Derivation:
    """A derivation with no input derivation. Nothing is left to resolve."""
    return Derivation(
        outputs=[DrvOutput(hash_algo="", hash_value="", output_name="out", path=LEAF_OUT)],
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", "echo leaf > $out"],
        env={"out": LEAF_OUT, "name": "leaf"},
    )


class FakeLocalStore:
    def __init__(self, *, takes_text: bool = True) -> None:
        self.layout = StoreLayout.chroot(None)
        self.takes_text = takes_text
        self.added: list[tuple[str, str, set[str]]] = []

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        if drv_path == BASE_DRV:
            return _base()
        if drv_path == TOP_DRV:
            return _top()
        if drv_path == LEAF_DRV:
            return _leaf()
        if drv_path == DEFER_DRV:
            return _defer()
        return None

    async def add_text_to_store(self, name: str, text: str, references: Any) -> str:
        if not self.takes_text:
            raise RuntimeError("this store cannot add a text file")
        self.added.append((name, text, {str(item) for item in references}))
        return STORED

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        """The output of the input is in the store, and no other output is."""
        del kwargs
        if isinstance(request, QueryRealisationRequest):
            # No output is realised, so every derivation here is built.
            return QueryRealisationResponse(realisations=[])
        return IsValidPathResponse(valid=str(request.path) == BASE_OUT)


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


class FakeSubstituteGoal:
    may_reach_a_root_goal = False
    """A substitute goal reaches no root goal. Issue nixidae/nixidae#246."""

    async def result(self) -> SubstituteAttempt:
        return SubstituteAttempt(found=False, result=goal_success())


class FakeEngine:
    def __init__(self, *, takes_text: bool = True) -> None:
        self.local_store = FakeLocalStore(takes_text=takes_text)
        self.ctx = cast("Any", _Ctx(self.local_store))
        self.build_goals: list[FakeBuildGoal] = []

    async def get_build_derivation_goal(self, request: BuildDerivationRequest) -> FakeBuildGoal:
        goal = FakeBuildGoal(request)
        self.build_goals.append(goal)
        return goal

    async def get_ensure_derived_path_goal(self, dp: Any, build_mode: int, substituter_ids: Any) -> Any:
        del dp, build_mode, substituter_ids
        raise AssertionError("the output of the input is valid, so no child goal runs")

    async def get_substitute_path_goal(self, path: StorePath, substituter_ids: tuple[str, ...]) -> Any:
        del path, substituter_ids
        return FakeSubstituteGoal()


class _Ctx:
    def __init__(self, local_store: FakeLocalStore) -> None:
        self.local_store = local_store


async def _request(drv_path: str = DEFER_DRV, *, takes_text: bool = True) -> tuple[FakeEngine, Any]:
    engine = FakeEngine(takes_text=takes_text)
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{drv_path}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    await goal.result()
    assert len(engine.build_goals) == 1
    return engine, engine.build_goals[0].request


@pytest.mark.anyio
async def test_the_build_names_the_resolved_derivation() -> None:
    engine, request = await _request()

    assert str(request.drv_path) == STORED
    assert str(request.drv_path) != DEFER_DRV
    assert len(engine.local_store.added) == 1


@pytest.mark.anyio
async def test_the_name_keeps_the_drv_suffix() -> None:
    """`writeDerivation` of Nix uses `drv.name + ".drv"`, at `derivations.cc:109`."""
    engine, _ = await _request()
    name, _text, _refs = engine.local_store.added[0]

    assert name == "defer.drv"


@pytest.mark.anyio
async def test_the_stored_text_is_the_derivation_that_the_wire_carries() -> None:
    """The daemon reads this text, so it must say what the wire says."""
    engine, request = await _request()
    _name, text, _refs = engine.local_store.added[0]
    stored = parse_drv(text)

    assert stored.input_drvs == {}
    assert {str(path) for path in stored.input_srcs} == {BASE_OUT}
    assert stored.output_paths()["out"] == request.derivation.output_paths()["out"]
    assert stored.args == request.derivation.args


@pytest.mark.anyio
async def test_the_references_are_the_sources_of_the_derivation() -> None:
    """`infoForDerivation` of Nix uses `inputSrcs` and the input derivations.

    A resolved derivation has no input derivation, so `inputSrcs` is the whole
    set. A reference that is missing makes the garbage collector remove an
    input that the build needs.
    """
    engine, _ = await _request()
    _name, _text, refs = engine.local_store.added[0]

    assert refs == {BASE_OUT}


@pytest.mark.anyio
async def test_a_derivation_with_no_input_keeps_its_own_path() -> None:
    """Nothing is resolved, so the derivation on the disk is the right one."""
    engine, request = await _request(LEAF_DRV)

    assert str(request.drv_path) == LEAF_DRV
    assert engine.local_store.added == []


@pytest.mark.anyio
async def test_an_input_addressed_derivation_keeps_its_own_path() -> None:
    """Its output path states the hash of this derivation, and not of another.

    The daemon refuses the resolved form: "derivation has incorrect output
    ..., should be ...", at `derivations.cc:1324`. Nix does not write one
    either, because `shouldResolve` answers false for it.
    """
    engine, request = await _request(TOP_DRV)

    assert str(request.drv_path) == TOP_DRV
    assert engine.local_store.added == []


@pytest.mark.anyio
async def test_the_wire_still_carries_the_resolved_derivation() -> None:
    """`BuildDerivation` holds a `BasicDerivation`, which names no input drv.

    The path that the request names and the derivation that it carries are two
    decisions. This one does not change.
    """
    _engine, request = await _request(TOP_DRV)

    assert BASE_OUT in {str(path) for path in request.derivation.input_srcs}


@pytest.mark.anyio
async def test_a_store_that_takes_no_text_keeps_the_original_path() -> None:
    """The build then reads the unresolved derivation, which is what pynixd did before.

    A backend store holds the output of a fleet build, and the local store
    refuses a reference it does not have. The build is worse, and it is not
    lost.
    """
    _engine, request = await _request(takes_text=False)

    assert str(request.drv_path) == DEFER_DRV
