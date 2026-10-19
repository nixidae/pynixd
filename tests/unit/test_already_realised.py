"""An output that the derivation cannot name, and the realisation that names it.

A floating content-addressed output takes its path from what the build makes,
so the derivation names no path for it. `EnsureDerivedPathGoal` read the
derivation alone, found no path, and built the derivation again.

`DerivationGoal::checkPathValidity` at `derivation-goal.cc:405` of Nix reads
the store instead: a realisation maps
`DrvOutput{staticOutputHashes(drv)[name], name}` to the path, and a valid path
there makes the goal `AlreadyValid`.

`ca:build` sees it in `testGC`. That part builds with `-j0` after a garbage
collection, so a second build is not allowed and the rooted output must
answer.

Issue nixidae/nixidae#224.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest
from test_drv_hash import BASE_PATH, BASE_TEXT

from pynixd.derived_path import DerivedPath
from pynixd.drv_hash import output_hashes
from pynixd.drv_parser import parse_drv
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import GoalResult, goal_success
from pynixd.goals.substitute import SubstituteAttempt
from pynixd.serde import (
    BuildMode,
    BuildResult,
    DrvOutput as SerdeDrvOutput,
    IsValidPathResponse,
    QueryRealisationRequest,
    QueryRealisationResponse,
    Realisation,
)
from pynixd.store_layout import StoreLayout
from pynixd.store_path import StorePath

if TYPE_CHECKING:
    from pynixd.drv_parser import Derivation
    from pynixd.goals.engine import GoalEngine
    from pynixd.serde import BuildDerivationRequest

FLOAT_PATH = "/nix/store/00000000000000000000000000000005-float.drv"
FLOAT_TEXT = (
    'Derive([("out","","r:sha256","")],'
    f'[("{BASE_PATH}",["out"])],[],'
    '"x86_64-linux","/bin/sh",["-c","echo hi > $out"],'
    '[("builder","/bin/sh"),("name","float"),("out",""),("system","x86_64-linux")])'
)
FLOAT_OUT = "/nix/store/77777777777777777777777777777777-float"


async def _read_drv(drv_path: str) -> Derivation | None:
    if drv_path == BASE_PATH:
        return parse_drv(BASE_TEXT)
    if drv_path == FLOAT_PATH:
        return parse_drv(FLOAT_TEXT)
    return None


async def _wanted_id() -> str:
    hashes = await output_hashes(parse_drv(FLOAT_TEXT), _read_drv, cache={})
    if hashes is None:
        raise AssertionError("the walk read both derivations, so it has an answer")
    return f"sha256:{hashes['out']}!out"


class FakeLocalStore:
    """A store that answers one realisation, and holds the path it names."""

    def __init__(self, *, realisation_id: str | None, valid: bool) -> None:
        self.layout = StoreLayout.chroot(None)
        self.realisation_id = realisation_id
        self.valid = valid
        self.asked: list[str] = []

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        return await _read_drv(drv_path)

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        del kwargs
        if isinstance(request, QueryRealisationRequest):
            self.asked.append(str(request.drv_output))
            wanted = self.realisation_id
            if wanted is None or str(request.drv_output) != wanted:
                return QueryRealisationResponse(realisations=[])
            return QueryRealisationResponse(
                realisations=[
                    Realisation(id=SerdeDrvOutput(wanted), out_path=StorePath(path=FLOAT_OUT)),
                ],
            )
        return IsValidPathResponse(valid=self.valid and str(request.path) == FLOAT_OUT)


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
        return goal_success()


class FakeSubstituteGoal:
    may_reach_a_root_goal = False
    """A substitute goal reaches no root goal. Issue nixidae/nixidae#246."""

    async def result(self) -> SubstituteAttempt:
        return SubstituteAttempt(found=False, result=goal_success())


class _Ctx:
    def __init__(self, local_store: FakeLocalStore) -> None:
        self.local_store = local_store


class FakeEngine:
    def __init__(self, *, realisation_id: str | None, valid: bool = True) -> None:
        self.local_store = FakeLocalStore(realisation_id=realisation_id, valid=valid)
        self.ctx = cast("Any", _Ctx(self.local_store))
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


async def _run(engine: FakeEngine) -> GoalResult:
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{FLOAT_PATH}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    return await goal.result()


@pytest.mark.anyio
async def test_a_realised_output_starts_no_build() -> None:
    engine = FakeEngine(realisation_id=await _wanted_id())

    result = await _run(engine)

    assert engine.build_goals == []
    assert str(result.resolved_outputs["out"]) == FLOAT_OUT


def _outputs(result: BuildResult) -> dict[str, Realisation]:
    """`built_outputs` is optional on the wire, and every response here sets it."""
    if result.built_outputs is None:
        raise AssertionError("the response carries built outputs")
    return result.built_outputs


@pytest.mark.anyio
async def test_the_answer_carries_the_realisation() -> None:
    """A client reads the output path out of it, at `BuiltPath::Built::toJSON`."""
    wanted = await _wanted_id()
    engine = FakeEngine(realisation_id=wanted)

    result = await _run(engine)

    assert list(_outputs(result.result)) == [wanted]
    assert str(_outputs(result.result)[wanted].out_path) == FLOAT_OUT


@pytest.mark.anyio
async def test_the_id_is_the_one_the_original_derivation_makes() -> None:
    """`staticOutputHashes` of the original, and not of the resolved derivation."""
    wanted = await _wanted_id()
    engine = FakeEngine(realisation_id=wanted)

    await _run(engine)

    assert engine.local_store.asked == [wanted]


@pytest.mark.anyio
async def test_no_realisation_means_a_build() -> None:
    engine = FakeEngine(realisation_id=None)

    await _run(engine)

    assert len(engine.build_goals) == 1


@pytest.mark.anyio
async def test_a_realisation_whose_path_is_gone_means_a_build() -> None:
    """The garbage collector removes a path and leaves the realisation."""
    engine = FakeEngine(realisation_id=await _wanted_id(), valid=False)

    await _run(engine)

    assert len(engine.build_goals) == 1
