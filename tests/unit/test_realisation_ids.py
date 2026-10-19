"""The id of a realisation, after pynixd resolves the derivation it sends.

pynixd resolves a derivation before it sends it, so the daemon reads a
different ATerm and answers with a different `DrvOutput`. The client holds the
original derivation and asks for the original id. These tests state the
difference, and then state that the goal corrects it.

`EnsureDerivedPathGoal` makes the correction, and `BuildDerivationGoal` does
not. That goal holds the original derivation, and the build goal holds the
resolved one alone since issue nixidae/nixidae#223.

`test_drv_hash.py` holds the same two derivations, and it states why they are
the oracle for the hash itself. Issue nixidae/nixidae#221.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, cast

import pytest
from test_drv_hash import BASE_PATH, BASE_TEXT, TOP_TEXT

from pynixd.derived_path import DerivedPath
from pynixd.drv_hash import output_hashes
from pynixd.drv_parser import parse_drv
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import GoalResult, goal_success
from pynixd.goals.substitute import SubstituteAttempt
from pynixd.serde import (
    BuildMode,
    BuildResult,
    BuildResultStatus,
    DrvOutput as SerdeDrvOutput,
    IsValidPathResponse,
    QueryRealisationRequest,
    QueryRealisationResponse,
    Realisation,
    RegisterDrvOutputRequest,
    Signature,
)
from pynixd.store_layout import StoreLayout
from pynixd.store_path import StorePath

if TYPE_CHECKING:
    from pynixd.drv_parser import Derivation
    from pynixd.goals.engine import GoalEngine
    from pynixd.serde import BuildDerivationRequest

TOP_PATH = "/nix/store/00000000000000000000000000000002-drv-hash-top.drv"
BASE_OUT = "/nix/store/d69pwdv3lma9vw5hgd09hlcnv484j307-drv-hash-base"
TOP_OUT = "/nix/store/331zkap2q342l1x5g9sd6i5cxqbwmw4a-drv-hash-top"

# The same derivation, with one floating content-addressed output. Only such
# an output has a realisation, so only this one is registered.
FLOAT_PATH = "/nix/store/00000000000000000000000000000003-drv-hash-float.drv"
FLOAT_TEXT = (
    'Derive([("out","","r:sha256","")],'
    f'[("{BASE_PATH}",["out"])],[],'
    '"x86_64-linux","/bin/sh",["-c","echo hi > $out"],'
    '[("builder","/bin/sh"),("name","drv-hash-float"),("out",""),("system","x86_64-linux")])'
)
FLOAT_OUT = "/nix/store/77777777777777777777777777777777-drv-hash-float"

# A deferred output: three empty strings. It is input-addressed, and its path
# is not known until the inputs are built. pynixd fills the path in before it
# sends the derivation, so the derivation on the wire names one and the
# original does not. `ca:build` builds `dependentNonCA`, which is this shape.
DEFER_PATH = "/nix/store/00000000000000000000000000000004-drv-hash-defer.drv"
DEFER_TEXT = (
    'Derive([("out","","","")],'
    f'[("{BASE_PATH}",["out"])],[],'
    '"x86_64-linux","/bin/sh",["-c","echo hi > $out"],'
    '[("builder","/bin/sh"),("name","drv-hash-defer"),("out",""),("system","x86_64-linux")])'
)
DEFER_OUT = "/nix/store/88888888888888888888888888888888-drv-hash-defer"

# An impure output: `("out","","r:sha256","impure")`, which is what Nix writes
# for one. Every build of it makes a new output, so one realisation id cannot
# hold two, and Nix registers none.
IMPURE_PATH = "/nix/store/00000000000000000000000000000005-drv-hash-impure.drv"
IMPURE_TEXT = (
    'Derive([("out","","r:sha256","impure")],'
    f'[("{BASE_PATH}",["out"])],[],'
    '"x86_64-linux","/bin/sh",["-c","echo hi > $out"],'
    '[("builder","/bin/sh"),("name","drv-hash-impure"),("out",""),("system","x86_64-linux")])'
)
IMPURE_OUT = "/nix/store/66666666666666666666666666666666-drv-hash-impure"


async def _read_drv(drv_path: str) -> Derivation | None:
    if drv_path == BASE_PATH:
        return parse_drv(BASE_TEXT)
    if drv_path == TOP_PATH:
        return parse_drv(TOP_TEXT)
    if drv_path == FLOAT_PATH:
        return parse_drv(FLOAT_TEXT)
    if drv_path == DEFER_PATH:
        return parse_drv(DEFER_TEXT)
    if drv_path == IMPURE_PATH:
        return parse_drv(IMPURE_TEXT)
    return None


def _flattened_hash() -> str:
    """The hash the daemon computes for the derivation that pynixd sends.

    The resolution moves the output of each input derivation into `inputSrcs`
    and leaves `inputDrvs` empty. This makes the same derivation.
    """
    resolved = dataclasses.replace(
        parse_drv(TOP_TEXT),
        input_drvs={},
        input_srcs={BASE_OUT},  # pyright: ignore[reportArgumentType] -- StorePath is a str
    )
    return resolved.hash_derivation_modulo(mask_outputs=True)["out"]


class FakeLocalStore:
    """A store that holds the output of the input derivation, and no other."""

    def __init__(self) -> None:
        self.layout = StoreLayout.chroot(None)
        self.valid: set[str] = {BASE_OUT}
        self.registered: list[Realisation] = []
        self.added: list[tuple[str, str, set[str]]] = []

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        return await _read_drv(drv_path)

    async def add_text_to_store(self, name: str, text: str, references: Any) -> str:
        self.added.append((name, text, set(references)))
        return f"/nix/store/{'9' * 32}-{name}"

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        del kwargs
        if isinstance(request, RegisterDrvOutputRequest):
            if request.realisation is None:
                raise AssertionError("RegisterDrvOutput carries a realisation in the 2.34 shape")
            self.registered.append(request.realisation)
            return IsValidPathResponse(valid=True)
        if isinstance(request, QueryRealisationRequest):
            # No output is realised yet, so every derivation here is built.
            return QueryRealisationResponse(realisations=[])
        return IsValidPathResponse(valid=str(request.path) in self.valid)


class FakeBuildGoal:
    """The build that the daemon made, and the path it left in the store."""

    may_reach_a_root_goal = False
    """A build goal reaches no root goal, so a caller keeps its place. Issue nixidae/nixidae#246."""

    def __init__(self, store: FakeLocalStore, request: BuildDerivationRequest, response: BuildResult) -> None:
        self.store = store
        self.request = request
        self.response = response

    async def subscribe(self, client: Any) -> None:
        del client

    async def start(self) -> None:
        """`Goal.start` begins the build and does not wait. Issue nixidae/nixidae#246."""

    async def wait_until_it_reached_the_queue(self) -> None:
        """This fake needs no queue, so the build is on it at once."""

    async def result(self) -> GoalResult:
        for realisation in _outputs(self.response).values():
            if realisation.out_path is not None:
                self.store.valid.add(str(realisation.out_path))
        return GoalResult(result=self.response)


class FakeSubstituteGoal:
    may_reach_a_root_goal = False
    """A substitute goal reaches no root goal. Issue nixidae/nixidae#246."""

    async def result(self) -> SubstituteAttempt:
        return SubstituteAttempt(found=False, result=goal_success())


class FakeEngine:
    def __init__(self, response: BuildResult) -> None:
        self.local_store = FakeLocalStore()
        self.ctx = cast("Any", _Ctx(self.local_store))
        self.response = response
        self.build_goals: list[FakeBuildGoal] = []

    async def get_build_derivation_goal(self, request: BuildDerivationRequest) -> FakeBuildGoal:
        goal = FakeBuildGoal(self.local_store, request, self.response)
        self.build_goals.append(goal)
        return goal

    async def get_substitute_path_goal(self, path: StorePath, substituter_ids: tuple[str, ...]) -> Any:
        del path, substituter_ids
        return FakeSubstituteGoal()


class _Ctx:
    def __init__(self, local_store: FakeLocalStore) -> None:
        self.local_store = local_store


def _outputs(result: BuildResult) -> dict[str, Realisation]:
    """`built_outputs` is optional on the wire, and every response here sets it."""
    if result.built_outputs is None:
        raise AssertionError("the response carries built outputs")
    return result.built_outputs


def _response(key: str, out_path: str) -> BuildResult:
    return BuildResult(
        status=BuildResultStatus.BUILT,
        built_outputs={key: Realisation(id=SerdeDrvOutput(key), out_path=StorePath(path=out_path))},
    )


async def _run(response: BuildResult, drv_path: str = TOP_PATH) -> tuple[FakeEngine, GoalResult]:
    engine = FakeEngine(response)
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{drv_path}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    result = await goal.result()
    return engine, result


@pytest.mark.anyio
async def test_the_flattened_derivation_hashes_to_something_else() -> None:
    """The premise. Without this difference the correction has no subject."""
    original = await output_hashes(parse_drv(TOP_TEXT), _read_drv, cache={})

    assert original is not None
    assert original["out"] != _flattened_hash()


@pytest.mark.anyio
async def test_the_answer_carries_the_id_of_the_original_derivation() -> None:
    original = await output_hashes(parse_drv(TOP_TEXT), _read_drv, cache={})
    if original is None:
        raise AssertionError("the walk read both derivations, so it has an answer")
    sent = f"sha256:{_flattened_hash()}!out"
    wanted = f"sha256:{original['out']}!out"

    _, result = await _run(_response(sent, TOP_OUT))

    assert list(_outputs(result.result)) == [wanted]
    assert str(_outputs(result.result)[wanted].out_path) == TOP_OUT
    assert _outputs(result.result)[wanted].id.output_name == "out"


@pytest.mark.anyio
async def test_pynixd_registers_the_id_that_the_client_asks_for() -> None:
    """`queryPartialDerivationOutputMap` of the client uses the original hash.

    `store-api.cc:406` reads `staticOutputHashes` of the derivation in the
    store of the client, so a realisation under any other id answers nothing.
    """
    original = await output_hashes(parse_drv(FLOAT_TEXT), _read_drv, cache={})
    if original is None:
        raise AssertionError("the walk read both derivations, so it has an answer")
    wanted = f"sha256:{original['out']}!out"

    engine, _ = await _run(_response(f"sha256:{'11' * 32}!out", FLOAT_OUT), FLOAT_PATH)

    assert [str(item.id) for item in engine.local_store.registered] == [wanted]


@pytest.mark.anyio
async def test_a_signature_does_not_survive_the_new_id() -> None:
    """A signature covers the id, so it means nothing under a different one.

    `derivation-goal.cc:234` of Nix clears them and signs again.
    """
    response = _response(f"sha256:{_flattened_hash()}!out", TOP_OUT)
    for realisation in _outputs(response).values():
        realisation.signatures = [Signature("key1:notarealsignature")]

    _, result = await _run(response)

    assert [item.signatures for item in _outputs(result.result).values()] == [[]]


@pytest.mark.anyio
async def test_an_id_that_already_agrees_stays_as_it_is() -> None:
    original = await output_hashes(parse_drv(FLOAT_TEXT), _read_drv, cache={})
    if original is None:
        raise AssertionError("the walk read both derivations, so it has an answer")
    wanted = f"sha256:{original['out']}!out"

    engine, result = await _run(_response(wanted, FLOAT_OUT), FLOAT_PATH)

    assert list(_outputs(result.result)) == [wanted]
    assert [str(item.id) for item in engine.local_store.registered] == [wanted]


@pytest.mark.anyio
async def test_a_deferred_output_registers_a_realisation() -> None:
    """The original derivation names no path, so the client reads a realisation.

    pynixd fills the path in before it sends the derivation, so the derivation
    on the wire names one. A rule that reads the wire derivation therefore
    registers nothing, and `nix-build.cc:730` of the client asserts.
    """
    original = await output_hashes(parse_drv(DEFER_TEXT), _read_drv, cache={})
    if original is None:
        raise AssertionError("the walk read both derivations, so it has an answer")
    wanted = f"sha256:{original['out']}!out"

    engine, _ = await _run(_response(f"sha256:{'22' * 32}!out", DEFER_OUT), DEFER_PATH)

    assert [str(item.id) for item in engine.local_store.registered] == [wanted]


@pytest.mark.anyio
async def test_an_input_addressed_output_registers_the_corrected_id() -> None:
    """Issue #40, and it used to assert the opposite.

    The rule here was `Derivation.needs_realisations`, which answers False for
    every output that names a path -- and an input-addressed output names its
    own. The corrected id therefore never reached the store.

    **Nix registers a realisation for an input-addressed output too.** Its own
    docstring says so: "Nix registers a realisation for every output of every
    derivation while `ca-derivations` is on", at `derivation-builder.cc:1994`
    and `derivation-goal.cc:236`. The control store of
    `ca/import-from-derivation` holds one for `add-path`, whose output names a
    path.

    That is what the old assertion missed, and the daemon answering a
    realisation at all is what disproves its reason: a daemon with
    `ca-derivations` off answers none, which the test below states.

    Measured: `nix-daemon` left `sha256:749c1858...!out` and pynixd left
    `sha256:58306574...!out`, both naming the same output path. The client
    read the corrected id either way, so no comparison of the wire could see
    it; the store comparison of issue #39 found it.
    """
    original = await output_hashes(parse_drv(TOP_TEXT), _read_drv, cache={})
    if original is None:
        raise AssertionError("the walk read both derivations, so it has an answer")
    wanted = f"sha256:{original['out']}!out"

    engine, _ = await _run(_response(f"sha256:{_flattened_hash()}!out", TOP_OUT))

    assert [str(item.id) for item in engine.local_store.registered] == [wanted]


@pytest.mark.anyio
async def test_an_impure_derivation_registers_no_realisation() -> None:
    """Every build of an impure derivation makes a new output.

    One realisation id cannot hold two, so Nix guards the registration with
    `if (!drv->type().isImpure())` at `derivation-goal.cc:226`. The daemon
    answers "Trying to register a realisation of '...', but we already have
    another one locally", the pool discards the connection as dirty, and the
    temporary roots it held stay in the file.

    `needs_realisations` is False here, and it was the whole guard until issue
    #40 added `changed` beside it. `changed` does not cover this case: the
    daemon answers a realisation for an impure derivation like any other, so
    the guard has to name impurity itself.

    `main:impure-derivations` of the functional suite is where the store
    comparison saw pynixd register two of them.
    """
    parsed = parse_drv(IMPURE_TEXT)
    assert parsed.is_impure is True
    assert parsed.needs_realisations is False

    engine, _ = await _run(_response(f"sha256:{'33' * 32}!out", IMPURE_OUT), IMPURE_PATH)

    assert engine.local_store.registered == []


@pytest.mark.anyio
async def test_a_daemon_that_answers_no_realisation_registers_nothing() -> None:
    """The guard that `needs_realisations` was written to be, stated directly.

    A daemon with `ca-derivations` off answers no realisation, and pynixd must
    not send `RegisterDrvOutput`: the daemon refuses it, and the pool then
    discards a good connection as dirty, leaving the temporary roots it held
    in the file.

    This is the condition the rule should always have read. The old one asked
    the derivation whether the store needed realisations; this asks whether
    the daemon gave one to correct.
    """
    engine, _ = await _run(
        BuildResult(status=BuildResultStatus.BUILT, built_outputs={}),
    )

    assert engine.local_store.registered == []
