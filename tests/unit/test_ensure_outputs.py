"""The derivation on the wire keeps every output, and the answer keeps one.

`BuildDerivation` carries no set of wanted outputs. The daemon builds each
output that the derivation names, and it rewrites `builtins.placeholder <name>`
for each of those names, at `derivation-builder.cc:802` of Nix.

`EnsureDerivedPathGoal` removed each output that the derived path does not
name, so `drv^out` sent a derivation of one output. The builder then read
`${placeholder "bin"}` as a path, and no file is there. `main:placeholders` of
the functional suite is that build. Issue nixidae/nixidae#217.

The removal also split one build in two: the derivation is the dedup key of a
`BuildDerivationGoal`, so `drv^out` and `drv^bin` made two derivations and two
builds of the same work.

The answer is the other half. Nix removes each unwanted output from
`builtOutputs`, at `derivation-goal.cc:291`, so a client that asks for one
output reads one output.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import PurePath
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.drv_parser import Derivation
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import GoalResult, goal_success
from pynixd.goals.substitute import SubstituteAttempt
from pynixd.serde import BuildMode, DrvOutput as SerdeDrvOutput, IsValidPathResponse, Realisation
from pynixd.store_layout import StoreLayout
from pynixd.store_path import DrvOutput, StorePath
from pynixd.utils import nix32_encode
from tests.goal_group import result_of

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine
    from pynixd.serde import BuildDerivationRequest

_DRV = "/nix/store/00000000000000000000000000000000-placeholders.drv"
_OUT_PATH = {
    "out": "/nix/store/11111111111111111111111111111111-placeholders",
    "bin": "/nix/store/22222222222222222222222222222222-placeholders-bin",
    "dev": "/nix/store/33333333333333333333333333333333-placeholders-dev",
}

# `nix eval --raw --expr 'builtins.placeholder "<name>"'`, Nix 2.34.8. The
# helper below must answer the same, and `04f3da1k...` is the path that the
# builder of `main:placeholders` could not read.
_REAL_PLACEHOLDER = {
    "out": "/1rz4g4znpzjwh1xymhjpm42vipw92pr73vdgl6xs1hycac8kf2n9",
    "bin": "/04f3da1kmbr67m3gzxikmsl4vjz5zf777sv6m14ahv22r65aac9m",
    "dev": "/02qcpld1y6xhs5gz9bchpxaw0xdhmsp5dv88lh25r2ss44kh8dxz",
}


def hash_placeholder(output_name: str) -> str:
    """`hashPlaceholder` of `derivations.cc:1071` of Nix."""
    return "/" + nix32_encode(hashlib.sha256(f"nix-output:{output_name}".encode()).digest())


def _build_command() -> str:
    """What `placeholders.sh` of the functional suite asks the builder to run."""
    lines = [f"echo {name} > ${name}" for name in _OUT_PATH]
    lines += [f"[[ $(cat {hash_placeholder(name)}) = {name} ]]" for name in _OUT_PATH]
    return "\n".join(lines)


def _derivation() -> Derivation:
    return Derivation(
        outputs=[
            DrvOutput(hash_algo="", hash_value="", output_name=name, path=path) for name, path in _OUT_PATH.items()
        ],
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", _build_command()],
        env={"buildCommand": _build_command(), **_OUT_PATH},
    )


class FakeLocalStore:
    """A store that holds the derivation, and the outputs it is told to hold."""

    def __init__(self, valid: frozenset[str] = frozenset()) -> None:
        self.layout = StoreLayout.chroot(None)
        self.valid = valid

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        return _derivation() if drv_path == _DRV else None

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        del kwargs
        return IsValidPathResponse(valid=str(request.path) in self.valid)


class FakeBuildGoal:
    """A build goal that records its request and answers for every output."""

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
        result = goal_success()
        result.result = result.result.model_copy(
            update={
                "built_outputs": {
                    f"sha256:abcd!{name}": Realisation(
                        id=SerdeDrvOutput(f"sha256:abcd!{name}"),
                        out_path=StorePath(path=path),
                    )
                    for name, path in _OUT_PATH.items()
                }
            }
        )
        result.resolved_outputs = {name: StorePath(path) for name, path in _OUT_PATH.items()}
        return result


class FakeSubstituteGoal:
    """A substituter that holds nothing."""

    may_reach_a_root_goal = False
    """A substitute goal reaches no root goal. Issue nixidae/nixidae#246."""

    async def result(self) -> SubstituteAttempt:
        return SubstituteAttempt(found=False, result=goal_success())


class FakeContext:
    def __init__(self, valid: frozenset[str] = frozenset()) -> None:
        self.local_store = FakeLocalStore(valid=valid)


class FakeEngine:
    """Enough of `GoalEngine` for a derivation with no input derivation."""

    def __init__(self, valid: frozenset[str] = frozenset()) -> None:
        self.ctx = FakeContext(valid)
        self.build_goals: list[FakeBuildGoal] = []

    async def get_build_derivation_goal(self, request: BuildDerivationRequest) -> FakeBuildGoal:
        goal = FakeBuildGoal(request)
        self.build_goals.append(goal)
        return goal

    async def get_substitute_path_goal(self, path: StorePath, substituter_ids: tuple[str, ...]) -> Any:
        """No substituter holds an output, so the goal has to build."""
        del path, substituter_ids
        return FakeSubstituteGoal()


def _goal(engine: FakeEngine, derived_path: str) -> EnsureDerivedPathGoal:
    return EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(derived_path),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )


def test_the_helper_answers_what_nix_answers() -> None:
    """Pin the formula against the real values, so the tests below mean something."""
    assert {name: hash_placeholder(name) for name in _REAL_PLACEHOLDER} == _REAL_PLACEHOLDER


@pytest.mark.anyio
async def test_one_wanted_output_still_sends_every_output() -> None:
    """The defect of issue nixidae/nixidae#217, stated where the derivation goes on the wire."""
    engine = FakeEngine()

    await result_of(_goal(engine, f"{_DRV}!out"))

    assert len(engine.build_goals) == 1
    sent = engine.build_goals[0].request.derivation
    assert set(sent.outputs) == set(_OUT_PATH)


@pytest.mark.anyio
async def test_the_builder_can_rewrite_every_placeholder_it_meets() -> None:
    """Each placeholder in the build command names an output of the derivation.

    The daemon builds `inputRewrites` from the outputs of the derivation it
    receives. A placeholder whose output is not there stays in the command, and
    the builder reads it as a path.
    """
    engine = FakeEngine()

    await result_of(_goal(engine, f"{_DRV}!out"))

    sent = engine.build_goals[0].request.derivation
    rewritable = {hash_placeholder(name) for name in sent.outputs}
    unrewritten = [
        placeholder
        for placeholder in (hash_placeholder(name) for name in _OUT_PATH)
        if placeholder not in rewritable and placeholder in sent.env["buildCommand"]
    ]
    assert unrewritten == []


@pytest.mark.anyio
async def test_the_answer_holds_the_wanted_output_alone() -> None:
    """`derivation-goal.cc:291` of Nix removes the others, so this does too."""
    engine = FakeEngine()

    result = await result_of(_goal(engine, f"{_DRV}!out"))

    built = result.result.built_outputs or {}
    assert {item.id.output_name for item in built.values()} == {"out"}


@pytest.mark.anyio
async def test_a_request_for_every_output_keeps_every_answer() -> None:
    engine = FakeEngine()

    result = await result_of(_goal(engine, f"{_DRV}!*"))

    built = result.result.built_outputs or {}
    assert {item.id.output_name for item in built.values()} == set(_OUT_PATH)


@pytest.mark.anyio
async def test_two_wanted_outputs_make_one_derivation() -> None:
    """The derivation is the dedup key, so it must not follow the wanted set."""
    engine = FakeEngine()

    await result_of(_goal(engine, f"{_DRV}!out"))
    await result_of(_goal(engine, f"{_DRV}!bin"))

    first, second = (goal.request.derivation for goal in engine.build_goals)
    assert first == second


@pytest.mark.anyio
async def test_an_already_valid_output_still_names_its_path() -> None:
    """The defect of issue nixidae/nixidae#218.

    pynixd answered "this succeeded" and said nothing about what it produced.
    `nix build --json` then wrote no `outputs` key at all, because
    `BuiltPath::Built::toJSON` of Nix writes that key once for each output.
    """
    engine = FakeEngine(valid=frozenset(_OUT_PATH.values()))

    result = await result_of(_goal(engine, f"{_DRV}!out"))

    assert engine.build_goals == [], "an already-valid output needs no build"
    built = result.result.built_outputs or {}
    assert {item.id.output_name for item in built.values()} == {"out"}
    # **Assert the JSON, because that is what a `Realisation` ships.**
    # `adl_serializer<nix::StorePath>::to_json`, `src/libstore/path.cc:95` of
    # Nix, writes `to_string()`, which is the base name. `str()` is
    # `printStorePath` and gives the whole path; the two are different on
    # purpose (#4), so reading `str()` here tested the wrong codec.
    carried = {json.loads(item.to_json())["outPath"] for item in built.values()}
    assert carried == {PurePath(_OUT_PATH["out"]).name}


@pytest.mark.anyio
async def test_the_realisation_names_the_derivation_that_made_it() -> None:
    """`DrvOutput::to_string` is `sha256:<hex>!<name>`, and the hex is real.

    `tests/unit/test_drv_hash.py` states that the hash is the one Nix computes.
    This states that the answer carries it in the shape a client can parse.
    """
    engine = FakeEngine(valid=frozenset(_OUT_PATH.values()))

    result = await result_of(_goal(engine, f"{_DRV}!out"))

    built = result.result.built_outputs or {}
    [key] = built
    algo, _, rest = key.partition(":")
    digest, _, name = rest.partition("!")
    assert algo == "sha256"
    assert name == "out"
    assert len(digest) == 64
    assert int(digest, 16) >= 0, "the hash is hexadecimal"
