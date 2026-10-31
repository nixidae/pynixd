"""A content-addressed input, and the derivation that goes on the wire.

A floating content-addressed output names no path until it is built, so a
derivation that uses one carries a `DownstreamPlaceholder` in place of that
path. `Derivation::tryResolve` of Nix replaces each placeholder with the path
the input really produced, and puts that path in `inputSrcs`.

pynixd sent such a derivation through `to_basic_derivation`, which rewrites no
placeholder and drops an input that names no path. The builder then read
`/<placeholder>/dep` and found nothing, and `ca:why-depends` and `ca:nix-shell`
of the functional suite are two of the tests that saw it.

`Derivation::shouldResolve` at `derivations.cc:1129` states which derivations
must resolve. A floating output is one of them.

The last test states the second half of the same correction: the resolution
read the paths of the inputs from the child goals alone, and a child goal runs
only for an input that needs a build. An input that was valid already
therefore had no path, and the derivation went unresolved.

Issue nixidae/nixidae#222.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.drv_parser import ChildMapNode, Derivation
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.resolution import (
    downstream_placeholder,
    downstream_placeholder_unknown_derivation,
    resolve_dynamic_derivation,
)
from pynixd.goals.results import GoalResult, goal_success
from pynixd.goals.substitute import SubstituteAttempt
from pynixd.serde import BuildMode, IsValidPathResponse, QueryRealisationRequest, QueryRealisationResponse
from pynixd.store_layout import StoreLayout
from pynixd.store_path import DrvOutput, StorePath
from pynixd.trust import TrustPolicy
from tests.goal_group import result_of

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine
    from pynixd.serde import BuildDerivationRequest

ROOT_DRV = "/nix/store/00000000000000000000000000000001-rootCA.drv"
DEP_DRV = "/nix/store/00000000000000000000000000000002-dependent.drv"
LEGACY_DRV = "/nix/store/00000000000000000000000000000003-legacy.drv"
FIXED_DRV = "/nix/store/00000000000000000000000000000004-fixed.drv"
ROOT_OUT = "/nix/store/44444444444444444444444444444444-rootCA"
LEGACY_OUT = "/nix/store/55555555555555555555555555555555-legacy"
FIXED_OUT = "/nix/store/66666666666666666666666666666666-fixed"

PLACEHOLDER = downstream_placeholder(StorePath(ROOT_DRV), "out")
BUILD_COMMAND = f"mkdir -p $out\ncat {PLACEHOLDER}/dep {LEGACY_OUT}/hello > $out/dep\n"
FIXED_COMMAND = f"cat {PLACEHOLDER}/dep > $out\n"


def _floating(name: str) -> DrvOutput:
    """A floating content-addressed output: a method, no digest and no path."""
    return DrvOutput(hash_algo="r:sha256", hash_value="", output_name=name, path="")


def _root() -> Derivation:
    return Derivation(
        outputs=[_floating("out")],
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", "mkdir -p $out; echo hi > $out/dep"],
        env={"out": "", "name": "rootCA"},
    )


def _legacy() -> Derivation:
    """An input-addressed input, whose output is in the store already."""
    return Derivation(
        outputs=[DrvOutput(hash_algo="", hash_value="", output_name="out", path=LEGACY_OUT)],
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", "mkdir -p $out; echo hi > $out/hello"],
        env={"out": LEGACY_OUT, "name": "legacy"},
    )


def _dependent() -> Derivation:
    return Derivation(
        outputs=[_floating("out")],
        input_drvs={
            StorePath(ROOT_DRV): ["out"],
            StorePath(LEGACY_DRV): ["out"],
        },
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", BUILD_COMMAND],
        env={"out": "", "name": "dependent", "buildCommand": BUILD_COMMAND},
    )


def _fixed() -> Derivation:
    """A fixed-output derivation that reads a content-addressed input.

    Its output names a path, because the hash of the content states that path.
    `ca/content-addressed.nix` calls this one `dependentFixedOutput`.
    """
    return Derivation(
        outputs=[DrvOutput(hash_algo="r:sha256", hash_value="00" * 32, output_name="out", path=FIXED_OUT)],
        input_drvs={StorePath(ROOT_DRV): ["out"]},
        platform="x86_64-linux",
        builder="/bin/sh",
        args=["-e", "-c", FIXED_COMMAND],
        env={"out": FIXED_OUT, "name": "fixed", "buildCommand": FIXED_COMMAND},
    )


class FakeLocalStore:
    def __init__(self) -> None:
        self.layout = StoreLayout.chroot(None)

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        if drv_path == ROOT_DRV:
            return _root()
        if drv_path == LEGACY_DRV:
            return _legacy()
        if drv_path == DEP_DRV:
            return _dependent()
        if drv_path == FIXED_DRV:
            return _fixed()
        return None

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        """The output of the input-addressed input is in the store already."""
        del kwargs
        if isinstance(request, QueryRealisationRequest):
            # No output is realised, so every derivation here is built.
            return QueryRealisationResponse(realisations=[])
        return IsValidPathResponse(valid=str(request.path) == LEGACY_OUT)


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
    """The input derivation, already built, and the path it produced."""

    may_reach_a_root_goal = True
    """The goal of an input can reach a root goal of the request. Issue nixidae/nixidae#246."""

    def note_a_parent(self) -> None:
        """The goal of an input has a goal that waits for it."""

    async def subscribe_many(self, clients: list[Any]) -> None:
        del clients

    async def result(self) -> GoalResult:
        result = goal_success()
        result.resolved_outputs = {"out": StorePath(ROOT_OUT)}
        return result.with_dynamic_outputs(StorePath(ROOT_DRV))


class FakeSubstituteGoal:
    may_reach_a_root_goal = False
    """A substitute goal reaches no root goal. Issue nixidae/nixidae#246."""

    async def result(self) -> SubstituteAttempt:
        return SubstituteAttempt(found=False, result=goal_success())


class FakeContext:
    def __init__(self) -> None:
        self.local_store = FakeLocalStore()
        # The daemons these tests stand for resolve a fixed-output derivation.
        self.trust = TrustPolicy(experimental_features=("ca-derivations",))


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


async def _sent(drv_path: str = DEP_DRV) -> Any:
    engine = FakeEngine()
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{drv_path}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    await result_of(goal)
    assert len(engine.build_goals) == 1
    return engine.build_goals[0].request.derivation


def test_the_placeholder_is_the_one_nix_computes() -> None:
    """52 characters after the separator, as `DownstreamPlaceholder::render`."""
    assert PLACEHOLDER.startswith("/")
    assert len(PLACEHOLDER) == 53


@pytest.mark.anyio
async def test_the_builder_reads_a_path_and_not_a_placeholder() -> None:
    sent = await _sent()

    assert PLACEHOLDER not in sent.env["buildCommand"]
    assert ROOT_OUT in sent.env["buildCommand"]
    assert all(PLACEHOLDER not in arg for arg in sent.args)


@pytest.mark.anyio
async def test_the_input_is_in_the_sources_of_the_derivation() -> None:
    """The daemon scans the output for the paths in `inputSrcs` alone.

    An input that is not there gets no reference, so the output claims to
    depend on nothing and the garbage collector removes what it needs.
    """
    sent = await _sent()

    assert ROOT_OUT in {str(path) for path in sent.input_srcs}


@pytest.mark.anyio
async def test_an_input_that_needed_no_build_is_a_source_as_well() -> None:
    """`LEGACY_OUT` is in the store, so no child goal runs for it.

    The resolution read the path of each input from the child goals alone, so
    this input had none, and it fell out of `inputSrcs`. The derivation of the
    input names the path, and `_input_paths` reads it there.
    """
    sent = await _sent()

    assert LEGACY_OUT in {str(path) for path in sent.input_srcs}


@pytest.mark.anyio
async def test_a_fixed_output_derivation_is_resolved_as_well() -> None:
    """Its own output names a path, and its input still does not.

    The kind of the output says nothing about the inputs, so a rule that reads
    the outputs alone leaves this one unresolved. `ca:build`, `ca:build-cache`,
    `ca:nix-copy` and `ca:signatures` all build one.
    """
    sent = await _sent(FIXED_DRV)

    assert PLACEHOLDER not in sent.env["buildCommand"]
    assert ROOT_OUT in sent.env["buildCommand"]
    assert ROOT_OUT in {str(path) for path in sent.input_srcs}
    assert sent.outputs["out"].path == FIXED_OUT, "a fixed output keeps the path its hash states"


@pytest.mark.anyio
async def test_the_output_stays_floating() -> None:
    """The daemon computes the path of a content-addressed output.

    A deferred output is the other case: pynixd fills that one in, because the
    daemon cannot without the input derivations.
    """
    sent = await _sent()

    assert sent.outputs["out"].path == ""
    assert sent.outputs["out"].method == "r:sha256"


@pytest.mark.anyio
async def test_dynamic_paths_survive_regular_input_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Issue #9: a deferred consumer keeps its producer output and source edge."""
    producer = StorePath(ROOT_DRV)
    parent_hash = hashlib.sha256(b"nix-upstream-output:00000000000000000000000000000001:rootCA").digest()
    placeholder = downstream_placeholder_unknown_derivation(parent_hash, "out")
    consumer = _dependent()
    consumer.input_drvs = {StorePath(LEGACY_DRV): ["out"]}
    consumer.dynamic_input_drvs = {producer: ChildMapNode(children={"out": ChildMapNode(outputs=["out"])})}
    consumer.args = [placeholder]
    consumer.env["application"] = placeholder
    read_derivation = FakeLocalStore.read_derivation

    async def read(store: FakeLocalStore, path: str) -> Derivation | None:
        return consumer if path == DEP_DRV else await read_derivation(store, path)

    async def child_result(child: FakeChildGoal) -> GoalResult:
        del child
        result = goal_success()
        result.resolved_outputs = {"out": StorePath(ROOT_OUT)}
        result.dynamic_paths = {(producer, "out", "out"): StorePath(ROOT_OUT)}
        return result

    monkeypatch.setattr(FakeLocalStore, "read_derivation", read)
    monkeypatch.setattr(FakeChildGoal, "result", child_result)
    sent = await _sent()

    assert sent.env["application"] == ROOT_OUT
    assert sent.args == [ROOT_OUT]
    assert {StorePath(ROOT_OUT), StorePath(LEGACY_OUT)} <= sent.input_srcs


@pytest.mark.anyio
@pytest.mark.parametrize("chain", [("out",), ("out", "next", "out")])
async def test_nested_goals_record_the_complete_output_chain(chain: tuple[str, ...]) -> None:
    producer = StorePath(ROOT_DRV)
    inner = StorePath(LEGACY_DRV)
    requested: list[DerivedPath] = []

    class Child(FakeChildGoal):
        def __init__(self, path: StorePath) -> None:
            self.path = path

        async def result(self) -> GoalResult:
            result = goal_success()
            result.resolved_outputs = {chain[-1] if self.path == inner else "out": self.path}
            return result

    class Engine(FakeEngine):
        async def get_ensure_derived_path_goal(self, dp: Any, build_mode: int, substituter_ids: Any) -> FakeChildGoal:
            del build_mode, substituter_ids
            requested.append(dp)
            return Child(inner if len(requested) == 1 else StorePath(ROOT_OUT))

    dp = DerivedPath("!".join((ROOT_DRV, *chain, "out")))
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", Engine()),
        derived_path=dp,
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    result = await result_of(goal)

    assert requested == [dp.outer, dp.wrap(inner)]
    key: tuple[StorePath | str, ...] = (producer, *chain, "out")  # type: ignore[assignment]  # zuban-bug: a tuple of StorePath and str is joined to object even with a declared type
    assert result.dynamic_paths[key] == StorePath(ROOT_OUT)
    assert (producer, "out") not in result.dynamic_paths
    assert inner in result.produced_paths


@pytest.mark.anyio
async def test_a_chain_past_a_plain_output_fails_as_nix_fails() -> None:
    """`deepWrapper` asks for `!out` past `target`, which is not a derivation.

    Measured with the stock nix-daemon 2.35.3: "error parsing derivation
    '...-target': store path '...-target' is not a valid derivation path".
    nixidae/nixidae#1005.
    """
    requested: list[DerivedPath] = []

    class Child(FakeChildGoal):
        async def result(self) -> GoalResult:
            result = goal_success()
            result.resolved_outputs = {"out": StorePath(ROOT_OUT)}
            return result

    class Engine(FakeEngine):
        async def get_ensure_derived_path_goal(self, dp: Any, build_mode: int, substituter_ids: Any) -> FakeChildGoal:
            del build_mode, substituter_ids
            requested.append(dp)
            return Child()

    dp = DerivedPath(f"{ROOT_DRV}!out!out")
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", Engine()),
        derived_path=dp,
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    result = await result_of(goal)

    assert requested == [dp.outer]
    assert "is not a valid derivation path" in str(result.result.error_msg)
    assert not result.dynamic_paths


def test_dynamic_branches_with_the_same_output_name_resolve_separately() -> None:
    producer = StorePath(ROOT_DRV)
    root_hash = "00000000000000000000000000000001"
    hashes = {
        name: hashlib.sha256(f"nix-upstream-output:{root_hash}:rootCA-{name}".encode()).digest() for name in ("a", "b")
    }
    placeholders = {name: downstream_placeholder_unknown_derivation(parent, "out") for name, parent in hashes.items()}
    consumer = _dependent()
    consumer.input_drvs = {}
    consumer.dynamic_input_drvs = {
        producer: ChildMapNode(
            outputs=["out"],
            children={name: ChildMapNode(outputs=["out"]) for name in hashes},
        )
    }
    consumer.env = {**placeholders, "producer": downstream_placeholder(producer, "out")}
    paths: dict[tuple[StorePath | str, ...], StorePath] = {
        (producer, "out"): StorePath(ROOT_OUT),
        (producer, "a", "out"): StorePath(LEGACY_OUT),
        (producer, "b", "out"): StorePath(FIXED_OUT),
    }
    sent = resolve_dynamic_derivation(consumer, StorePath(DEP_DRV), paths)

    assert sent.env == {"a": LEGACY_OUT, "b": FIXED_OUT, "producer": ROOT_OUT}
    assert set(paths.values()) <= sent.input_srcs


def test_a_missing_dynamic_output_refuses_resolution() -> None:
    consumer = _dependent()
    consumer.input_drvs = {}
    consumer.dynamic_input_drvs = {StorePath(ROOT_DRV): ChildMapNode(children={"out": ChildMapNode(outputs=["out"])})}
    with pytest.raises(ValueError, match=r"No resolved path for .*!out!out"):
        resolve_dynamic_derivation(consumer, StorePath(DEP_DRV), {})
