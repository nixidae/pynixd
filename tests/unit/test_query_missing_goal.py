"""Unit tests for read-only QueryMissing planning goals."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from pynixd.drv_parser import ChildMapNode, Derivation, DrvOutput
from pynixd.goals.engine import GoalEngine
from pynixd.goals.query_missing import QueryMissingPlanGoal
from pynixd.serde import (
    DerivedPath as SerdeDerivedPath,
    DrvOutput as SerdeDrvOutput,
    IsValidPathResponse,
    LogNext,
    QueryMissingRequest,
    QueryRealisationRequest,
    QueryRealisationResponse,
    Realisation,
    StorePath,
)
from pynixd.substitution_queue import SubstitutionAvailability

if TYPE_CHECKING:
    from pynixd.context import PynixdContext
    from pynixd.store_path import StorePath


class FakeLocalStore:
    def __init__(
        self,
        valid_paths: set[str],
        derivations: dict[str, Derivation] | None = None,
        realisations: dict[str, str] | None = None,
    ) -> None:
        self.valid_paths = valid_paths
        self.derivations = derivations or {}
        # The path of each realised output, by output name. The digest in the
        # `DrvOutput` id comes from the derivation, so a test that states it
        # would state the hash of Nix, and the name is enough to find it.
        self.realisations = realisations or {}
        self.realisation_queries: list[str] = []

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        del kwargs
        if isinstance(request, QueryRealisationRequest):
            key = str(request.drv_output)
            self.realisation_queries.append(key)
            path = self.realisations.get(key.rpartition("!")[2])
            if path is None:
                return QueryRealisationResponse(realisations=[])
            return QueryRealisationResponse(
                realisations=[Realisation(id=SerdeDrvOutput(key), out_path=StorePath(path=path))],
            )
        return IsValidPathResponse(valid=str(request.path) in self.valid_paths)

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        return self.derivations.get(str(drv_store_path))


class FakeSubstitutionQueue:
    def __init__(self, substitutable: dict[str, SubstitutionAvailability]) -> None:
        self.substitutable = substitutable
        self.queries: list[str] = []

    async def can_substitute(self, path: StorePath) -> SubstitutionAvailability:
        self.queries.append(str(path))
        return self.substitutable.get(str(path), SubstitutionAvailability.unavailable())


class BlockingSubstitutionQueue:
    def __init__(self, blocked_path: str, releasing_path: str) -> None:
        self.blocked_path = blocked_path
        self.releasing_path = releasing_path
        self.releasing_path_queried = anyio.Event()
        self.queries: list[str] = []

    async def can_substitute(self, path: StorePath) -> SubstitutionAvailability:
        path_str = str(path)
        self.queries.append(path_str)
        if path_str == self.releasing_path:
            self.releasing_path_queried.set()
        if path_str == self.blocked_path:
            await self.releasing_path_queried.wait()
        return SubstitutionAvailability.unavailable()


def _derived_path_set(path: str) -> set[SerdeDerivedPath]:
    derived_path: Any = SerdeDerivedPath(value=path)
    return cast("set[SerdeDerivedPath]", {derived_path})


def _derived_paths(paths: set[str]) -> set[SerdeDerivedPath]:
    derived_paths: set[Any] = set()
    for path in paths:
        derived_path: Any = SerdeDerivedPath(value=path)
        derived_paths.add(derived_path)
    return cast("set[SerdeDerivedPath]", derived_paths)


def _derivation(output_path: str, *, is_dynamic: bool = False) -> Derivation:
    return Derivation(
        outputs=[DrvOutput(output_name="out", path=output_path, hash_algo="", hash_value="")],
        is_dynamic=is_dynamic,
    )


@pytest.mark.anyio
async def test_query_missing_reports_substitutable_opaque_path() -> None:
    path = "/nix/store/00000000000000000000000000000000-example"
    substitution_queue = FakeSubstitutionQueue(
        {
            path: SubstitutionAvailability(
                available=True,
                nar_size=123,
                download_size=123,
            )
        }
    )
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=FakeLocalStore(valid_paths=set()),
            scheduler=SimpleNamespace(substitution_queue=substitution_queue),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(path))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_substitute} == {path}
    assert not response.unknown
    assert response.nar_size == 123
    assert response.download_size == 123
    assert substitution_queue.queries == [path]


@pytest.mark.anyio
async def test_query_missing_reports_unknown_when_no_substituter_has_path() -> None:
    path = "/nix/store/00000000000000000000000000000000-example"
    substitution_queue = FakeSubstitutionQueue({})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=FakeLocalStore(valid_paths=set()),
            scheduler=SimpleNamespace(substitution_queue=substitution_queue),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(path))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.unknown} == {path}
    assert not response.will_substitute
    assert response.nar_size == 0
    assert response.download_size == 0


@pytest.mark.anyio
async def test_query_missing_skips_valid_derivation_output() -> None:
    drv_path = "/nix/store/11111111111111111111111111111111-example.drv"
    out_path = "/nix/store/22222222222222222222222222222222-example"
    substitution_queue = FakeSubstitutionQueue({})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=FakeLocalStore(
                valid_paths={out_path},
                derivations={drv_path: _derivation(out_path)},
            ),
            scheduler=SimpleNamespace(substitution_queue=substitution_queue),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert not response.will_build
    assert not response.will_substitute
    assert not response.unknown
    assert not substitution_queue.queries


@pytest.mark.anyio
async def test_query_missing_reports_substitutable_derivation_output() -> None:
    drv_path = "/nix/store/11111111111111111111111111111111-example.drv"
    out_path = "/nix/store/22222222222222222222222222222222-example"
    substitution_queue = FakeSubstitutionQueue(
        {
            out_path: SubstitutionAvailability(available=True, nar_size=42, download_size=42),
        }
    )
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=FakeLocalStore(
                valid_paths=set(),
                derivations={drv_path: _derivation(out_path)},
            ),
            scheduler=SimpleNamespace(substitution_queue=substitution_queue),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_substitute} == {out_path}
    assert not response.will_build
    assert not response.unknown
    assert response.nar_size == 42


@pytest.mark.anyio
async def test_query_missing_reports_will_build_for_missing_derivation_output() -> None:
    drv_path = "/nix/store/11111111111111111111111111111111-example.drv"
    out_path = "/nix/store/22222222222222222222222222222222-example"
    substitution_queue = FakeSubstitutionQueue({})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=FakeLocalStore(
                valid_paths=set(),
                derivations={drv_path: _derivation(out_path)},
            ),
            scheduler=SimpleNamespace(substitution_queue=substitution_queue),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_build} == {drv_path}
    assert not response.will_substitute
    assert not response.unknown


@pytest.mark.anyio
async def test_query_missing_reports_will_build_for_dynamic_derivation() -> None:
    drv_path = "/nix/store/11111111111111111111111111111111-example.drv"
    out_path = "/nix/store/22222222222222222222222222222222-example"
    substitution_queue = FakeSubstitutionQueue({})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=FakeLocalStore(
                valid_paths=set(),
                derivations={drv_path: _derivation(out_path, is_dynamic=True)},
            ),
            scheduler=SimpleNamespace(substitution_queue=substitution_queue),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_build} == {drv_path}
    assert not substitution_queue.queries


def _content_addressed(output_name: str = "out") -> Derivation:
    """A derivation whose output takes its path from the build.

    The path is empty, which is what a floating content-addressed output, a
    deferred output and an impure output all look like in the `.drv` file.
    """
    return Derivation(outputs=[DrvOutput(output_name=output_name, path="", hash_algo="", hash_value="")])


def _content_addressed_outputs(*output_names: str) -> Derivation:
    """A content-addressed derivation that has more than one output."""
    return Derivation(
        outputs=[DrvOutput(output_name=name, path="", hash_algo="", hash_value="") for name in output_names]
    )


@pytest.mark.anyio
async def test_a_realised_content_addressed_output_is_in_no_bucket() -> None:
    """`nix-daemon` answers an empty `willBuild` for a derivation it built.

    `Store::queryMissing` reads `queryPartialDerivationOutputMap`, and that
    map answers from the realisation when the derivation names no path.
    pynixd read the derivation alone, so a second `nix build` of a
    content-addressed derivation asked for a build. `tests/parity/` recorded
    the difference. Issue nixidae/nixidae#214.
    """
    drv_path = "/nix/store/11111111111111111111111111111111-example.drv"
    out_path = "/nix/store/22222222222222222222222222222222-example"
    store = FakeLocalStore(
        valid_paths={out_path},
        derivations={drv_path: _content_addressed()},
        realisations={"out": out_path},
    )
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(local_store=store, scheduler=SimpleNamespace(substitution_queue=FakeSubstitutionQueue({}))),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert not response.will_build
    assert not response.will_substitute
    assert not response.unknown
    assert len(store.realisation_queries) == 1
    assert store.realisation_queries[0].endswith("!out")


@pytest.mark.anyio
async def test_a_content_addressed_output_with_no_realisation_gets_built() -> None:
    """That is `knownOutputPaths = false` at `misc.cc:219` of Nix."""
    drv_path = "/nix/store/11111111111111111111111111111111-example.drv"
    store = FakeLocalStore(valid_paths=set(), derivations={drv_path: _content_addressed()})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(local_store=store, scheduler=SimpleNamespace(substitution_queue=FakeSubstitutionQueue({}))),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_build} == {drv_path}


@pytest.mark.anyio
async def test_a_realisation_that_names_a_deleted_path_gets_built() -> None:
    """A realisation stays after a garbage collection removes the path.

    So the path that the realisation names is checked as well, exactly as
    `misc.cc:222` checks it.
    """
    drv_path = "/nix/store/11111111111111111111111111111111-example.drv"
    out_path = "/nix/store/22222222222222222222222222222222-example"
    store = FakeLocalStore(
        valid_paths=set(),
        derivations={drv_path: _content_addressed()},
        realisations={"out": out_path},
    )
    substitution_queue = FakeSubstitutionQueue({})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(local_store=store, scheduler=SimpleNamespace(substitution_queue=substitution_queue)),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_build} == {drv_path}
    assert substitution_queue.queries == [out_path]


@pytest.mark.anyio
@pytest.mark.parametrize("wanted", ["not-an-output", ""])
async def test_a_request_that_names_no_output_plans_nothing(wanted: str) -> None:
    """**This test stands for `build.sh:98` and `build.sh:102`.**

    Both lines assert that `nix build` fails: one sends
    `multiple-outputs-a.drv^not-an-output`, and the other sends `^` with an
    empty output list. The build fails either way, so the suite passed while
    the plan was wrong, and `streams build` is what found it.

    `Store::queryMissing` reads `bfd.outputs` only to decide whether an output
    path is invalid, at `misc.cc:222`. A name that the derivation does not
    carry matches nothing, so `invalid` stays empty and the walk returns at
    `misc.cc:225` with an empty answer.

    pynixd read the empty selection as "no output path is known" and answered
    `willBuild`. The client then asked for the path of that derivation and
    tried to build it, so it sent four operations where it sends three to
    `nix-daemon`. Issue #26.
    """
    drv_path = "/nix/store/11111111111111111111111111111111-multiple-outputs-a.drv"
    first = "/nix/store/22222222222222222222222222222222-a-first"
    second = "/nix/store/33333333333333333333333333333333-a-second"
    store = FakeLocalStore(
        valid_paths={first, second},
        derivations={
            drv_path: Derivation(
                outputs=[
                    DrvOutput(output_name="first", path=first, hash_algo="", hash_value=""),
                    DrvOutput(output_name="second", path=second, hash_algo="", hash_value=""),
                ]
            )
        },
    )
    substitution_queue = FakeSubstitutionQueue({})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(local_store=store, scheduler=SimpleNamespace(substitution_queue=substitution_queue)),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!{wanted}"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert not response.will_build
    assert not response.will_substitute
    assert not response.unknown
    assert not substitution_queue.queries


@pytest.mark.anyio
async def test_a_sibling_output_with_no_realisation_does_not_force_a_build() -> None:
    """pynixd reads the wanted outputs, and Nix reads every output.

    **This test stands for a difference of the wire, and not for a line of the
    suite.** `ca:build` passes on both sides. `streams build` records the
    difference: `QueryMissing` of `nix-daemon` names `rootCA.drv` in
    `will_build` seven times, and pynixd names it in none of them. Issue #26.

    `rootCA` of `ca/content-addressed.nix` has the outputs `out`, `dev` and
    `foo`, and `dev` and `foo` are symbolic links to `$out`. A client that
    builds `rootCA^out` realises `out` alone, because `DerivationGoal` of Nix
    holds one `wantedOutput` and maps that one output back to the original
    derivation, at `derivation-goal.cc:228-236`.

    `queryMissing` of Nix then decides `knownOutputPaths` from every output,
    at `misc.cc:217-225`, so an unrealised `dev` makes it announce a build of
    a derivation whose `out` is already there. Measured against 2.34.8 with no
    daemon: `nix build rootCA^out --dry-run` names the derivation, and it
    names nothing after `rootCA^*` registers `dev` and `foo`.
    `Lillecarl/nix#312` reports that, and `NIX-DEVIATION` does not apply
    because Nix is wrong here.

    pynixd reads the wanted outputs through `selected_output_paths`, so it
    asks for the realisation of `out` alone and puts the derivation in no
    bucket. `docs/notes/querymissing.md` holds the measurement.
    """
    drv_path = "/nix/store/11111111111111111111111111111111-rootCA.drv"
    out_path = "/nix/store/22222222222222222222222222222222-rootCA"
    store = FakeLocalStore(
        valid_paths={out_path},
        derivations={drv_path: _content_addressed_outputs("out", "dev", "foo")},
        realisations={"out": out_path},
    )
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(local_store=store, scheduler=SimpleNamespace(substitution_queue=FakeSubstitutionQueue({}))),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert not response.will_build
    assert not response.will_substitute
    assert not response.unknown
    # The whole difference is here: `dev` and `foo` are never asked about.
    assert [query.rpartition("!")[2] for query in store.realisation_queries] == ["out"]


@pytest.mark.anyio
async def test_an_input_addressed_output_asks_for_no_realisation() -> None:
    """The derivation names the path, so the store answers nothing new."""
    drv_path = "/nix/store/11111111111111111111111111111111-example.drv"
    out_path = "/nix/store/22222222222222222222222222222222-example"
    store = FakeLocalStore(valid_paths={out_path}, derivations={drv_path: _derivation(out_path)})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(local_store=store, scheduler=SimpleNamespace(substitution_queue=FakeSubstitutionQueue({}))),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{drv_path}!out"))

    await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert store.realisation_queries == []


@pytest.mark.anyio
async def test_query_missing_classifies_roots_in_parallel() -> None:
    blocked_path = "/nix/store/00000000000000000000000000000000-blocked"
    releasing_path = "/nix/store/11111111111111111111111111111111-releasing"
    substitution_queue = BlockingSubstitutionQueue(blocked_path, releasing_path)
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=FakeLocalStore(valid_paths=set()),
            scheduler=SimpleNamespace(substitution_queue=substitution_queue),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_paths({blocked_path, releasing_path}))

    with anyio.fail_after(1):
        response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.unknown} == {blocked_path, releasing_path}
    assert set(substitution_queue.queries) == {blocked_path, releasing_path}


@pytest.mark.anyio
async def test_query_missing_walks_the_input_derivations() -> None:
    """`mustBuildDrv` at `misc.cc:139` enqueues each input of what it builds.

    `nix build` prints "these N derivations will be built" from this answer.
    pynixd classified the derived paths of the request alone, so the list held
    the top derivation and none of the inputs under it.

    The chain is `top` -> `middle` -> `bottom`, and no output is valid, so
    every one of the three must build.
    """
    top = "/nix/store/11111111111111111111111111111111-top.drv"
    middle = "/nix/store/22222222222222222222222222222222-middle.drv"
    bottom = "/nix/store/33333333333333333333333333333333-bottom.drv"

    def _with_input(output: str, input_drv: str | None) -> Derivation:
        return Derivation(
            outputs=[DrvOutput(output_name="out", path=output, hash_algo="", hash_value="")],
            input_drvs={input_drv: ["out"]} if input_drv else {},  # pyright: ignore[reportArgumentType] -- a plain str
        )

    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=FakeLocalStore(
                valid_paths=set(),
                derivations={
                    top: _with_input("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-top", middle),
                    middle: _with_input("/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-middle", bottom),
                    bottom: _with_input("/nix/store/cccccccccccccccccccccccccccccccc-bottom", None),
                },
            ),
            scheduler=SimpleNamespace(substitution_queue=FakeSubstitutionQueue({})),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{top}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_build} == {top, middle, bottom}
    assert not response.unknown


@pytest.mark.anyio
async def test_query_missing_reads_a_shared_input_once() -> None:
    """`doPath` keeps a `done` set of derived paths, at `misc.cc:188`.

    Two derivations that share one input give the same derived path twice, and
    the walk must read the derivation of that input once.
    """
    left = "/nix/store/11111111111111111111111111111111-left.drv"
    right = "/nix/store/22222222222222222222222222222222-right.drv"
    shared = "/nix/store/33333333333333333333333333333333-shared.drv"

    def _with_input(output: str, input_drv: str | None) -> Derivation:
        return Derivation(
            outputs=[DrvOutput(output_name="out", path=output, hash_algo="", hash_value="")],
            input_drvs={input_drv: ["out"]} if input_drv else {},  # pyright: ignore[reportArgumentType] -- a plain str
        )

    store = FakeLocalStore(
        valid_paths=set(),
        derivations={
            left: _with_input("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-left", shared),
            right: _with_input("/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-right", shared),
            shared: _with_input("/nix/store/cccccccccccccccccccccccccccccccc-shared", None),
        },
    )
    reads: list[str] = []
    original = store.read_derivation

    async def _counting(drv_store_path: StorePath | str) -> Derivation | None:
        reads.append(str(drv_store_path))
        return await original(drv_store_path)

    store.read_derivation = _counting  # pyright: ignore[reportAttributeAccessIssue] -- a fake, for the count alone

    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=store,
            scheduler=SimpleNamespace(substitution_queue=FakeSubstitutionQueue({})),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_paths({f"{left}!out", f"{right}!out"}))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_build} == {left, right, shared}
    assert reads.count(shared) == 1


@pytest.mark.anyio
async def test_a_dynamic_input_gets_a_warning_and_no_bucket() -> None:
    """`doPath` at `misc.cc:196` warns for one and puts it nowhere.

    `mustBuildDrv` enqueues each input of a derivation that must be built, and
    `enqueueDerivedPaths` walks the tree of a dynamic one. Each level that
    names an output becomes a derived path whose derivation path is itself a
    derived path, and `doPath` refuses that shape.
    """
    top = "/nix/store/11111111111111111111111111111111-top.drv"
    producer = "/nix/store/22222222222222222222222222222222-producer.drv.drv"
    parsed = Derivation(
        outputs=[DrvOutput(output_name="out", path="", hash_algo="", hash_value="")],
        dynamic_input_drvs={producer: ChildMapNode(children={"out": ChildMapNode(outputs=["out"])})},  # pyright: ignore[reportArgumentType] -- a plain str
    )
    store = FakeLocalStore(valid_paths=set(), derivations={top: parsed})
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(
            local_store=store,
            scheduler=SimpleNamespace(substitution_queue=FakeSubstitutionQueue({})),
        ),
    )
    request = QueryMissingRequest(derived_paths=_derived_path_set(f"{top}!out"))

    response = await QueryMissingPlanGoal(GoalEngine(ctx), request).result()

    assert {str(path) for path in response.will_build} == {top}
    assert not response.unknown
    # `LogNext` is the only message with a `text`, and the length below
    # keeps the assertion exact: a message of another kind would not fall
    # out of the list quietly.
    assert len(response.logs.messages) == 1
    assert [message.text for message in response.logs.messages if isinstance(message, LogNext)] == [
        f"warning: Ignoring dynamic derivation {producer}^out while querying missing paths; not yet implemented\n",
    ]
