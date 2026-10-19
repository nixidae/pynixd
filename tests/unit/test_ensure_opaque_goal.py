"""An opaque path a backend holds is a path pynixd can produce.

A fleet build leaves its outputs on the backend that ran it. `Scheduler.
_collect_outputs` records where each one went in `ctx.output_locations`, and
`DaemonProxy` and `NarFromPathHandler` both read a path from the backend that
holds it. `EnsureDerivedPathGoal` asked the local store alone, so the build
succeeded and the very next request for its outputs failed with `opaque path
is not valid locally` -- issue nixidae/nixidae#199.

`nix copy` asks this way. It realises its installables against the source
store before it copies anything, and a store path installable is an opaque
derived path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import StoreSpecBase
from pynixd.context import PynixdContext
from pynixd.derived_path import DerivedPath
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import result_succeeded
from pynixd.serde import BuildMode, IsValidPathResponse
from pynixd.store.daemon import DaemonStore
from pynixd.store_path import StorePath

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine
    from pynixd.store.base import Store

_OUTPUT = "/nix/store/00000000000000000000000000000000-diff-single"


class FakeLocalStore:
    """A local store that holds nothing, which is the state under test."""

    def __init__(self) -> None:
        self.store_id = StoreId("local")
        self.queried: list[str] = []

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        del kwargs
        self.queried.append(str(request.path))
        return IsValidPathResponse(valid=False)


class FakeEngine:
    """Enough of `GoalEngine` for the opaque branch.

    `get_substitute_path_goal` raises. A backend-resident path must be
    answered from the map, and a substitution attempt for a path pynixd
    already has would be wasted work as well as a wrong answer.
    """

    def __init__(self, ctx: PynixdContext) -> None:
        self.ctx = ctx
        self.substitutions_attempted = 0

    async def get_substitute_path_goal(self, path: StorePath, substituter_ids: tuple[str, ...]) -> Any:
        del path, substituter_ids
        self.substitutions_attempted += 1
        raise AssertionError("the goal must answer from `output_locations` before it asks a substituter")


class OfflineBackend(DaemonStore):
    """A real `DaemonStore` that never connects.

    A real one and not a fake, because `store_for_output_path` type-checks
    what it returns: only a store that speaks the wire can serve a mapped
    read. `create_conn` is the one abstract method, and nothing here calls it.
    """

    async def create_conn(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise AssertionError("this test opens no connection")


def _backend(store_id: str) -> DaemonStore:
    return OfflineBackend(StoreSpecBase(store_id=StoreId(store_id)))


def _context(stores: dict[StoreId, Store], locations: dict[str, StoreId]) -> PynixdContext:
    return PynixdContext(
        settings=cast("Any", None),
        _stores=stores,
        output_locations=locations,
    )


def _goal(engine: FakeEngine) -> EnsureDerivedPathGoal:
    return EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(_OUTPUT),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )


@pytest.mark.anyio
async def test_an_opaque_path_a_backend_holds_is_ensured() -> None:
    """The defect of issue nixidae/nixidae#199, stated at the goal that reported it."""
    local = FakeLocalStore()
    builder = _backend("builder")
    ctx = _context(
        {StoreId("local"): cast("Store", local), StoreId("builder"): builder},
        {_OUTPUT: StoreId("builder")},
    )
    engine = FakeEngine(ctx)

    result = await _goal(engine).result()

    assert result_succeeded(result.result), result.result.error_msg
    assert result.resolved_outputs == {"out": StorePath(_OUTPUT)}
    assert result.produced_paths == {StorePath(_OUTPUT)}
    assert engine.substitutions_attempted == 0
    assert local.queried == [_OUTPUT], "the local store is still asked first"


@pytest.mark.anyio
async def test_an_opaque_path_no_store_holds_is_still_a_failure() -> None:
    """The guard answers for a recorded path, and for no other one.

    Without this, the correction above reads as "opaque paths now always
    succeed", which would hide every real missing path.
    """
    local = FakeLocalStore()
    ctx = _context({StoreId("local"): cast("Store", local)}, {})
    engine = FakeEngine(ctx)
    engine.substitutions_attempted = 0

    # No backend holds it, so the goal reaches the substituter it would have
    # reached before. `FakeEngine` raises there, which is the proof it got
    # that far.
    with pytest.raises(AssertionError, match="before it asks a substituter"):
        await _goal(engine).result()


@pytest.mark.anyio
async def test_a_backend_that_left_the_fleet_is_not_a_location() -> None:
    """`output_locations` outlives a store, so the lookup checks the store is there."""
    ctx = _context({}, {_OUTPUT: StoreId("builder")})
    assert ctx.store_for_output_path(_OUTPUT) is None


def test_the_lookup_ignores_a_store_that_does_not_speak_the_wire() -> None:
    """Only a `DaemonStore` can serve a mapped read, so only one counts."""
    ctx = _context(
        {StoreId("cache"): cast("Store", object())},
        {_OUTPUT: StoreId("cache")},
    )
    assert ctx.store_for_output_path(_OUTPUT) is None


def test_an_unrecorded_path_has_no_location() -> None:
    ctx = _context({StoreId("builder"): _backend("builder")}, {})
    assert ctx.store_for_output_path(_OUTPUT) is None
