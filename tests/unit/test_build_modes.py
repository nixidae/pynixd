"""A check and a repair go to the local store, and no goal runs.

`nix build --rebuild` sends `BuildMode.CHECK`, and `--repair` sends
`BuildMode.REPAIR`. `nix-store --realise --check`, `--repair-path` and
`--verify --repair` send the same two. The goal system raised `RuntimeError`
for each one, so every such command failed through pynixd and succeeded
through `nix-daemon`. `tests/parity/` recorded the difference.

Neither mode is a build that pynixd can schedule. Read
`GoalEngine._straight_to_the_store` for the reason.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.exceptions import BackendError
from pynixd.goals.engine import GoalEngine
from pynixd.serde import (
    BuildMode,
    BuildPathsRequest,
    BuildPathsResponse,
    BuildPathsWithResultsRequest,
    BuildPathsWithResultsResponse,
    DerivedPath as SerdeDerivedPath,
    IsValidPathRequest,
    IsValidPathResponse,
)

if TYPE_CHECKING:
    from pynixd.context import PynixdContext

DRV = "/nix/store/11111111111111111111111111111111-example.drv"
NOT_NORMAL = [BuildMode.CHECK, BuildMode.REPAIR]


class RecordingStore:
    """A local store that records each request and answers a fixed value."""

    def __init__(self, answer: Any) -> None:
        self.answer = answer
        self.calls: list[Any] = []
        self.executed: list[Any] = []
        self.derivations_read: list[str] = []

    async def call(self, request: Any, **kwargs: Any) -> Any:
        del kwargs
        self.calls.append(request)
        return self.answer

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        del kwargs
        self.executed.append(request)
        # A missing `.drv` is first substituted (nixidae/nixidae#1005), and
        # that asks whether the path is valid. Nothing else may reach here.
        if isinstance(request, IsValidPathRequest):
            return IsValidPathResponse(valid=False)
        raise AssertionError("the goal system ran, and a check must not reach it")

    async def read_derivation(self, drv_store_path: Any) -> None:
        # No derivation. The goal then fails, and the name it asked for is
        # the proof that the goal system ran at all.
        self.derivations_read.append(str(drv_store_path))
        return None


def _engine(store: RecordingStore) -> GoalEngine:
    ctx = cast("PynixdContext", SimpleNamespace(local_store=store, scheduler=None, stores={}))
    return GoalEngine(ctx)


def _derived_paths() -> list[SerdeDerivedPath]:
    one: Any = SerdeDerivedPath(value=f"{DRV}!out")
    return cast("list[SerdeDerivedPath]", [one])


@pytest.mark.anyio
@pytest.mark.parametrize("mode", NOT_NORMAL)
async def test_build_paths_in_another_mode_reaches_the_store(mode: BuildMode) -> None:
    store = RecordingStore(BuildPathsResponse(value=1))
    request = BuildPathsRequest(derived_paths=_derived_paths(), build_mode=mode)

    answer = await _engine(store).build_paths(request)

    assert answer.value == 1
    assert store.calls == [request]
    assert store.executed == []


@pytest.mark.anyio
@pytest.mark.parametrize("mode", NOT_NORMAL)
async def test_build_paths_with_results_in_another_mode_reaches_the_store(mode: BuildMode) -> None:
    store = RecordingStore(BuildPathsWithResultsResponse(results=[]))
    request = BuildPathsWithResultsRequest(derived_paths=_derived_paths(), build_mode=mode)

    answer = await _engine(store).build_paths_with_results(request)

    assert answer.results == []
    assert store.calls == [request]


@pytest.mark.anyio
async def test_a_normal_build_still_runs_the_goal_system() -> None:
    """The rule above must not take the ordinary build with it."""
    store = RecordingStore(BuildPathsResponse(value=1))
    request = BuildPathsRequest(derived_paths=_derived_paths(), build_mode=BuildMode.NORMAL)

    # The fake store holds no derivation, so the goal fails. The name that
    # it asked for is the proof that the goal system ran at all.
    with pytest.raises(BackendError, match="failed to obtain derivation of"):
        await _engine(store).build_paths(request)

    assert store.derivations_read == [DRV]
    assert store.calls == []
