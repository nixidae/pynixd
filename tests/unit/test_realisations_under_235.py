"""Content-addressed outputs that Nix 2.35 already realised are not built again.

Nix 2.35 records a realisation under the resolved derivation
(`derivation-goal.cc:240`). pynixd asked for it only when the client named
a substituter, so a derivation the stock daemon had built went to a builder
again: measured on dynhetz, `nix-daemon` answered at once and pynixd built on
sg1-n2. A realisation whose path was collected is substituted before a build,
as `derivation-goal.cc:113-122` does. nixidae/nixidae#1004.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.goals import ensure
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import goal_success
from pynixd.serde import BuildMode, DrvOutput, IsValidPathResponse, Realisation, StorePath

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine

ORIGINAL = StorePath("/nix/store/00000000000000000000000000000001-ca-top.drv")
RESOLVED = StorePath("/nix/store/00000000000000000000000000000002-ca-top.drv")
OUT = "/nix/store/00000000000000000000000000000003-ca-top"


class Store:
    def __init__(self, *, valid: bool) -> None:
        self.valid = valid

    async def read_derivation(self, _path: str) -> Any:
        return SimpleNamespace(is_impure=False, output_paths=lambda: {"out": StorePath("")})

    async def execute(self, _request: Any, **_kwargs: Any) -> Any:
        return IsValidPathResponse(valid=self.valid)


def _goal(store: Store) -> EnsureDerivedPathGoal:
    engine = SimpleNamespace(ctx=SimpleNamespace(local_store=store))
    return EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{ORIGINAL}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )


@pytest.fixture
def asked(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    paths: list[str] = []

    async def realisations_of(_parsed: Any, _wanted: Any, _store: Any, drv_path: StorePath) -> Any:
        paths.append(str(drv_path))
        key = f"sha256:{'0' * 64}!out"
        return {key: Realisation(id=DrvOutput(key), out_path=StorePath(OUT.removeprefix("/nix/store/")))}

    monkeypatch.setattr(ensure, "realisations_of", realisations_of)
    return paths


@pytest.mark.anyio
async def test_a_local_realisation_of_the_resolved_derivation_ends_the_goal(asked: list[str]) -> None:
    """No client here names a substituter; the local store answers anyway."""
    answer = await _goal(Store(valid=True))._try_realise_the_resolved_one(RESOLVED, ORIGINAL, {"out"})  # noqa: SLF001

    assert answer is not None
    assert asked == [str(RESOLVED)]


@pytest.mark.anyio
async def test_a_collected_realised_path_is_substituted_first(asked: list[str]) -> None:
    goal = _goal(Store(valid=False))
    substituted: list[str] = []

    async def substitute(path: StorePath) -> Any:
        substituted.append(str(path))
        return goal_success()

    goal._try_substitute_path = substitute  # type: ignore[method-assign]  # noqa: SLF001 -- the cache has it
    parsed = await Store(valid=False).read_derivation("")

    answer = await goal._already_realised(parsed, {"out"})  # noqa: SLF001

    assert answer is not None
    assert substituted
