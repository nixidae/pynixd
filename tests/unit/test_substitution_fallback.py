"""A substitute that cannot be had builds the derivation, as Nix does.

`derivation-goal.cc:124`: when no substituter can provide an output, the
derivation builds; a failed copy builds with `--fallback` and fails with
"try '--fallback'" without. pynixd failed the derivation on any failed
substitute. With HTTP caches now checked against `trusted-public-keys`
(nixidae/nixidae#1008), an unsigned substitute would have failed every such
build. nixidae/nixidae#1003.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.derived_path import DerivedPath
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import goal_failure
from pynixd.goals.substitute import UNSIGNED_REFUSAL
from pynixd.serde import BuildMode, BuildResultStatus, IsValidPathRequest, IsValidPathResponse
from pynixd.store_path import StorePath

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine
    from pynixd.goals.results import GoalResult

_DRV = "/nix/store/00000000000000000000000000000000-probe.drv"
_PATH = StorePath("/nix/store/11111111111111111111111111111111-probe")


class InvalidStore:
    async def execute(self, request: Any, **_kwargs: Any) -> Any:
        if isinstance(request, IsValidPathRequest):
            return IsValidPathResponse(valid=False)
        raise AssertionError(f"unexpected request {type(request).__name__}")


def _goal(*, fallback: bool) -> EnsureDerivedPathGoal:
    engine = SimpleNamespace(ctx=SimpleNamespace(local_store=InvalidStore()))
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{_DRV}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    goal._watchers.append(cast("Any", SimpleNamespace(options=SimpleNamespace(try_fallback=int(fallback)))))  # noqa: SLF001 -- the client of the goal

    async def failed_copy(_path: StorePath) -> GoalResult | None:
        return goal_failure("pynixd: failed to substitute: 0 bytes read", BuildResultStatus.TRANSIENT_FAILURE)

    goal._try_substitute_path = failed_copy  # type: ignore[method-assign, assignment]  # noqa: SLF001 -- the substitute is the input
    return goal


@pytest.mark.anyio
async def test_a_failed_copy_builds_with_fallback() -> None:
    assert await _goal(fallback=True)._try_substitute_known_outputs({"out": _PATH}) is None  # noqa: SLF001


@pytest.mark.anyio
async def test_a_failed_copy_without_fallback_names_it() -> None:
    result = await _goal(fallback=False)._try_substitute_known_outputs({"out": _PATH})  # noqa: SLF001

    assert result is not None
    assert result.result.status == BuildResultStatus.TRANSIENT_FAILURE
    assert "try '--fallback'" in str(result.result.error_msg)


def test_the_unsigned_refusal_matches_nix_words() -> None:
    """`LocalStore::addToStore` at `local-store.cc:1049`."""
    message = f"cannot add path '{_PATH}' because it lacks a signature by a trusted key"
    assert UNSIGNED_REFUSAL in message
