"""Build outcomes that a client reads, as Nix 2.35.3 gives them. nixidae/nixidae#1012."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.ids import RequestId
from pynixd.build_queue import BuildQueue
from pynixd.derived_path import DerivedPath
from pynixd.goals.engine import _build_failure_message  # noqa: PLC2701 -- the text is the unit
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    IsValidPathResponse,
    SetOptionsRequest,
    StorePath,
    TimeSpan,
    Verbosity,
)

if TYPE_CHECKING:
    from pynixd.goals.engine import GoalEngine


def _options(*, keep_going: bool) -> SetOptionsRequest:
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=keep_going,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=1,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=1,
        use_substitutes=True,
        overrides={},
    )


def _request(number: int) -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/{number:032d}-x.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"),
        build_mode=BuildMode.NORMAL,
    )


@pytest.mark.anyio
async def test_a_standalone_waiter_keeps_a_shared_build():
    """An op-36 client lost the build when the goal client let go.

    In Nix two daemons never kill each other's build
    (`derivation-building-goal.cc:421`).
    """
    queue = BuildQueue()
    build_id, future = await queue.enqueue(
        _request(1), from_goal_path=True, goal_request_id=RequestId(1), options=_options(keep_going=False)
    )
    await queue.enqueue(_request(1))

    await queue.release_request(RequestId(1))

    assert not future.done()
    assert not queue.nobody_wants(queue.by_id[build_id])


@pytest.mark.anyio
async def test_keep_going_is_read_per_request():
    """`keepGoing` belongs to one worker (`worker.cc:173`); the first asker decided for all."""
    queue = BuildQueue()
    failed, _ = await queue.enqueue(
        _request(1), from_goal_path=True, goal_request_id=RequestId(1), options=_options(keep_going=False)
    )
    await queue.enqueue(
        _request(1), from_goal_path=True, goal_request_id=RequestId(2), options=_options(keep_going=True)
    )
    other, _ = await queue.enqueue(
        _request(2), from_goal_path=True, goal_request_id=RequestId(2), options=_options(keep_going=True)
    )

    await queue.fail(failed, "it failed")

    assert not queue.nobody_wants(queue.by_id[other])


def test_several_failures_read_as_nix_names_them():
    """`entry-points.cc:36-43`: one failure is its own message, several are named."""
    one = SimpleNamespace(
        path="/nix/store/00000000000000000000000000000001-a.drv!out", result=BuildResult(status=1, error_msg="boom")
    )
    two = SimpleNamespace(
        path="/nix/store/00000000000000000000000000000002-b", result=BuildResult(status=1, error_msg="x")
    )

    assert _build_failure_message([one]) == "boom"
    assert _build_failure_message([one, two]) == (
        "build of '/nix/store/00000000000000000000000000000001-a.drv', "
        "'/nix/store/00000000000000000000000000000002-b' failed"
    )


class InvalidStore:
    async def execute(self, _request: Any, **_kwargs: Any) -> Any:
        return IsValidPathResponse(valid=False)


@pytest.mark.anyio
async def test_an_opaque_path_no_substituter_has_is_no_substituters():
    """`substitution-goal.cc:159`: NoSubstituters, with Nix's words."""
    path = "/nix/store/00000000000000000000000000000003-gone"
    engine = SimpleNamespace(ctx=SimpleNamespace(local_store=InvalidStore(), store_for_output_path=lambda _p: None))
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(path),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )

    async def nothing(_path: Any) -> None:
        return None

    goal._try_substitute_path = nothing  # type: ignore[method-assign]  # noqa: SLF001 -- no substituter

    result = await goal._ensure_opaque()  # noqa: SLF001

    assert result.result.status == BuildResultStatus.NO_SUBSTITUTERS
    assert str(result.result.error_msg) == f"path '{path}' is required, but there is no substituter that can build it"
