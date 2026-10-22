"""Unit tests for scheduler build queue behavior."""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import BuildId, RequestId
from pynixd.build_queue import BuildQueue
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    SetOptionsRequest,
    TimeSpan,
    Verbosity,
)
from pynixd.store_path import StorePath


def _build_request(
    drv_path: StorePath, *, builder: str = "", mode: BuildMode = BuildMode.NORMAL
) -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=str(drv_path)),
        derivation=BasicDerivation(platform="x86_64-linux", builder=builder),
        build_mode=mode,
    )


@pytest.mark.anyio
async def test_build_queue_deduplicates_active_builds_by_drv_path() -> None:
    queue = BuildQueue()
    drv_path = StorePath("/nix/store/00000000000000000000000000000001-test.drv")

    first_id, first_future = await queue.enqueue(_build_request(drv_path, builder="/bin/first"))
    second_id, second_future = await queue.enqueue(_build_request(drv_path, builder="/bin/second"))

    assert second_id == first_id
    assert second_future is first_future
    assert len(queue.queue) == 1
    assert queue.by_path[(str(drv_path), int(BuildMode.NORMAL))].build_id == first_id


@pytest.mark.anyio
@pytest.mark.parametrize("mode", [BuildMode.CHECK, BuildMode.REPAIR])
async def test_a_check_or_repair_never_joins_a_normal_build(mode: BuildMode) -> None:
    """A NORMAL answer proves no determinism and repairs nothing. nixidae/nixidae#1012."""
    queue = BuildQueue()
    drv_path = StorePath("/nix/store/00000000000000000000000000000001-test.drv")

    normal_id, _ = await queue.enqueue(_build_request(drv_path))
    other_id, _ = await queue.enqueue(_build_request(drv_path, mode=mode))
    again_id, _ = await queue.enqueue(_build_request(drv_path, mode=mode))

    assert other_id != normal_id
    assert again_id == other_id


@pytest.mark.anyio
async def test_a_build_that_no_goal_waits_for_ends() -> None:
    """The last goal system that lets go ends the build.

    `build.sh:269` of Nix's functional suite reads this. It builds the
    `cancelled-builds` fixture with `-j2`. The `slow` derivation of that
    fixture writes to a fifo, which blocks until a reader opens the fifo, and
    the only reader is `fast-fail`, which fails. The test then removes the
    directory of the fifo, so no reader can ever appear. Nix answers in about
    two seconds and kills that builder, and pynixd left it running until the
    300 s timeout of the test. Issue nixidae/nixidae#235.
    """
    queue = BuildQueue()
    drv_path = StorePath("/nix/store/00000000000000000000000000000001-slow.drv")

    build_id, future = await queue.enqueue(_build_request(drv_path), from_goal_path=True, goal_request_id=RequestId(1))
    assert queue.by_id[build_id].holders == {RequestId(1)}

    await queue.release_request(RequestId(1))

    assert future.done()
    assert "no goal waits for it" in str(future.result().result.error_msg)


@pytest.mark.anyio
async def test_a_build_that_another_request_still_wants_runs_on() -> None:
    """A build of pynixd serves every client that asked for the same derivation.

    `_the_result_unless_it_stops` in `goals/requests.py` states the rule, and
    this is the count that makes it hold: one request that gives up takes no
    work from the other. Issue nixidae/nixidae#235.
    """
    queue = BuildQueue()
    drv_path = StorePath("/nix/store/00000000000000000000000000000001-slow.drv")

    first_id, future = await queue.enqueue(_build_request(drv_path), from_goal_path=True, goal_request_id=RequestId(1))
    second_id, _ = await queue.enqueue(_build_request(drv_path), from_goal_path=True, goal_request_id=RequestId(2))

    assert second_id == first_id
    assert queue.by_id[first_id].holders == {RequestId(1), RequestId(2)}

    await queue.release_request(RequestId(1))
    assert not future.done()

    await queue.release_request(RequestId(2))
    assert future.done()


@pytest.mark.anyio
async def test_one_request_holds_once_and_releases_once() -> None:
    """`drv^out` and `drv^*` reach one derivation through two goals of one request."""
    queue = BuildQueue()
    drv_path = StorePath("/nix/store/00000000000000000000000000000001-slow.drv")

    build_id, future = await queue.enqueue(_build_request(drv_path), from_goal_path=True, goal_request_id=RequestId(1))
    await queue.enqueue(_build_request(drv_path), from_goal_path=True, goal_request_id=RequestId(1))
    assert queue.by_id[build_id].holders == {RequestId(1)}

    await queue.release_request(RequestId(1))
    assert future.done()
    await queue.release_request(RequestId(1))


@pytest.mark.anyio
async def test_a_build_that_no_goal_ever_held_is_left_alone() -> None:
    """`from_goal_path` marks the calls that take a hold, and no other."""
    queue = BuildQueue()
    drv_path = StorePath("/nix/store/00000000000000000000000000000001-slow.drv")

    build_id, future = await queue.enqueue(_build_request(drv_path))

    assert queue.by_id[build_id].holders == set()

    await queue.release_request(RequestId(1))

    assert not future.done()


@pytest.mark.anyio
async def test_a_goal_build_names_its_request() -> None:
    queue = BuildQueue()
    drv_path = StorePath("/nix/store/00000000000000000000000000000001-slow.drv")
    with pytest.raises(RuntimeError, match="goal request"):
        await queue.enqueue(_build_request(drv_path), from_goal_path=True)


# ── A request that stopped takes no further build slot. Issue nixidae/nixidae#325 ───


def _options(*, keep_going: bool) -> SetOptionsRequest:
    """A `SetOptions` request with every field, because none has a default."""
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


def _failed() -> BuildDerivationResponse:
    """What the daemon answers for a build that did not succeed."""
    return BuildDerivationResponse(
        result=BuildResult(status=BuildResultStatus.PERMANENT_FAILURE, error_msg="hash mismatch"),
    )


def _built() -> BuildDerivationResponse:
    return BuildDerivationResponse(result=BuildResult(status=BuildResultStatus.BUILT))


_X1 = StorePath("/nix/store/00000000000000000000000000000001-x1.drv")
_X2 = StorePath("/nix/store/00000000000000000000000000000002-x2.drv")


async def _two_builds_of(
    queue: BuildQueue,
    request_id: RequestId,
    options: SetOptionsRequest | None = None,
) -> tuple[BuildId, BuildId]:
    """One build to fail and one still pending, both wanted by *request_id*."""
    first, _ = await queue.enqueue(
        _build_request(_X1),
        from_goal_path=True,
        goal_request_id=request_id,
        options=options,
    )
    second, _ = await queue.enqueue(
        _build_request(_X2),
        from_goal_path=True,
        goal_request_id=request_id,
        options=options,
    )
    return first, second


@pytest.mark.anyio
async def test_a_failure_makes_the_rest_of_that_request_unwanted() -> None:
    """x1 fails, so the build slot it frees does not go to x2 of the same request.

    **This test stands for `build.sh:167` and `build.sh:176`.**
    `nix build -f fod-failing.nix -j1 -L` builds four fixed-output derivations
    that all give the wrong hash, and asserts one `error:` line naming x1.
    `Worker::removeGoal` at `worker.cc:173` clears `topGoals` at the first
    failed top goal when `keep-going` is off, so Nix starts nothing further.

    pynixd cannot copy the mechanism -- its goals are coroutines, and the
    request learns of the failure several awaits after the completion -- so it
    takes the decision here instead, where the failure first becomes a fact.
    Measured before this: the goal system reached its decision 2280 us after
    the completion and the next build reached the daemon at 2328 us, so the
    old answer rested on 48 us. Issue nixidae/nixidae#325.
    """
    queue = BuildQueue()
    request = RequestId(1)
    first, second = await _two_builds_of(queue, request, options=_options(keep_going=False))

    await queue.complete(first, _failed())

    assert queue.nobody_wants(queue.by_id[second]) is True


@pytest.mark.anyio
async def test_keep_going_keeps_every_build_of_the_request_wanted() -> None:
    """`--keep-going` is the client saying "build the rest anyway".

    `build.sh:180` runs `nix build -f fod-failing.nix -L x1 x2 x3
    --keep-going` and `build.sh:183` reads four `error:` lines: one hash
    mismatch for each build, and one summary. All three have to run.
    """
    queue = BuildQueue()
    request = RequestId(1)
    first, second = await _two_builds_of(queue, request, options=_options(keep_going=True))

    await queue.complete(first, _failed())

    assert queue.nobody_wants(queue.by_id[second]) is False


@pytest.mark.anyio
async def test_a_build_that_a_second_request_wants_stays_wanted() -> None:
    """A build of pynixd serves every client that asked for the same derivation.

    `AGENTS.md` states the property, and the subset test is what keeps it: the
    failure of one request must not take the work of another. Issue nixidae/nixidae#325.
    """
    queue = BuildQueue()
    gone, live = RequestId(1), RequestId(2)
    first, second = await _two_builds_of(queue, gone, options=_options(keep_going=False))
    await queue.enqueue(
        _build_request(_X2),
        from_goal_path=True,
        goal_request_id=live,
        options=_options(keep_going=False),
    )

    await queue.complete(first, _failed())

    assert queue.nobody_wants(queue.by_id[second]) is False


@pytest.mark.anyio
async def test_a_build_that_succeeds_ends_no_request() -> None:
    """Only a failure stops a request, so a success leaves the queue alone."""
    queue = BuildQueue()
    request = RequestId(1)
    first, second = await _two_builds_of(queue, request, options=_options(keep_going=False))

    await queue.complete(first, _built())

    assert queue.nobody_wants(queue.by_id[second]) is False


@pytest.mark.anyio
async def test_a_build_no_goal_asked_for_is_wanted() -> None:
    """`build_derived_paths` and pynixd's own builds have no goal system.

    Their `holders` is empty, and the empty set is a subset of every
    set, so the answer has to be guarded rather than left to the subset test.
    """
    queue = BuildQueue()
    build_id, _ = await queue.enqueue(
        _build_request(StorePath("/nix/store/00000000000000000000000000000003-x3.drv")),
    )

    assert queue.nobody_wants(queue.by_id[build_id]) is False


@pytest.mark.anyio
async def test_a_request_that_finished_is_forgotten() -> None:
    """The give-up set must not grow for the life of the daemon.

    The release ends the build the request left behind, and it forgets the
    give-up mark: a later build under the same id is wanted again.
    """
    queue = BuildQueue()
    request = RequestId(1)
    first, second = await _two_builds_of(queue, request, options=_options(keep_going=False))
    await queue.complete(first, _failed())
    left_behind = queue.by_id[second]
    assert queue.nobody_wants(left_behind) is True

    await queue.release_request(request)

    assert left_behind.is_done
    assert second not in queue.by_id
    third, _ = await queue.enqueue(
        _build_request(StorePath("/nix/store/00000000000000000000000000000003-x3.drv")),
        from_goal_path=True,
        goal_request_id=request,
        options=_options(keep_going=False),
    )
    assert queue.nobody_wants(queue.by_id[third]) is False


@pytest.mark.anyio
async def test_a_build_with_no_option_set_ends_no_request() -> None:
    """A build that pynixd made itself names no client, so it reads no `keep-going`.

    `_local_slot_is_full` takes the same decision for the same reason: the
    request behind such a build named no option set.
    """
    queue = BuildQueue()
    request = RequestId(1)
    first, second = await _two_builds_of(queue, request)

    await queue.complete(first, _failed())

    assert queue.nobody_wants(queue.by_id[second]) is False
