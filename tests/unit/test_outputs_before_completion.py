"""A build whose outputs did not arrive is not a build that succeeded.

`execute_build` completed the build and then pulled the outputs, catching a
pull failure and writing it to the server's log. The client already held a
successful `BuildResult` by then. It went on to ask the local store for a
path that is not there, and the error it got named the missing path rather
than the copy that failed. Issue #12.

The ordering was never a race: `_collect_outputs` is awaited in the same
coroutine, so `execute_build` did not return until it finished. The
swallowed failure is the defect.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.scheduler import Scheduler
from pynixd.serde import BuildResultStatus

if TYPE_CHECKING:
    from nix_daemon_protocol.logs import LogMessage

PULL_FAILED = "the builder hung up mid-copy"


class FakeResult:
    status = 0
    error_msg = ""


class FakeResponse:
    result = FakeResult()


class FakeQueue:
    """Records which of the two ways a build was resolved, and with what."""

    def __init__(self, steps: list[str]) -> None:
        self.resolved: list[tuple[str, Any]] = []
        self.steps = steps

    async def complete(self, build_id: str, response: Any) -> None:
        self.steps.append("complete")
        self.resolved.append(("complete", response))

    async def fail(self, build_id: str, error_msg: str) -> None:
        self.steps.append("fail")
        self.resolved.append(("fail", error_msg))


class FakeBuild:
    build_id = "b1"
    assigned_store_id: StoreId | None = None
    options = None
    enqueued_at = 0.0
    started_at: float | None = None

    def __init__(self) -> None:
        self.messages: list[str] = []

    async def post_log_and_fanout(self, msg: LogMessage) -> None:
        self.messages.append(str(getattr(msg, "text", "")))


class FakeStore:
    store_id = StoreId("builder1")

    def build_conn(self, _options: Any = None) -> Any:
        return contextlib.nullcontext(object())


class FakeScheduler:
    """Enough of `Scheduler` for `execute_build`, with the collection wired
    to fail. `execute_build` is called unbound against this, the way
    `test_scheduler_build_note.py` calls `_say_where_it_builds`."""

    def __init__(self, *, collection_fails: bool) -> None:
        self.steps: list[str] = []
        """Every step that matters, in the order it ran. A test that checks
        only which steps happened passes against the old order too."""
        self.queue = FakeQueue(self.steps)
        self.collection_fails = collection_fails
        self.triggers = 0
        self.local_store = None
        """No local store here: `execute_build` records nothing without one."""

    def trigger(self) -> None:
        self.triggers += 1

    async def _prepare_build(self, *_args: Any) -> None:
        return

    async def _say_where_it_builds(self, *_args: Any) -> None:
        return

    async def _execute(self, *_args: Any) -> FakeResponse:
        return FakeResponse()

    async def _collect_outputs(self, *_args: Any) -> None:
        self.steps.append("collect")
        if self.collection_fails:
            raise RuntimeError(PULL_FAILED)


async def _run(*, collection_fails: bool) -> tuple[FakeScheduler, FakeBuild]:
    scheduler = FakeScheduler(collection_fails=collection_fails)
    build = FakeBuild()
    await Scheduler.execute_build(cast("Any", scheduler), cast("Any", build), cast("Any", FakeStore()))
    return scheduler, build


class TestACollectionFailure:
    @pytest.mark.anyio
    async def test_fails_the_build_instead_of_completing_it(self):
        scheduler, _ = await _run(collection_fails=True)

        assert [kind for kind, _ in scheduler.queue.resolved] == ["fail"]

    @pytest.mark.anyio
    async def test_says_what_failed_and_where(self):
        scheduler, _ = await _run(collection_fails=True)

        (_, reason) = scheduler.queue.resolved[0]
        assert "builder1" in reason
        assert PULL_FAILED in reason

    @pytest.mark.anyio
    async def test_reaches_the_client_and_not_only_the_server_log(self):
        """The acceptance of issue #12. A log line on the server is not the
        client being told."""
        _, build = await _run(collection_fails=True)

        assert any(PULL_FAILED in message for message in build.messages)


class TestASuccessfulBuild:
    @pytest.mark.anyio
    async def test_collects_before_it_completes(self):
        """The order, not the set. Both steps ran in the old code too."""
        scheduler, _ = await _run(collection_fails=False)

        assert scheduler.steps == ["collect", "complete"]

    @pytest.mark.anyio
    async def test_completes_with_the_backend_response(self):
        scheduler, _ = await _run(collection_fails=False)

        (_, response) = scheduler.queue.resolved[0]
        assert response.result.status == BuildResultStatus.BUILT
