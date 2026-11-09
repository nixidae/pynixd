"""A build whose outputs did not arrive is not a build that succeeded.

`Executor.run` completed the build and then pulled the outputs, catching a
pull failure and writing it to the server's log. The client already held a
successful `BuildResult` by then. It went on to ask the local store for a
path that is not there, and the error it got named the missing path rather
than the copy that failed. Issue #12.

The ordering was never a race: `collect_outputs` is awaited in the same
coroutine, so `Executor.run` did not return until it finished. The
swallowed failure is the defect.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue, QueuedBuild
from pynixd.serde import BuildResultStatus
from tests._conftest.execution import FakeLocal, Streamed, Triggers, answering, fake_executor, log_bytes, queued
from tests._conftest.placement import FakeStore

PULL_FAILED = "the builder hung up mid-copy"
OUT = "/nix/store/00000000000000000000000000000001-out"


@dataclass
class Run:
    build: QueuedBuild
    steps: list[str]
    """Every step that matters, in the order it ran. A test that checks
    only which steps happened passes against the old order too."""


async def _run(*, collection_fails: bool) -> Run:
    steps: list[str] = []
    store = FakeStore(StoreId("builder1"))
    store.conn.answer = answering(BuildResultStatus.BUILT)
    stream = Streamed(fail_first=3 if collection_fails else 0, error=OSError(PULL_FAILED))
    stream.before = lambda: steps.append("collect")
    queue = BuildQueue()
    build = await queued(queue, outputs={"out": OUT})
    executor = fake_executor(queue, {store.store_id: store}, local=FakeLocal(), stream=stream, trigger=Triggers(steps))
    await executor.run(build, store)
    return Run(build, steps)


class TestACollectionFailure:
    @pytest.mark.anyio
    async def test_fails_the_build_instead_of_completing_it(self) -> None:
        run = await _run(collection_fails=True)

        assert run.build.answer.result().result.status == BuildResultStatus.MISC_FAILURE

    @pytest.mark.anyio
    async def test_says_what_failed_and_where(self) -> None:
        run = await _run(collection_fails=True)

        reason = run.build.answer.result().result.error_msg
        assert "builder1" in reason
        assert PULL_FAILED in reason

    @pytest.mark.anyio
    async def test_reaches_the_client_and_not_only_the_server_log(self) -> None:
        """The acceptance of issue #12. A log line on the server is not the
        client being told."""
        run = await _run(collection_fails=True)

        assert PULL_FAILED.encode() in log_bytes(run.build)


class TestASuccessfulBuild:
    @pytest.mark.anyio
    async def test_collects_before_it_completes(self) -> None:
        """The order, not the set. Both steps ran in the old code too."""
        run = await _run(collection_fails=False)

        assert run.steps == ["collect", "trigger"]

    @pytest.mark.anyio
    async def test_completes_with_the_backend_response(self) -> None:
        run = await _run(collection_fails=False)

        assert run.build.answer.result().result.status == BuildResultStatus.BUILT
