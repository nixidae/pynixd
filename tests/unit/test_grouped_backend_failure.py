"""A backend failure inside a task group is still a backend failure.

`stream_paths_store_to_store` reads the destination's answer in a task
group, so a `BackendError` from the builder reaches `Executor.run` as an
`ExceptionGroup`. `except (BackendError, InfrastructureError)` does not
match a group, so the build was never retried, and the client read
"Internal scheduler error" in place of the builder's message. Seen on
dynhetz: "path '.../boehm-gc-8.2.12' is not valid", twice. Issues #889 and
#890.
"""

from __future__ import annotations

import asyncssh
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue, QueuedBuild
from pynixd.exceptions import BackendError
from pynixd.serde import QueryValidPathsResponse
from tests._conftest.execution import FakeLocal, Streamed, fake_executor, log_bytes, queued
from tests._conftest.placement import FakeStore

NOT_VALID = "path '/nix/store/x5lf684477ybm9xb0i70p0d89dp5w23b-boehm-gc-8.2.12' is not valid"
INPUT = "/nix/store/00000000000000000000000000000002-input"


async def _run(raised: Exception) -> QueuedBuild:
    """A build whose input push to the builder raised *raised*."""
    store = FakeStore(StoreId("builder1"))

    async def nothing_there(_request: object) -> QueryValidPathsResponse:
        return QueryValidPathsResponse(paths=set())

    store.answer = nothing_there
    queue = BuildQueue()
    build = await queued(queue, input_srcs=[INPUT])
    stream = Streamed(fail_first=1, error=raised)
    await fake_executor(queue, {store.store_id: store}, local=FakeLocal(), stream=stream).run(build, store)
    return build


@pytest.mark.anyio
async def test_a_grouped_backend_error_retries_with_its_message() -> None:
    build = await _run(ExceptionGroup("unhandled errors in a TaskGroup", [BackendError(NOT_VALID)]))

    assert not build.answer.done()
    assert build.store_errors == {StoreId("builder1"): NOT_VALID}
    assert NOT_VALID.encode() in log_bytes(build)


@pytest.mark.anyio
async def test_a_bare_backend_error_still_retries() -> None:
    build = await _run(BackendError(NOT_VALID))

    assert not build.answer.done()
    assert build.store_errors == {StoreId("builder1"): NOT_VALID}


@pytest.mark.parametrize(
    "dropped",
    [
        EOFError("SSH connection closed"),
        ConnectionResetError("reset by peer"),
        asyncssh.ConnectionLost("keepalive timeout"),
    ],
    ids=["eof", "reset", "keepalive"],
)
@pytest.mark.anyio
async def test_a_dropped_connection_retries_elsewhere(dropped: Exception) -> None:
    """A builder that went silent and lost its connection is the builder's
    fault, not pynixd's. nixidae/nixidae#1063 saw 12 builds fail with
    `EOFError: SSH connection closed`."""
    build = await _run(ExceptionGroup("g", [dropped]))

    assert not build.answer.done()
    assert list(build.store_failures) == [StoreId("builder1")]


@pytest.mark.anyio
async def test_a_group_holding_a_pynixd_fault_fails_and_says_what_failed() -> None:
    """A group with one fault of pynixd in it is not retryable, even beside
    a backend error: retrying would hide the fault."""
    fault = ExceptionGroup("g", [BackendError(NOT_VALID), KeyError("worker")])
    build = await _run(fault)

    assert build.store_failures == {}
    failed = build.answer.result().result.error_msg
    assert "KeyError" in failed
    assert NOT_VALID in failed
