"""A backend failure inside a task group is still a backend failure.

`stream_paths_store_to_store` reads the destination's answer in a task
group, so a `BackendError` from the builder reaches `execute_build` as an
`ExceptionGroup`. `except (BackendError, InfrastructureError)` does not
match a group, so the build was never retried, and the client read
"Internal scheduler error" in place of the builder's message. Seen on
dynhetz: "path '.../boehm-gc-8.2.12' is not valid", twice. Issues #889 and
#890.
"""

from __future__ import annotations

import contextlib
from typing import Any, cast

import asyncssh
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.exceptions import BackendError
from pynixd.scheduler import Scheduler

NOT_VALID = "path '/nix/store/x5lf684477ybm9xb0i70p0d89dp5w23b-boehm-gc-8.2.12' is not valid"


class FakeQueue:
    def __init__(self) -> None:
        self.failed: list[str] = []

    async def complete(self, build_id: str, response: Any) -> None:
        raise AssertionError("nothing completes here")

    async def fail(self, build_id: str, error_msg: str) -> None:
        self.failed.append(error_msg)


class FakeBuild:
    build_id = "b1"
    assigned_store_id: StoreId | None = None
    options = None
    enqueued_at = 0.0
    started_at: float | None = None

    def __init__(self) -> None:
        self.messages: list[str] = []
        self.retried: list[tuple[StoreId, str]] = []

    async def post_log_and_fanout(self, msg: Any) -> None:
        self.messages.append(str(getattr(msg, "text", "")))

    def reset_for_retry(self, store_id: StoreId, reason: str) -> None:
        self.retried.append((store_id, reason))


class FakeStore:
    store_id = StoreId("builder1")

    def build_conn(self, _options: Any = None) -> Any:
        return contextlib.nullcontext(object())


class FakeScheduler:
    def __init__(self, raised: BaseException) -> None:
        self.queue = FakeQueue()
        self.raised = raised
        self.local_store = None

    def trigger(self) -> None:
        return

    def _note_disk_full(self, *_args: Any) -> None:
        return

    async def _prepare_build(self, *_args: Any) -> None:
        raise self.raised


async def _run(raised: BaseException) -> tuple[FakeScheduler, FakeBuild]:
    scheduler = FakeScheduler(raised)
    build = FakeBuild()
    await Scheduler.execute_build(cast("Any", scheduler), cast("Any", build), cast("Any", FakeStore()))
    return scheduler, build


@pytest.mark.anyio
async def test_a_grouped_backend_error_retries_with_its_message() -> None:
    scheduler, build = await _run(ExceptionGroup("unhandled errors in a TaskGroup", [BackendError(NOT_VALID)]))

    assert scheduler.queue.failed == []
    assert build.retried == [(StoreId("builder1"), NOT_VALID)]
    assert any(NOT_VALID in m for m in build.messages)


@pytest.mark.anyio
async def test_a_bare_backend_error_still_retries() -> None:
    scheduler, build = await _run(BackendError(NOT_VALID))

    assert scheduler.queue.failed == []
    assert build.retried == [(StoreId("builder1"), NOT_VALID)]


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
    scheduler, build = await _run(ExceptionGroup("g", [dropped]))

    assert scheduler.queue.failed == []
    assert [store for store, _ in build.retried] == [StoreId("builder1")]


@pytest.mark.anyio
async def test_a_group_holding_a_pynixd_fault_fails_and_says_what_failed() -> None:
    """A group with one fault of pynixd in it is not retryable, even beside
    a backend error: retrying would hide the fault."""
    fault = ExceptionGroup("g", [BackendError(NOT_VALID), KeyError("worker")])
    scheduler, build = await _run(fault)

    assert build.retried == []
    assert len(scheduler.queue.failed) == 1
    assert "KeyError" in scheduler.queue.failed[0]
    assert NOT_VALID in scheduler.queue.failed[0]
