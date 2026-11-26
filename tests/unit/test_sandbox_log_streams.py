"""A line of pynixd's own sandbox build reaches its client while the build runs.

A sandbox build that keeps its messages until it ends sends its client nothing until then. On the Chromium build of
nixidae/nixidae#1632 that was 70 minutes, and 231,489 messages as objects the garbage collector walked.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import anyio

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.build_queue import BuildQueue
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus, LogNext
from tests._conftest.execution import FakeSandbox, fake_executor, queued, sandbox_settings
from tests._conftest.placement import FakeStore

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from nix_daemon_protocol.logs import LogMessage
    from pynixd.connection import ClientConn

LINE = "compiling chrome\n"


class Client:
    def __init__(self) -> None:
        self.received = b""
        self.got_line = anyio.Event()

    async def send_raw(self, raw: bytes) -> None:
        self.received += raw
        if LINE.encode() in self.received:
            self.got_line.set()


async def test_a_line_reaches_the_client_before_the_build_ends() -> None:
    ends = anyio.Event()

    async def prints(on_log: Callable[[LogMessage], Awaitable[None]]) -> None:
        await on_log(LogNext(text=LINE))

    async def builds() -> BuildDerivationResponse:
        await ends.wait()
        return BuildDerivationResponse(result=BuildResult(status=BuildResultStatus.BUILT))

    stores = {LOCAL_STORE_ID: FakeStore(LOCAL_STORE_ID)}
    queue = BuildQueue()
    sandbox = FakeSandbox(build=builds, during=prints)
    executor = fake_executor(queue, stores, here=sandbox)
    client = Client()
    async with queue.sending(), anyio.create_task_group() as tg:
        build = await queued(queue)
        await queue.subscribe(build.build_id, cast("ClientConn", client))
        tg.start_soon(executor.build_in_sandbox, build, stores[StoreId("local")], sandbox, sandbox_settings(), None)

        with anyio.fail_after(5):
            await client.got_line.wait()
        assert not build.answer.done()
        ends.set()
