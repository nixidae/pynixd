"""The stream of a build on a builder reaches the client as it arrives.

A stream kept with `keep_logs=True` and sent on when the build ends leaves the client of a long build reading
nothing until then, and holds every message as an object (nixidae/nixidae#1632).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import anyio

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.logs import ActivityField, LogResult, LogStartActivity, LogStopActivity
from nix_daemon_protocol.protocol import ActivityType, FieldType, ResultType, Verbosity
from pynixd import wire
from pynixd.build_queue import BuildQueue
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus
from pynixd.serde.context import WriteContext
from tests._conftest.execution import FakeLocal, Logs, fake_executor, queued
from tests._conftest.placement import FakeStore

if TYPE_CHECKING:
    from nix_daemon_protocol.logs import LogMessage
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd.connection import ClientConn

BUILDER = StoreId("builder1")
LINE = "compiling chrome"
ACTIVITY = 7


class Client:
    def __init__(self) -> None:
        self.received = b""
        self.got_line = anyio.Event()

    async def send_raw(self, raw: bytes) -> None:
        self.received += raw
        if LINE.encode() in self.received:
            self.got_line.set()


def _line() -> LogResult:
    return LogResult(
        act_id=ACTIVITY,
        result_type=ResultType.BUILD_LOG_LINE,
        fields=[ActivityField(type=FieldType.STRING, valstr=LINE)],
    )


async def _serialized(message: LogMessage) -> bytes:
    writer = wire.BytesWriter()
    await message.to_writer(WriteContext(writer=writer, version=wire.PROTOCOL_VERSION))
    return writer.get_bytes()


async def test_a_line_reaches_the_client_before_the_build_ends() -> None:
    ends = anyio.Event()

    async def prints(sink: ClientConn) -> None:
        await sink.send(_line())

    async def builds(request: WireRequest) -> BuildDerivationResponse:
        del request
        await ends.wait()
        return BuildDerivationResponse(result=BuildResult(status=int(BuildResultStatus.BUILT)))

    store = FakeStore(BUILDER)
    store.conn.during = prints
    store.conn.answer = builds
    queue = BuildQueue()
    logs = Logs()
    executor = fake_executor(queue, {store.store_id: store}, local=FakeLocal(), logs=logs)
    client = Client()
    async with queue.sending(), anyio.create_task_group() as tg:
        build = await queued(queue)
        await queue.subscribe(build.build_id, cast("ClientConn", client))
        tg.start_soon(executor.run, build, store)

        with anyio.fail_after(5):
            await client.got_line.wait()
        assert not build.answer.done()
        ends.set()
    assert logs.written[0][1] == f"{LINE}\n".encode()


async def test_an_attempt_that_raises_closes_the_activities_it_opened() -> None:
    async def starts(sink: ClientConn) -> None:
        await sink.send(LogStartActivity(act_id=ACTIVITY, level=Verbosity.INFO, type=ActivityType.BUILD, text="x"))

    async def drops(request: WireRequest) -> BuildDerivationResponse:
        del request
        raise ConnectionResetError(104, "Connection reset by peer")

    store = FakeStore(BUILDER)
    store.conn.during = starts
    store.conn.answer = drops
    queue = BuildQueue()
    executor = fake_executor(queue, {store.store_id: store}, local=FakeLocal())
    build = await queued(queue)

    await executor.run(build, store)

    assert not build.answer.done(), "the build goes back to the queue"
    assert await _serialized(LogStopActivity(act_id=ACTIVITY)) in build.log_bytes()
