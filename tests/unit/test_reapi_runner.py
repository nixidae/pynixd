"""`pynixd reapi-runner`: the closure an action names is made valid before bb_runner runs it.

The guest test `buildbarn-split` drives it with a real bb_worker and bb_runner.
These tests hold the parts it cannot reach cheaply: the protobuf reader, and
the order and errors of the proxy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pytest
from grpclib.client import Channel, UnaryUnaryMethod
from grpclib.const import Cardinality, Handler, Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server

from nix_daemon_protocol.store_path import StorePath
from pynixd.exceptions import BackendError
from pynixd.reapi_runner import SERVICE, Closures, RawCodec, RunnerProxy, closure_root, program

if TYPE_CHECKING:
    from pathlib import Path

    from grpclib.server import Stream

CC = "/nix/store/00000000000000000000000000000000-cc"


def _varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _length(number: int, payload: bytes) -> bytes:
    return _varint(number << 3 | 2) + _varint(len(payload)) + payload


def run_request(*arguments: str) -> bytes:
    """A `RunRequest` as protobuf writes it: an environment entry (2), the arguments (1), a working directory (3).

    The environment comes first, so the reader must find argv[0] past another field.
    """
    data = _length(2, _length(1, b"LANG") + _length(2, b"C"))
    data += b"".join(_length(1, arg.encode()) for arg in arguments)
    return data + _length(3, b"out/x") + _varint(9 << 3) + _varint(300)


def test_program_is_the_first_argument() -> None:
    assert program(run_request(f"{CC}/bin/cc", "-c", "x.c")) == f"{CC}/bin/cc"
    assert program(run_request()) is None


def test_a_truncated_request_is_refused() -> None:
    with pytest.raises(ValueError, match="truncated"):
        program(_length(1, f"{CC}/bin/cc".encode())[:-3])


def test_closure_root() -> None:
    assert closure_root(f"{CC}/bin/cc") == StorePath(CC)
    assert closure_root(CC) == StorePath(CC)
    assert closure_root("../../third_party/llvm-build/bin/clang") is None
    assert closure_root("/usr/bin/cc") is None
    assert closure_root(None) is None
    assert closure_root("/nix/store/") is None


@dataclass
class FakeClosures(Closures):
    ensured: list[StorePath] = field(default_factory=list)
    error: Exception | None = None

    async def ensure(self, path: StorePath) -> None:
        if self.error is not None:
            raise self.error
        self.ensured.append(path)


@dataclass
class FakeRunner:
    """bb_runner: answers `Run` with a fixed response and records what it saw."""

    seen: list[bytes] = field(default_factory=list)
    closures: FakeClosures | None = None
    ensured_before_run: list[StorePath] = field(default_factory=list)

    def __mapping__(self) -> dict[str, Handler]:
        return {
            f"{SERVICE}/Run": Handler(self._run, Cardinality.UNARY_UNARY, bytes, bytes),
            f"{SERVICE}/CheckReadiness": Handler(self._ready, Cardinality.UNARY_UNARY, bytes, bytes),
        }

    async def _run(self, stream: Stream[bytes, bytes]) -> None:
        request = await stream.recv_message()
        assert request is not None
        self.seen.append(request)
        if self.closures is not None:
            self.ensured_before_run = list(self.closures.ensured)
        await stream.send_message(b"\x08\x07")  # exit_code = 7

    async def _ready(self, stream: Stream[bytes, bytes]) -> None:
        request = await stream.recv_message()
        assert request is not None
        await stream.send_message(b"")


async def _call(socket: Path, method: str, request: bytes) -> bytes:
    channel = Channel(path=str(socket), codec=RawCodec())
    try:
        return await UnaryUnaryMethod[bytes, bytes](channel, f"{SERVICE}/{method}", bytes, bytes)(request)
    finally:
        channel.close()


async def _serve(tmp_path: Path, closures: FakeClosures) -> tuple[FakeRunner, Server, Server, Channel]:
    upstream = tmp_path / "bb-runner.sock"
    listen = tmp_path / "runner.sock"
    runner = FakeRunner(closures=closures)
    bb_runner = Server([runner], codec=RawCodec())
    await bb_runner.start(path=str(upstream))
    channel = Channel(path=str(upstream), codec=RawCodec())
    proxy = Server([RunnerProxy(closures, channel)], codec=RawCodec())
    await proxy.start(path=str(listen))
    return runner, bb_runner, proxy, channel


async def _stop(*servers: Server) -> None:
    for server in servers:
        server.close()
        await server.wait_closed()


async def test_the_closure_is_valid_before_the_command_runs(tmp_path: Path) -> None:
    closures = FakeClosures(tmp_path / "daemon.sock")
    runner, bb_runner, proxy, channel = await _serve(tmp_path, closures)
    try:
        request = run_request(f"{CC}/bin/cc", "-c", "x.c")
        response = await _call(tmp_path / "runner.sock", "Run", request)
        await _call(tmp_path / "runner.sock", "CheckReadiness", b"")
    finally:
        channel.close()
        await _stop(proxy, bb_runner)
    assert response == b"\x08\x07"
    assert runner.seen == [request], "the request reaches bb_runner byte for byte"
    assert runner.ensured_before_run == [StorePath(CC)]


async def test_a_program_outside_the_store_runs_unchanged(tmp_path: Path) -> None:
    closures = FakeClosures(tmp_path / "daemon.sock")
    runner, bb_runner, proxy, channel = await _serve(tmp_path, closures)
    try:
        await _call(tmp_path / "runner.sock", "Run", run_request("/bin/sh", "-c", "true"))
    finally:
        channel.close()
        await _stop(proxy, bb_runner)
    assert closures.ensured == []
    assert len(runner.seen) == 1


@pytest.mark.parametrize(
    ("error", "status"),
    [
        # Siso fails the step on FAILED_PRECONDITION (`reapi/exec.go`), and retries UNAVAILABLE.
        (BackendError("no substituter has it"), Status.FAILED_PRECONDITION),
        (ConnectionRefusedError("daemon down"), Status.UNAVAILABLE),
    ],
)
async def test_a_closure_that_cannot_be_fetched_is_an_error_status(
    tmp_path: Path, error: Exception, status: Status
) -> None:
    """Not an exit code: a client reads that as the compiler's own failure."""
    closures = FakeClosures(tmp_path / "daemon.sock", error=error)
    runner, bb_runner, proxy, channel = await _serve(tmp_path, closures)
    try:
        with pytest.raises(GRPCError) as caught:
            await _call(tmp_path / "runner.sock", "Run", run_request(f"{CC}/bin/cc"))
    finally:
        channel.close()
        await _stop(proxy, bb_runner)
    assert caught.value.status == status
    assert str(error) in (caught.value.message or "")
    assert runner.seen == [], "bb_runner never sees the action"


async def test_a_request_that_is_not_protobuf_is_refused(tmp_path: Path) -> None:
    closures = FakeClosures(tmp_path / "daemon.sock")
    runner, bb_runner, proxy, channel = await _serve(tmp_path, closures)
    try:
        with pytest.raises(GRPCError) as caught:
            await _call(tmp_path / "runner.sock", "Run", b"\x0a\x10short")
    finally:
        channel.close()
        await _stop(proxy, bb_runner)
    assert caught.value.status == Status.INVALID_ARGUMENT
    assert runner.seen == []
