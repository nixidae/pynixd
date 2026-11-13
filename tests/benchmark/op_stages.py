"""Where one IsValidPath costs its time in pynixd's serving path, stage by stage (nixidae/nixidae#1345).

`nix-shell shell.nix --run "python tests/benchmark/op_stages.py [N]"` from pynixd/. Codec stages run in memory.
Round trips run over a socketpair against a client in another process with a blocking socket, so the client's
Python is off the server's loop and its GIL. Every server runs on uvloop, as pynixd does. A faster codec is
compared by its `from_reader` and `to_writer` lines.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import multiprocessing.connection
import socket
import struct
import sys
import time
from multiprocessing.process import BaseProcess

import anyio
import uvloop

from nix_daemon_protocol.constants import PROTOCOL_VERSION, STDERR_LAST
from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.io import BytesReader, BytesWriter
from nix_daemon_protocol.is_valid_path import IsValidPathRequest, IsValidPathResponse
from pynixd.unix_socket import link_socket
from pynixd.wire import UnixNixReader, UnixNixWriter

N = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000
PATH = "/nix/store/" + "a" * 32 + "-hello-2.12.1"
V = PROTOCOL_VERSION


def _string(value: bytes) -> bytes:
    return struct.pack("<Q", len(value)) + value + b"\0" * (-len(value) % 8)


BODY = _string(PATH.encode())
REQUEST = struct.pack("<Q", IsValidPathRequest.op) + BODY
ANSWER = struct.pack("<QQ", STDERR_LAST, 1)


def report(name: str, seconds: float, n: int = N) -> None:
    print(f"{name:<44} {seconds / n * 1e6:8.2f} us/op", flush=True)


async def codec_stages() -> None:
    start = time.perf_counter()
    for _ in range(N):
        await BytesReader(BODY).read_string()
    report("decode: BytesReader.read_string", time.perf_counter() - start)

    start = time.perf_counter()
    for _ in range(N):
        await IsValidPathRequest.from_reader(ReadContext(reader=BytesReader(BODY), version=V))
    report("decode: IsValidPathRequest.from_reader", time.perf_counter() - start)

    start = time.perf_counter()
    for _ in range(N):
        await IsValidPathResponse(valid=True).to_writer(WriteContext(writer=BytesWriter(), version=V))
    report("encode: IsValidPathResponse.to_writer", time.perf_counter() - start)

    start = time.perf_counter()
    for _ in range(N):
        IsValidPathResponse(valid=True)
    report("encode: IsValidPathResponse(...) alone", time.perf_counter() - start)


def client(sock: socket.socket, n: int, out: multiprocessing.connection.Connection) -> None:
    sock.setblocking(True)
    for _ in range(200):
        sock.sendall(REQUEST)
        sock.recv(16, socket.MSG_WAITALL)
    start = time.perf_counter()
    for _ in range(n):
        sock.sendall(REQUEST)
        got = sock.recv(16, socket.MSG_WAITALL)
        if len(got) != 16:
            raise RuntimeError(f"short answer: {got!r}")
    out.send(time.perf_counter() - start)
    sock.close()


def pair(n: int) -> tuple[socket.socket, BaseProcess, multiprocessing.connection.Connection]:
    server, peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    recv, send = multiprocessing.Pipe(duplex=False)
    proc = multiprocessing.get_context("fork").Process(target=client, args=(peer, n, send))
    proc.start()
    peer.close()
    return server, proc, recv


class _Floor(asyncio.Protocol):
    """The least an asyncio server does: count bytes, answer each request with a fixed answer."""

    def __init__(self, done: asyncio.Future[None], total: int) -> None:
        self.done, self.left, self.pending = done, total, 0

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport

    def data_received(self, data: bytes) -> None:
        self.pending += len(data)
        while self.pending >= len(REQUEST):
            self.pending -= len(REQUEST)
            self.transport.write(ANSWER)  # type: ignore[attr-defined]
            self.left -= 1
        if self.left <= 0 and not self.done.done():
            self.done.set_result(None)

    def connection_lost(self, exc: Exception | None) -> None:
        if not self.done.done():
            self.done.set_result(None)


async def floor_protocol() -> None:
    sock, proc, recv = pair(N)
    loop = asyncio.get_running_loop()
    done: asyncio.Future[None] = loop.create_future()
    await loop.connect_accepted_socket(lambda: _Floor(done, N + 200), sock)
    await done
    proc.join()
    report("round trip: asyncio Protocol, fixed answer", recv.recv())


async def pynixd_wire(codec: bool) -> None:
    sock, proc, recv = pair(N)
    link = await link_socket(sock)
    r, w = UnixNixReader(link, identifier="s"), UnixNixWriter(link, identifier="s")
    try:
        for _ in range(N + 200):
            await r.read_uint64()
            if codec:
                await IsValidPathRequest.from_reader(ReadContext(reader=r, version=V))
                await IsValidPathResponse(valid=True).to_writer(WriteContext(writer=w, version=V))
            else:
                await r.read_string()
                w.write_uint64(STDERR_LAST)
                w.write_uint64(1)
            await w.drain()
    finally:
        await link.stream.aclose()
    proc.join()
    label = "round trip: pynixd wire + codec" if codec else "round trip: pynixd wire, hand-written answer"
    report(label, recv.recv())


async def main() -> None:
    await codec_stages()
    await floor_protocol()
    await pynixd_wire(codec=False)
    await pynixd_wire(codec=True)


if __name__ == "__main__":
    anyio.run(main, backend="asyncio", backend_options={"loop_factory": uvloop.new_event_loop})
