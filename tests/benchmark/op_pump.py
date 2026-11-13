"""Per-operation cost of a real daemon socket, with a client that costs almost nothing (nixidae/nixidae#1345).

`nix-shell shell.nix --run "python tests/benchmark/op_pump.py N SOCKET..."` from pynixd/: for each socket, one
connection, the handshake through pynixd's `Connection`, then N
pre-encoded IsValidPath and N AddTempRoot requests over a blocking socket, one at a time as Nix's client sends them.
Prints microseconds per operation. The path asked for is valid, so both daemons read their database.
"""

from __future__ import annotations

import os
import socket
import struct
import sys
import time
from pathlib import Path

import anyio
import uvloop

from nix_daemon_protocol.constants import FEATURE_OPEN_TELEMETRY, STDERR_LAST
from pynixd.connection import Connection
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter

N = int(sys.argv[1])
SOCKETS = sys.argv[2:]
PATH = os.readlink("/run/current-system").encode()
IS_VALID_PATH, ADD_TEMP_ROOT = 1, 11


def _string(value: bytes) -> bytes:
    return struct.pack("<Q", len(value)) + value + b"\0" * (-len(value) % 8)


async def handshake(path: str) -> tuple[socket.socket, bool]:
    """The connected socket, and whether each op code carries a `traceparent` after it."""
    link = await open_unix_connection(Path(path))
    conn = Connection(UnixNixReader(link, identifier="pump"), UnixNixWriter(link, identifier="pump"), "pump")
    await conn.connect()
    return link.sock.dup(), FEATURE_OPEN_TELEMETRY in conn.standard_features


def answer(sock: socket.socket) -> int:
    """Read the stderr stream to its end and the one uint64 after it; anything but LAST before it is an error."""
    head = sock.recv(8, socket.MSG_WAITALL)
    (msg,) = struct.unpack("<Q", head)
    if msg != STDERR_LAST:
        raise RuntimeError(f"stderr message {msg:#x} before the answer")
    value: int = struct.unpack("<Q", sock.recv(8, socket.MSG_WAITALL))[0]
    return value


def pump(sock: socket.socket, op: int, n: int, *, traced: bool) -> float:
    request = struct.pack("<Q", op) + (_string(b"") if traced else b"") + _string(PATH)
    for _ in range(200):
        sock.sendall(request)
        answer(sock)
    start = time.perf_counter()
    for _ in range(n):
        sock.sendall(request)
        answer(sock)
    return (time.perf_counter() - start) / n * 1e6


for path in SOCKETS:
    sock, traced = anyio.run(
        handshake, path, backend="asyncio", backend_options={"loop_factory": uvloop.new_event_loop}
    )
    sock.setblocking(True)
    valid = pump(sock, IS_VALID_PATH, N, traced=traced)
    root = pump(sock, ADD_TEMP_ROOT, N, traced=traced)
    print(f"{path:<48} IsValidPath {valid:7.1f} us   AddTempRoot {root:7.1f} us   traced={traced}", flush=True)
    sock.close()
