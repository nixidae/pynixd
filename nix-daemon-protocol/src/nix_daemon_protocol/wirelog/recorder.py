"""A passthrough that records what a client and a daemon say to each other.

The recorder listens on one Unix socket and connects to another. It copies
every byte both ways and writes each read to a file. It writes one file for
each connection, in the order the connections arrive.

    nix client ── recorder ──> nix-daemon

`NIX_DAEMON_SOCKET_PATH` puts it in the path of a test, so no client and no
daemon needs a change.

**This module decodes nothing, and it must keep that property.** A recorder
that reads the protocol can hold a defect of the codecs, and the recording
then measures the recorder. `tests/wirelog/test_recorder_is_dumb.py` states
the rule: this module imports `framing` from this package, and nothing else
of it. Issue nixidae/nixidae#214.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import anyio

from .framing import Direction, encode_chunk, encode_header

if TYPE_CHECKING:
    from pathlib import Path

    from anyio.abc import SocketStream

# One read of a socket. A larger buffer holds more of a NAR in one chunk, and
# the chunk boundaries mean nothing to a decoder either way.
READ_SIZE = 65536


class Recorder:
    """A Unix socket that copies to another one, and writes what it copies."""

    def __init__(self, listen: Path, connect: Path, out_dir: Path, store_dir: str) -> None:
        """Take the socket to serve, the socket to reach, and where to write.

        *store_dir* is the directory that the paths on this connection begin
        with, and it goes in the header of every recording. Passed in, not
        read: this module decodes nothing and imports nothing of the protocol,
        and the store directory is the caller's to know.
        """
        self.listen = listen
        self.connect = connect
        self.out_dir = out_dir
        self.store_dir = store_dir
        self.connections = 0
        self._started = anyio.Event()

    async def serve(self, task_status: object = anyio.TASK_STATUS_IGNORED) -> None:
        """Accept connections until the caller cancels this task."""
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.listen.parent.mkdir(parents=True, exist_ok=True)
        self.listen.unlink(missing_ok=True)
        listener = await anyio.create_unix_listener(self.listen)
        self._started.set()
        task_status.started()  # type: ignore[attr-defined]  # anyio's status object
        async with listener:
            await listener.serve(self._handle)

    async def wait_started(self) -> None:
        """Return once the socket of this recorder accepts a connection."""
        await self._started.wait()

    async def _handle(self, client: SocketStream) -> None:
        index = self.connections
        self.connections += 1
        path = self.out_dir / f"conn-{index:04d}.wire"

        async with await anyio.open_file(path, "wb") as handle:
            await handle.write(encode_header(self.store_dir))
            start = time.monotonic_ns()
            lock = anyio.Lock()

            async def copy(source: SocketStream, sink: SocketStream, direction: Direction) -> None:
                try:
                    while True:
                        try:
                            data = await source.receive(READ_SIZE)
                        except (anyio.EndOfStream, anyio.ClosedResourceError, anyio.BrokenResourceError):
                            return
                        async with lock:
                            await handle.write(encode_chunk(direction, time.monotonic_ns() - start, data))
                            await handle.flush()
                        try:
                            await sink.send(data)
                        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
                            return
                finally:
                    # The other half must stop as well, or a closed client
                    # leaves a copy task waiting on a daemon that says nothing.
                    await sink.aclose()

            async with (
                client,
                await anyio.connect_unix(self.connect) as upstream,
                anyio.create_task_group() as group,
            ):
                group.start_soon(copy, client, upstream, Direction.CLIENT)
                group.start_soon(copy, upstream, client, Direction.SERVER)
