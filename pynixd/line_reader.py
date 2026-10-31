"""`readline` and `readexactly` over an anyio byte stream, for a process's output."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

if TYPE_CHECKING:
    from anyio.abc import ByteReceiveStream


class LineReader:
    """The two reads of `asyncio.StreamReader` that the agent and the daemon log need.

    No line length limit: a `@nix` JSON log line has no bound either.
    """

    def __init__(self, stream: ByteReceiveStream) -> None:
        self._stream = stream
        self._buffer = bytearray()

    async def _fill(self) -> bool:
        try:
            self._buffer += await self._stream.receive()
        except anyio.EndOfStream:
            return False
        return True

    async def readline(self) -> bytes:
        """One line with its newline, the rest at the end of the stream, or `b""` after it."""
        start = 0
        while (end := self._buffer.find(b"\n", start)) < 0:
            start = len(self._buffer)
            if not await self._fill():
                line = bytes(self._buffer)
                self._buffer.clear()
                return line
        line = bytes(self._buffer[: end + 1])
        del self._buffer[: end + 1]
        return line

    async def readexactly(self, n: int) -> bytes:
        """*n* bytes; `anyio.IncompleteRead` if the stream ends first."""
        while len(self._buffer) < n:
            if not await self._fill():
                raise anyio.IncompleteRead
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        return data
