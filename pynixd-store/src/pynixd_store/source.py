"""The one way the store engine reads a dump or a NAR, as Nix reads a `Source` (`serialise.hh`).

A source answers `read(n)` with at most *n* bytes, and `b""` at the end.
The engine reads it from a worker thread through `BlockingReader`. The
adapters are `FramedReader` (a client's frames, in `wire`), `Pieces` (an
async iterator: a cache, a daemon, a decompressed file) and the wrappers
here. This module imports nothing of the wire protocol.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Self

import anyio
import anyio.from_thread

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable
    from types import TracebackType

CHUNK = 64 * 1024
"""What `BlockingReader` asks for at once: one hop to the event loop per frame, not per NAR token."""


class Source(Protocol):
    async def read(self, n: int) -> bytes:
        """At most *n* bytes, and `b""` at the end."""
        ...

    async def skip(self, n: int) -> None:
        """Discard *n* bytes; `EOFError` when the source ends first."""
        ...


class Bounded:
    """`EnsureRead` (`serialise.hh:768`) with a cap: at most *size* bytes, and `finish` skips the rest.

    Nix's parser never reads past a NAR's end, so `EnsureRead` needs no cap.
    `BlockingReader` reads ahead, and the cap keeps it out of the next record
    of a shared stream. For a NAR whose end is at *size*, nothing differs.
    """

    def __init__(self, source: Source, size: int) -> None:
        self._source = source
        self._left = size

    async def read(self, n: int) -> bytes:
        data = await self._source.read(min(n, self._left))
        self._left -= len(data)
        return data

    async def skip(self, n: int) -> None:
        n = min(n, self._left)
        await self._source.skip(n)
        self._left -= n

    async def finish(self) -> None:
        if self._left:
            await self.skip(self._left)


class Prefixed:
    """`ChainSource` (`serialise.hh:476`): *prefix*, then what *source* gives."""

    def __init__(self, prefix: bytes, source: Source) -> None:
        self._prefix = memoryview(prefix)
        self._source = source

    async def read(self, n: int) -> bytes:
        if not self._prefix:
            return await self._source.read(n)
        out = bytes(self._prefix[:n])
        self._prefix = self._prefix[n:]
        return out

    async def skip(self, n: int) -> None:
        taken = min(n, len(self._prefix))
        self._prefix = self._prefix[taken:]
        if n > taken:
            await self._source.skip(n - taken)


async def read_up_to(source: Source, limit: int) -> tuple[bytes, bool]:
    """At most *limit* bytes of *source*, and whether *source* ended before *limit*."""
    pieces: list[bytes] = []
    size = 0
    while size < limit:
        piece = await source.read(min(CHUNK, limit - size))
        if not piece:
            return b"".join(pieces), True
        pieces.append(piece)
        size += len(piece)
    return b"".join(pieces), False


class Metered:
    """*source*, with *on_bytes* told the size of each read."""

    def __init__(self, source: Source, on_bytes: Callable[[int], None] | None) -> None:
        self._source = source
        self._on_bytes = on_bytes

    async def read(self, n: int) -> bytes:
        data = await self._source.read(n)
        if self._on_bytes is not None:
            self._on_bytes(len(data))
        return data

    async def skip(self, n: int) -> None:
        await self._source.skip(n)


class Pieces:
    """An async iterator of pieces as a source; `aclose` closes the iterator, read to the end or not.

    A daemon connection that streams the pieces leaves dirty when closed
    mid-NAR, and the pool discards it (`Connection.__aexit__`).
    """

    def __init__(self, pieces: AsyncIterator[bytes]) -> None:
        self._pieces = pieces
        self._piece = memoryview(b"")

    async def read(self, n: int) -> bytes:
        while not self._piece:
            piece = await anext(self._pieces, None)
            if piece is None:
                return b""
            self._piece = memoryview(piece)
        data = bytes(self._piece[:n])
        self._piece = self._piece[n:]
        return data

    async def skip(self, n: int) -> None:
        while n:
            data = await self.read(n)
            if not data:
                raise EOFError(f"the source ended, {n} more bytes to skip")
            n -= len(data)

    async def aclose(self) -> None:
        aclose = getattr(self._pieces, "aclose", None)
        if aclose is not None:
            await aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        with anyio.CancelScope(shield=True):
            await self.aclose()


class BlockingReader:
    """The sync `read` that `restore_path` takes, over a source, for a worker thread.

    It asks the event loop for `CHUNK` bytes at a time and serves the parser's
    small reads from that. What it reads ahead past the archive is lost, so a
    source shared with a next record goes in `Bounded` first.
    """

    def __init__(self, source: Source) -> None:
        self._source = source
        self._buffer = memoryview(b"")

    def __call__(self, n: int) -> bytes:
        if not self._buffer:
            self._buffer = memoryview(anyio.from_thread.run(self._source.read, CHUNK))
        out = bytes(self._buffer[:n])
        self._buffer = self._buffer[n:]
        return out
