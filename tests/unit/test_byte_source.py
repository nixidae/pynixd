"""The store engine's byte source (`pynixd/store/source.py`) and `FramedReader.read`.

The case that matters is op 44: one framed stream holds a path's NAR and the
next path's info, often in the same frame. `BlockingReader` reads ahead, and
`Bounded` keeps it inside the NAR (`EnsureRead`, `daemon.cc:564`).
"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING

import anyio.to_thread
import pytest

from pynixd.store.source import BlockingReader, Bounded, Metered, Pieces
from pynixd.wire import BytesReader, FramedReader

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


def _frames(*chunks: bytes) -> FramedReader:
    body = b"".join(struct.pack("<Q", len(c)) + c for c in chunks) + struct.pack("<Q", 0)
    return FramedReader(BytesReader(body))


async def _pieces(*pieces: bytes) -> AsyncGenerator[bytes]:
    for piece in pieces:
        yield piece


async def test_framed_read_gives_at_most_n_and_keeps_the_rest() -> None:
    frames = _frames(b"abcdef", b"gh")
    assert await frames.read(4) == b"abcd"
    assert await frames.read(100) == b"ef"
    assert await frames.read(100) == b"gh"
    assert await frames.read(100) == b""


async def test_bounded_stops_at_its_size_and_finish_skips_the_rest() -> None:
    frames = _frames(b"NARNARnext", b"more")
    nar = Bounded(frames, 6)
    assert await nar.read(4) == b"NARN"
    await nar.finish()
    assert await frames.read(100) == b"next"


async def test_bounded_never_gives_past_its_size() -> None:
    nar = Bounded(_frames(b"NARnext"), 3)
    assert await nar.read(100) == b"NAR"
    assert await nar.read(100) == b""


async def test_a_blocking_reader_over_bounded_leaves_the_next_record() -> None:
    frames = _frames(b"x" * 10 + b"next")
    nar = Bounded(frames, 10)
    reader = BlockingReader(nar)

    def parse() -> bytes:
        return reader(4) + reader(6)

    assert await anyio.to_thread.run_sync(parse) == b"x" * 10
    await nar.finish()
    assert await frames.read(100) == b"next"


async def test_pieces_reads_across_pieces_and_skips() -> None:
    async with Pieces(_pieces(b"ab", b"", b"cdef")) as source:
        assert await source.read(3) == b"ab"
        await source.skip(2)
        assert await source.read(10) == b"ef"
        assert await source.read(10) == b""
        with pytest.raises(EOFError):
            await source.skip(1)


async def test_pieces_closes_its_iterator_unread() -> None:
    closed: list[bool] = []

    async def pieces() -> AsyncGenerator[bytes]:
        try:
            yield b"a"
            yield b"b"
        finally:
            closed.append(True)

    async with Pieces(pieces()) as source:
        assert await source.read(1) == b"a"
    assert closed == [True]


async def test_metered_counts_each_read() -> None:
    seen: list[int] = []
    source = Metered(_frames(b"abc", b"de"), seen.append)
    assert await source.read(10) == b"abc"
    assert await source.read(10) == b"de"
    assert await source.read(10) == b""
    assert seen == [3, 2, 0]
