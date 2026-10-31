"""`UnixNixReader` reads ahead. Prove it never strands a byte.

The reader buffers, because the protocol makes three reads for every string:
the length, the payload, and its padding. One larger read serves many small ones from memory.
That is also the way it could corrupt a connection: bytes held in this buffer
belong to whatever reads next on the same connection.

These drive `UnixNixReader` over a socket pair that already holds the bytes,
because the property is about the reader's own bookkeeping and not about the
peer.
"""

from __future__ import annotations

import asyncio

import pytest

from pynixd.wire import UnixNixReader
from tests._conftest.socket_pair import fed


async def test_small_reads_across_chunk_boundaries_keep_every_byte() -> None:
    """Reads smaller than the feed arrive split, and still join in order."""
    data = bytes(range(256)) * 64
    async with fed(data) as stream:
        src = UnixNixReader(stream)

        first = await src.readexactly(8)
        rest = b""
        while len(rest) < len(data) - 8:
            rest += await src.readexactly(min(1000, len(data) - 8 - len(rest)))

        assert first + rest == data


async def test_a_read_larger_than_the_readahead_still_joins_the_buffer() -> None:
    """A bulk read bypasses buffering, so it must still take what is buffered.

    Reading one byte fills the buffer. The next read is larger than
    `_READAHEAD`, so it goes straight to the stream -- and it has to start with
    the bytes already held, or it silently skips them.
    """
    data = bytes(range(256)) * 256
    assert len(data) > UnixNixReader._READAHEAD * 2
    async with fed(data) as stream:
        src = UnixNixReader(stream)

        first = await src.readexactly(1)
        bulk = await src.readexactly(UnixNixReader._READAHEAD * 2)

        assert first + bulk == data[: 1 + UnixNixReader._READAHEAD * 2]


async def test_eof_after_a_partial_read_reports_what_arrived() -> None:
    """A short stream raises `IncompleteReadError`, as `readexactly` does.

    The `.partial` holds what arrived, so a caller can tell a truncation from
    an empty read.
    """
    data = b"short"
    async with fed(data) as stream:
        src = UnixNixReader(stream)

        with pytest.raises(asyncio.IncompleteReadError) as caught:
            await src.readexactly(len(data) + 8)

        assert caught.value.partial == data


async def test_the_reader_reports_dirty_while_it_holds_bytes() -> None:
    """`is_dirty` must count the buffer, not only the stream.

    A reader that read ahead and reported clean would let a caller conclude the
    stream was drained while bytes were still owed.
    """
    data = bytes(range(256)) * 64
    async with fed(data) as stream:
        src = UnixNixReader(stream)

        await src.readexactly(8)
        assert await src.is_dirty()

        rest = await src.readexactly(len(data) - 8)
        assert rest == data[8:]
        assert not await src.is_dirty()
