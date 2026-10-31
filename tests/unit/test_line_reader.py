"""`LineReader` reads as `asyncio.StreamReader` does, over chunks that split lines anywhere."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import anyio
import pytest

from pynixd.line_reader import LineReader

if TYPE_CHECKING:
    from anyio.abc import ByteReceiveStream


async def _reader(*chunks: bytes) -> LineReader:
    send, receive = anyio.create_memory_object_stream[bytes](len(chunks))
    for chunk in chunks:
        send.send_nowait(chunk)
    send.close()
    # A memory stream ends with `EndOfStream`, as a process's pipe does.
    return LineReader(cast("ByteReceiveStream", receive))


@pytest.mark.anyio
async def test_lines_cross_chunks_and_the_tail_has_no_newline() -> None:
    reader = await _reader(b"F /a 3\nab", b"c", b"E tick\npart", b"ial")

    assert await reader.readline() == b"F /a 3\n"
    assert await reader.readexactly(3) == b"abc"
    assert await reader.readline() == b"E tick\n"
    assert await reader.readline() == b"partial"
    assert await reader.readline() == b""


@pytest.mark.anyio
async def test_readexactly_past_the_end_raises() -> None:
    reader = await _reader(b"ab")

    with pytest.raises(anyio.IncompleteRead):
        await reader.readexactly(3)
