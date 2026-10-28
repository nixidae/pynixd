"""Unit tests for pynixd.wire protocol primitives."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pynixd.store_path import StorePath
from pynixd.wire import (
    BytesReader,
    BytesWriter,
    FramedReader,
    FramedWriter,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

# ═════════════════════════════════════════════════════════════════════════════
# 1. Primitive wire protocol roundtrips
# ═════════════════════════════════════════════════════════════════════════════


class TestWirePrimitives:
    """Roundtrip tests for NixReader/NixWriter primitive methods."""

    async def _roundtrip(
        self,
        writer_fn: Callable[[BytesWriter], object],
        reader_fn: Callable[[BytesReader], Awaitable[Any]],
    ) -> Any:
        w = BytesWriter()
        writer_fn(w)
        r = BytesReader(w.get_bytes())
        return await reader_fn(r)

    async def test_uint64_zero(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_uint64(0),
            lambda r: r.read_uint64(),
        )
        assert val == 0

    async def test_uint64_one(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_uint64(1),
            lambda r: r.read_uint64(),
        )
        assert val == 1

    async def test_uint64_max(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_uint64(2**64 - 1),
            lambda r: r.read_uint64(),
        )
        assert val == 2**64 - 1

    async def test_uint64_large(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_uint64(12345678901234567890),
            lambda r: r.read_uint64(),
        )
        assert val == 12345678901234567890

    async def test_uint64s_empty(self) -> None:
        w = BytesWriter()
        w.write_uint64s([])
        assert w.get_bytes() == b""

    async def test_uint64s_single(self) -> None:
        w = BytesWriter()
        w.write_uint64s([42])
        r = BytesReader(w.get_bytes())
        assert (await r.read_uint64()) == 42

    async def test_uint64s_multiple(self) -> None:
        w = BytesWriter()
        w.write_uint64s([1, 2, 3])
        r = BytesReader(w.get_bytes())
        for expected in [1, 2, 3]:
            assert (await r.read_uint64()) == expected

    async def test_optional_present(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_optional_uint64(42),
            lambda r: r.read_optional_uint64(),
        )
        assert val == 42

    async def test_optional_none(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_optional_uint64(None),
            lambda r: r.read_optional_uint64(),
        )
        assert val is None

    async def test_optional_zero(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_optional_uint64(0),
            lambda r: r.read_optional_uint64(),
        )
        assert val == 0

    async def test_string_empty(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string(""),
            lambda r: r.read_string(),
        )
        assert val == ""

    async def test_string_ascii(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string("hello world"),
            lambda r: r.read_string(),
        )
        assert val == "hello world"

    async def test_string_unicode(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string("héllo wörld 🔥"),
            lambda r: r.read_string(),
        )
        assert val == "héllo wörld 🔥"

    async def test_string_with_storepath(self) -> None:
        sp = StorePath("/nix/store/abc123-foo")
        val = await self._roundtrip(
            lambda w: w.write_string(sp),
            lambda r: r.read_string(StorePath),
        )
        assert val == sp
        assert isinstance(val, StorePath)

    async def test_bytes_empty(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_bytes(b""),
            lambda r: r.read_bytes(),
        )
        assert val == b""

    async def test_bytes_small(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_bytes(b"\x00\x01\x02"),
            lambda r: r.read_bytes(),
        )
        assert val == b"\x00\x01\x02"

    async def test_bytes_aligned(self) -> None:
        """8 bytes — no padding needed."""
        val = await self._roundtrip(
            lambda w: w.write_bytes(b"\x01" * 8),
            lambda r: r.read_bytes(),
        )
        assert val == b"\x01" * 8

    async def test_bytes_padded(self) -> None:
        """5 bytes + 3 padding = 8."""
        val = await self._roundtrip(
            lambda w: w.write_bytes(b"\x01" * 5),
            lambda r: r.read_bytes(),
        )
        assert val == b"\x01" * 5

    async def test_string_list_empty(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string_list([]),
            lambda r: r.read_string_list(),
        )
        assert val == []

    async def test_string_list_single(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string_list(["a"]),
            lambda r: r.read_string_list(),
        )
        assert val == ["a"]

    async def test_string_list_multiple(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string_list(["a", "b", "c"]),
            lambda r: r.read_string_list(),
        )
        assert val == ["a", "b", "c"]

    async def test_string_set_empty(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string_set(set()),
            lambda r: r.read_string_set(),
        )
        assert val == set()

    async def test_string_set_single(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string_set({"a"}),
            lambda r: r.read_string_set(),
        )
        assert val == {"a"}

    async def test_string_set_multiple(self) -> None:
        val = await self._roundtrip(
            lambda w: w.write_string_set({"a", "b", "c"}),
            lambda r: r.read_string_set(),
        )
        assert val == {"a", "b", "c"}

    async def test_string_set_with_storepath(self) -> None:
        paths = {StorePath("/nix/store/a-foo"), StorePath("/nix/store/b-bar")}
        val = await self._roundtrip(
            lambda w: w.write_string_set(paths),
            lambda r: r.read_string_set(StorePath),
        )
        assert val == paths
        assert all(isinstance(p, StorePath) for p in val)


class TestFramedRoundtrip:
    """Roundtrip tests for FramedWriter / FramedReader."""

    async def test_framed_single_chunk(self) -> None:
        w = BytesWriter()
        fw = FramedWriter(w)
        fw.write(b"hello")
        await fw.finalize()

        fr = FramedReader(BytesReader(w.get_bytes()))
        data = await fr.readexactly(5)
        assert data == b"hello"

    async def test_framed_multiple_chunks(self) -> None:
        w = BytesWriter()
        fw = FramedWriter(w)
        fw.write(b"aaa")
        fw.write(b"bbb")
        await fw.finalize()

        fr = FramedReader(BytesReader(w.get_bytes()))
        data = b""
        while len(data) < 6:
            data += await fr.readexactly(1)
        assert data == b"aaabbb"

    async def test_framed_empty(self) -> None:
        w = BytesWriter()
        fw = FramedWriter(w)
        await fw.finalize()

        fr = FramedReader(BytesReader(w.get_bytes()))
        with pytest.raises(EOFError):
            await fr.readexactly(1)

    async def test_framed_ensure_eof(self) -> None:
        w = BytesWriter()
        fw = FramedWriter(w)
        fw.write(b"data")
        await fw.finalize()

        fr = FramedReader(BytesReader(w.get_bytes()))
        await fr.readexactly(4)
        await fr.ensure_eof()
        assert fr.at_eof

    async def test_framed_ensure_eof_consumes_extra(self) -> None:
        """ensure_eof should consume trailing chunks even if we didn't read them."""
        w = BytesWriter()
        fw = FramedWriter(w)
        fw.write(b"part1")
        fw.write(b"part2")
        await fw.finalize()

        fr = FramedReader(BytesReader(w.get_bytes()))
        await fr.readexactly(5)  # read "part1" only
        await fr.ensure_eof()
        assert fr.at_eof


# ═════════════════════════════════════════════════════════════════════════════
