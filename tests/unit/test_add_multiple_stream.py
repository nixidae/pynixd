"""Op 44's client stream as the router reads it, on either route, and the two senders to the daemon.

The client's frames fall anywhere: one frame holds the end of a NAR and the
start of the next path's info. `_paths` yields each NAR `Bounded` and skips
what the reader left before it parses the next info.
"""

from __future__ import annotations

import contextlib
import struct
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol import FEATURE_PROVENANCE, FEATURE_VERSIONED_ADD_TO_STORE_MULTIPLE, PROTOCOL_VERSION
from nix_daemon_protocol.add_multiple_to_store import UNVERSIONED_PATH_INFO, path_info_version
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd.handlers.add_multiple_to_store import _paths
from pynixd.wire import BytesReader, BytesWriter, FramedReader, send_framed, send_raw
from pynixd_store.source import Pieces

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

A = "/nix/store/" + "a" * 32 + "-a"
B = "/nix/store/" + "b" * 32 + "-b"


def _info(path: str, size: int, provenance: str = "") -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path=path),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash="0" * 64),
            references=set(),
            registration_time=Time(ts=1),
            nar_size=size,
            ultimate=False,
            sigs=set(),
            ca=ContentAddress(value=""),
            provenance=provenance,
        ),
    )


def _framed(payload: bytes, frame: int) -> bytes:
    chunks = [payload[i : i + frame] for i in range(0, len(payload), frame)]
    return b"".join(struct.pack("<Q", len(c)) + c for c in chunks) + struct.pack("<Q", 0)


async def _stream() -> bytes:
    one, two = _info(A, 5), _info(B, 3)
    return struct.pack("<Q", 2) + await one.bytes_wire() + b"AAAAA" + await two.bytes_wire() + b"BBB"


@pytest.mark.parametrize("frame", [1, 7, 64, 4096])
async def test_each_nar_ends_before_the_next_info(frame: int) -> None:
    frames = FramedReader(BytesReader(_framed(await _stream(), frame)))
    count = await frames.read_uint64()
    out = BytesWriter()
    seen: list[str] = []
    async with contextlib.aclosing(_paths(frames, count, UNVERSIONED_PATH_INFO, frozenset())) as paths:
        async for info, nar in paths:
            seen.append(str(info.path))
            await send_raw(nar.read, out, info.info.nar_size)
    assert seen == [A, B]
    assert out.get_bytes() == b"AAAAABBB"
    assert await frames.read(1) == b""


async def test_a_nar_left_unread_is_skipped() -> None:
    frames = FramedReader(BytesReader(_framed(await _stream(), 7)))
    count = await frames.read_uint64()
    seen: list[str] = []
    async with contextlib.aclosing(_paths(frames, count, UNVERSIONED_PATH_INFO, frozenset())) as paths:
        async for info, _nar in paths:
            seen.append(str(info.path))
    assert seen == [A, B]


PROVENANCE = '{"type":"copied","from":"https://cache.example","next":null}'


@pytest.mark.parametrize(
    ("features", "kept"),
    [
        (frozenset({FEATURE_PROVENANCE, FEATURE_VERSIONED_ADD_TO_STORE_MULTIPLE}), PROVENANCE),
        # `daemon.cc:552`: without the second feature the stream is 1.16 with no feature, whatever the session names.
        (frozenset({FEATURE_PROVENANCE}), ""),
    ],
)
async def test_a_stream_carries_provenance_under_versioned_add_to_store_multiple(
    features: frozenset[str], kept: str
) -> None:
    version, inner = path_info_version(PROTOCOL_VERSION, features)
    payload = struct.pack("<Q", 1) + await _info(A, 2, PROVENANCE).bytes_wire(version, inner) + b"AA"
    frames = FramedReader(BytesReader(_framed(payload, 4096)))
    count = await frames.read_uint64()
    async with contextlib.aclosing(_paths(frames, count, version, inner)) as paths:
        found = [info.info.provenance async for info, _nar in paths]
    assert found == [kept]
    assert await frames.read(1) == b""


async def test_send_raw_refuses_a_short_source() -> None:
    async def pieces() -> AsyncGenerator[bytes]:
        yield b"ab"

    async with Pieces(pieces()) as source:
        with pytest.raises(EOFError, match="unsent"):
            await send_raw(source.read, BytesWriter(), 3)


async def test_send_framed_gathers_pieces_into_frames() -> None:
    async def pieces() -> AsyncGenerator[bytes]:
        for _ in range(10):
            yield b"x" * 3

    out = BytesWriter()
    sent: list[int] = []
    async with Pieces(pieces()) as source:
        await send_framed(source.read, out, chunk_size=8, on_bytes=sent.append)
    frames = FramedReader(BytesReader(out.get_bytes()))
    body = b""
    while chunk := await frames.next_chunk():
        assert len(chunk) <= 8
        body += chunk
    assert body == b"x" * 30
    assert sum(sent) == 30
    assert sent == [8, 8, 8, 6]
