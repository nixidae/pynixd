from __future__ import annotations

import gzip
import lzma
import os
import socket
import threading
from typing import TYPE_CHECKING

import anyio
import pytest
import zstandard as zstd
from aiohttp import web
from aiohttp.test_utils import TestServer

from nix_daemon_protocol.ids import StoreId
from pynixd.config import HTTPBinaryCacheSpec
from pynixd.serde import IsValidPathRequest, QueryPathInfoRequest, QueryValidPathsRequest
from pynixd.store.http_binary_cache import _NAR_PIECE, HTTPBinaryCacheStore, _SocketReader
from pynixd.store_path import StorePath

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Buffer


_PATH = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test"
_HASH = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_NAR = b"nar-bytes"


def _narinfo(compression: str = "none", url: str | None = None) -> str:
    nar_url = url or f"nar/{_HASH}.nar"
    return "\n".join(
        [
            f"StorePath: {_PATH}",
            f"URL: {nar_url}",
            f"Compression: {compression}",
            "NarHash: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            f"NarSize: {len(_NAR)}",
            f"References: {StorePath(_PATH).name}",
            "",
        ]
    )


def _free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


async def _serve(narinfo: str, nar: bytes = _NAR) -> AsyncIterator[str]:
    app = web.Application()

    async def cache_info(request: web.Request) -> web.Response:
        return web.Response(text="StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 30\n")

    async def get_narinfo(request: web.Request) -> web.Response:
        if request.match_info["hash"] != _HASH:
            return web.Response(status=404)
        return web.Response(text=narinfo, content_type="text/x-nix-narinfo")

    async def get_nar(request: web.Request) -> web.Response:
        return web.Response(body=nar)

    app.router.add_get("/nix-cache-info", cache_info)
    app.router.add_get("/{hash}.narinfo", get_narinfo)
    app.router.add_get("/nar/{filename}", get_nar)

    runner = web.AppRunner(app)
    await runner.setup()
    port = _free_port()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


async def test_http_binary_cache_queries_path_info() -> None:
    async for url in _serve(_narinfo()):
        store = HTTPBinaryCacheStore(
            HTTPBinaryCacheSpec(store_id=StoreId("cache"), url=url),
        )
        await store.start()
        try:
            valid = await store.execute(IsValidPathRequest(path=StorePath(path=_PATH)))
            assert valid.valid

            info = await store.execute(QueryPathInfoRequest(path=StorePath(path=_PATH)))
            assert info.valid
            assert info.info is not None
            assert info.info.nar_size == len(_NAR)
        finally:
            await store.close()


async def test_http_binary_cache_query_valid_paths() -> None:
    missing = "/nix/store/cccccccccccccccccccccccccccccccc-missing"
    async for url in _serve(_narinfo()):
        store = HTTPBinaryCacheStore(
            HTTPBinaryCacheSpec(store_id=StoreId("cache"), url=url),
        )
        await store.start()
        try:
            response = await store.execute(
                QueryValidPathsRequest(
                    paths={
                        StorePath(path=_PATH),
                        StorePath(path=missing),
                    },
                    substitute=0,
                )
            )
            assert response.paths == {StorePath(path=_PATH)}
        finally:
            await store.close()


async def test_http_binary_cache_404_does_not_mark_store_unhealthy() -> None:
    missing = "/nix/store/cccccccccccccccccccccccccccccccc-missing"
    async for url in _serve(_narinfo()):
        store = HTTPBinaryCacheStore(
            HTTPBinaryCacheSpec(store_id=StoreId("cache"), url=url, health_window=2),
        )
        await store.start()
        try:
            for _ in range(5):
                valid = await store.execute(IsValidPathRequest(path=StorePath(path=missing)))
                assert not valid.valid
            assert store.is_healthy
        finally:
            await store.close()


async def test_http_binary_cache_streams_decompressed_nar() -> None:
    compressed = gzip.compress(_NAR)
    async for url in _serve(_narinfo(compression="gzip", url=f"nar/{_HASH}.nar.gz"), nar=compressed):
        store = HTTPBinaryCacheStore(
            HTTPBinaryCacheSpec(store_id=StoreId("cache"), url=url),
        )
        await store.start()
        try:
            narinfo = await store.get_narinfo(StorePath(_PATH))
            assert narinfo is not None
            chunks = [chunk async for chunk in store.stream_nar(narinfo)]
            assert b"".join(chunks) == _NAR
        finally:
            await store.close()


async def test_http_binary_cache_decompresses_off_the_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """An xz NAR decompresses on a worker thread, so it cannot stall the loop."""
    threads: list[int] = []
    readinto = _SocketReader.readinto

    def recording(self: _SocketReader, buffer: Buffer) -> int:
        threads.append(threading.get_ident())
        return readinto(self, buffer)

    monkeypatch.setattr(_SocketReader, "readinto", recording)
    async for url in _serve(_narinfo(compression="xz", url=f"nar/{_HASH}.nar.xz"), nar=lzma.compress(_NAR)):
        store = HTTPBinaryCacheStore(
            HTTPBinaryCacheSpec(store_id=StoreId("cache"), url=url),
        )
        await store.start()
        try:
            narinfo = await store.get_narinfo(StorePath(_PATH))
            assert narinfo is not None
            chunks = [chunk async for chunk in store.stream_nar(narinfo)]
            assert b"".join(chunks) == _NAR
        finally:
            await store.close()

    assert threads
    assert threading.get_ident() not in threads


async def _stream(narinfo: str, nar: bytes) -> list[bytes]:
    async for url in _serve(narinfo, nar=nar):
        store = HTTPBinaryCacheStore(HTTPBinaryCacheSpec(store_id=StoreId("cache"), url=url))
        await store.start()
        try:
            info = await store.get_narinfo(StorePath(_PATH))
            assert info is not None
            return [chunk async for chunk in store.stream_nar(info)]
        finally:
            await store.close()
    raise AssertionError("the cache never served")


async def test_http_binary_cache_bounds_each_decompressed_piece() -> None:
    """64 MiB of zeros is 10 KB of xz; no piece holds more than `_NAR_PIECE`.

    Nix pulls decompressed output through an 8 KiB buffer (`serialise.cc`
    `Source::drainInto`), so a high ratio never lands in memory at once.
    """
    raw = bytes(64 * 2**20)
    chunks = await _stream(_narinfo(compression="xz", url=f"nar/{_HASH}.nar.xz"), lzma.compress(raw))
    assert sum(map(len, chunks)) == len(raw)
    assert max(map(len, chunks)) <= _NAR_PIECE


async def test_http_binary_cache_reads_every_zstd_frame() -> None:
    """Nix writes a zstd NAR as independent frames (`compression.cc`
    `ZstdMultiFrameCompressionSink`); every frame is part of the NAR."""
    first, second = b"first-frame " * 1000, b"second-frame " * 1000
    compressor = zstd.ZstdCompressor()
    frames = compressor.compress(first) + compressor.compress(second)
    chunks = await _stream(_narinfo(compression="zstd", url=f"nar/{_HASH}.nar.zst"), frames)
    assert b"".join(chunks) == first + second


async def test_http_binary_cache_stream_stops_when_the_consumer_does() -> None:
    """A consumer that stops mid-NAR is not held for the next network read."""
    raw = os.urandom(4 * 2**20)
    compressed = lzma.compress(raw, preset=0)
    stalled = anyio.Event()
    app = web.Application()

    async def get_narinfo(request: web.Request) -> web.Response:
        return web.Response(
            text=_narinfo(compression="xz", url=f"nar/{_HASH}.nar.xz"), content_type="text/x-nix-narinfo"
        )

    async def get_nar(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Length": str(len(compressed))})
        await response.prepare(request)
        await response.write(compressed[: len(compressed) // 2])
        await stalled.wait()
        return response

    app.router.add_get("/{hash}.narinfo", get_narinfo)
    app.router.add_get("/nar/{filename}", get_nar)
    async with TestServer(app) as server:
        store = HTTPBinaryCacheStore(HTTPBinaryCacheSpec(store_id=StoreId("cache"), url=str(server.make_url("/"))))
        await store.start()
        try:
            info = await store.get_narinfo(StorePath(_PATH))
            assert info is not None
            with anyio.fail_after(5):
                stream = store.stream_nar(info)
                assert await anext(stream)
                await stream.aclose()
        finally:
            stalled.set()
            await store.close()
