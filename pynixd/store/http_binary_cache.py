"""HTTP binary cache store implementation."""

from __future__ import annotations

import bz2
import lzma
import re
import zlib
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol
from urllib.parse import urljoin

import aiohttp
import anyio
import structlog
import zstandard as zstd

from nix_daemon_protocol.store_dir import store_dir
from nix_daemon_protocol.valid_path_info import ValidPathInfo

from .. import metrics
from ..exceptions import NarGone, OpNotImplementedError
from ..serde import (
    IsValidPathRequest,
    IsValidPathResponse,
    NarFromPathResponse,
    QueryPathFromHashPartRequest,
    QueryPathFromHashPartResponse,
    QueryPathInfoRequest,
    QueryPathInfoResponse,
    QueryValidPathsRequest,
    QueryValidPathsResponse,
)
from ..store_path import StorePath
from .base import Store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from nix_daemon_protocol.wire_ops import WireRequest

    from ..config import HTTPBinaryCacheSpec
    from ..connection import ClientConn, Connection
    from ..drv_parser import Derivation

log = structlog.get_logger(__name__)

_NARINFO_KEY_RE = re.compile(r"^([A-Za-z]+):\s*(.*)$")
_HTTP_CHUNK_SIZE = 1024 * 256


@dataclass(frozen=True)
class HTTPNarInfo:
    """Parsed .narinfo metadata from an HTTP binary cache."""

    path: StorePath
    url: str
    compression: str
    file_hash: str
    file_size: int
    valid_path_info: ValidPathInfo

    @property
    def references(self) -> set[StorePath]:
        return {StorePath(str(path)) for path in self.valid_path_info.info.references}


class HTTPBinaryCacheStore(Store):
    """Read-only Store backed by the Nix HTTP binary cache protocol."""

    def __init__(self, spec: HTTPBinaryCacheSpec) -> None:
        """Configure HTTP binary cache from spec, normalising the base URL."""
        super().__init__(spec)
        self.url = _normalise_base_url(spec.url)
        self.trusted = spec.trusted
        self.max_concurrent = spec.max_concurrent
        self.max_fail_ratio = spec.max_fail_ratio
        self.health_window = spec.health_window
        self.cache_info: dict[str, str] = {}
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: Any = None
        self._recent_results: deque[bool] = deque(maxlen=spec.health_window)

    async def start(self, sync_paths: bool = True) -> None:
        """Initialise HTTP session, load nix-cache-info, and configure concurrency."""
        if self._started:
            return
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100, force_close=False),
            timeout=aiohttp.ClientTimeout(total=30),
        )
        self.cache_info = await self._load_cache_info()
        concurrency = self.max_concurrent
        if concurrency is None:
            concurrency = 100
        self._semaphore = anyio.Semaphore(concurrency)
        self._started = True

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._started = False

    async def create_conn(self) -> Connection:
        """Not supported — HTTP binary caches do not use wire connections."""
        raise OpNotImplementedError("HTTPBinaryCacheStore does not use daemon wire connections")

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        """Not supported — HTTP binary caches cannot forward arbitrary wire requests."""
        raise OpNotImplementedError(f"HTTPBinaryCacheStore cannot call {type(request).__name__}")

    async def execute(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        """Execute a supported operation via the HTTP cache protocol."""

        if isinstance(request, IsValidPathRequest):
            return await self.is_valid_path(request, client=client, suppress_last=suppress_last)
        if isinstance(request, QueryPathInfoRequest):
            return await self.query_path_info(request, client=client, suppress_last=suppress_last)
        if isinstance(request, QueryPathFromHashPartRequest):
            return await self.query_path_from_hash_part(request, client=client, suppress_last=suppress_last)
        if isinstance(request, QueryValidPathsRequest):
            return await self.query_valid_paths(request, client=client, suppress_last=suppress_last)
        raise OpNotImplementedError(f"HTTPBinaryCacheStore does not support {type(request).__name__}")

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        """Not supported — HTTP binary caches do not provide derivation parsing."""
        return None

    @property
    def is_healthy(self) -> bool:
        """Whether the recent failure rate is within the configured max_fail_ratio."""
        if not self._recent_results:
            return True
        failures = sum(1 for result in self._recent_results if not result)
        return failures / len(self._recent_results) <= self.max_fail_ratio

    async def is_valid_path(
        self, request: IsValidPathRequest, client: Any = None, suppress_last: bool = False
    ) -> IsValidPathResponse:
        """IsValidPath — check existence via .narinfo lookup."""
        narinfo = await self.get_narinfo(StorePath(str(request.path)))
        return IsValidPathResponse.fast(valid=narinfo is not None)

    async def query_path_info(
        self, request: QueryPathInfoRequest, client: Any = None, suppress_last: bool = False
    ) -> QueryPathInfoResponse:
        """QueryPathInfo — fetch path info from cache or remote .narinfo."""

        cached = self.get_path_info(request.path)
        if cached is not None:
            return QueryPathInfoResponse.fast(valid=True, info=cached.info)

        narinfo = await self.get_narinfo(StorePath(str(request.path)))
        if narinfo is None:
            return QueryPathInfoResponse.fast(valid=False)
        self.add_path_info(narinfo.valid_path_info)
        return QueryPathInfoResponse.fast(valid=True, info=narinfo.valid_path_info.info)

    async def query_path_from_hash_part(
        self, request: QueryPathFromHashPartRequest, client: Any = None, suppress_last: bool = False
    ) -> QueryPathFromHashPartResponse | None:
        """QueryPathFromHashPart — resolve hash prefix via .narinfo lookup."""

        narinfo = await self.get_narinfo_by_hash_part(request.path)
        if narinfo is None:
            return None
        return QueryPathFromHashPartResponse(value=StorePath(path=str(narinfo.path)))

    async def query_valid_paths(
        self, request: QueryValidPathsRequest, client: Any = None, suppress_last: bool = False
    ) -> QueryValidPathsResponse:
        """QueryValidPaths — check each path via .narinfo, concurrently.

        One HTTPS round trip per path, so a serial loop pays the full
        latency of every miss: a thousand-path closure at 50 ms a lookup is
        nearly a minute of waiting. The semaphore in `_get_narinfo_raw`
        bounds the flight, so fanning out here takes every slot the operator
        allowed instead of one. Issue #54: asking substituters whether a NAR
        exists is the read-only half pynixd keeps; the importing stays with
        the daemon.
        """
        valid: set[StorePath] = set()
        lock = anyio.Lock()

        async def check(path: StorePath) -> None:
            if await self.get_narinfo(StorePath(str(path))) is not None:
                async with lock:
                    valid.add(path)

        async with anyio.create_task_group() as tg:
            for path in request.paths:
                tg.start_soon(check, path)
        return QueryValidPathsResponse(paths=valid)

    async def nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> NarFromPathResponse:
        """Not supported directly — use stream_nar() for NAR streaming."""
        raise OpNotImplementedError("HTTPBinaryCacheStore streams NARs through stream_nar()")

    async def get_narinfo(self, path: StorePath) -> HTTPNarInfo | None:
        """Fetch .narinfo for a store path, consulting the path-info cache first."""

        cached = self.get_path_info(path)
        hash_part = path.hash_part()
        if cached is not None:
            raw = await self._get_narinfo_raw(hash_part)
            if raw is None:
                return None
            return _parse_narinfo(raw, expected_path=path)
        return await self.get_narinfo_by_hash_part(hash_part, expected_path=path)

    async def get_narinfo_by_hash_part(
        self, hash_part: str, expected_path: StorePath | None = None
    ) -> HTTPNarInfo | None:
        """Fetch .narinfo by hash prefix, optionally verifying the expected path."""

        fetched = await self.fetch_narinfo(hash_part)
        if fetched is None:
            return None
        _, info = fetched
        if expected_path is not None and info.path != expected_path:
            return None
        return info

    async def fetch_narinfo(self, hash_part: str) -> tuple[str, HTTPNarInfo] | None:
        """Fetch one `.narinfo` by hash, raw text and parsed answer together.

        The upstream race serves the winner's text with only the URL line
        rewritten, so it needs the bytes the parse would throw away, and
        the path the bytes name. One request serves both. Issue #85.
        """
        raw = await self._get_narinfo_raw(hash_part)
        if raw is None:
            return None
        info = _parse_narinfo(raw, expected_path=None)
        if info is None:
            return None
        return raw, info

    def nar_url(self, narinfo: HTTPNarInfo) -> str:
        """The cache URL, a slash, and the narinfo's URL, as Nix's `getFile` builds it.

        `urljoin` followed an absolute or `//host` URL out of the cache.
        nixidae/nixidae#1008.
        """
        return f"{self.url.rstrip('/')}/{narinfo.url.lstrip('/')}"

    async def stream_nar(self, narinfo: HTTPNarInfo) -> AsyncIterator[bytes]:
        """Stream a decompressed NAR from the cache as an async byte iterator."""

        session = self._require_session()
        nar_url = self.nar_url(narinfo)
        async with session.get(
            nar_url,
            raise_for_status=False,
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=60),
        ) as response:
            if response.status in (403, 404):
                raise NarGone(f"HTTP cache returned {response.status} for {nar_url}")
            if response.status != 200:
                raise RuntimeError(f"HTTP cache returned {response.status} for {nar_url}")
            decompressor = _decompressor(narinfo.compression)
            # `lzma`, `bz2`, `zlib` and `zstandard` release the GIL while they
            # decompress: four xz streams on four threads ran 3.8x faster than
            # one after the other (dynhetz, 2026-10-30). On the loop, one xz
            # NAR stalls every client of the daemon.
            passthrough = isinstance(decompressor, _PassthroughDecompressor)
            async for chunk in response.content.iter_chunked(_HTTP_CHUNK_SIZE):
                outs = (
                    decompressor.decompress(chunk)
                    if passthrough
                    else await anyio.to_thread.run_sync(decompressor.decompress, chunk)
                )
                for out in outs:
                    if out:
                        yield out
            for out in decompressor.finish():
                if out:
                    yield out

    async def _load_cache_info(self) -> dict[str, str]:
        def _check_response(status: int) -> None:
            if status != 200:
                raise RuntimeError(f"HTTP cache returned {status}")

        try:
            session = self._require_session()
            async with session.get(urljoin(self.url, "nix-cache-info"), raise_for_status=False) as response:
                _check_response(response.status)
                text = await response.text()
        except (TimeoutError, aiohttp.ClientError, OSError, RuntimeError):
            log.warning("http_cache_info_fetch_failed", store_id=self.store_id, url=self.url, exc_info=True)
            return {"WantMassQuery": "0", "Priority": str(int(self.priority)), "StoreDir": store_dir()}

        result: dict[str, str] = {}
        for line in text.splitlines():
            if ":" not in line:
                continue
            key, _, value = line.partition(":")
            result[key.strip()] = value.strip()
        result.setdefault("WantMassQuery", "0")
        result.setdefault("Priority", str(int(self.priority)))
        result.setdefault("StoreDir", store_dir())
        return result

    async def _get_narinfo_raw(self, hash_part: str) -> str | None:
        session = self._require_session()
        url = urljoin(self.url, f"{hash_part}.narinfo")
        async with self._semaphore:
            try:
                async with session.get(url, raise_for_status=False, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 404:
                        # A miss, and not a fault. Counted apart from an error
                        # because a cache that answers 404 for everything and
                        # one that cannot be reached look the same to a build
                        # and are a different thing to fix.
                        metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="miss").inc()
                        return None
                    if response.status != 200:
                        metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="error").inc()
                        log.debug("http_cache_narinfo_status", store_id=self.store_id, url=url, status=response.status)
                        self._record_result(False)
                        return None
                    text = await response.text()
            except (TimeoutError, aiohttp.ClientError, OSError):
                metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="error").inc()
                log.debug("http_cache_narinfo_failed", store_id=self.store_id, url=url, exc_info=True)
                self._record_result(False)
                return None

        metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="hit").inc()
        self._record_result(True)
        return text

    def _record_result(self, success: bool) -> None:
        self._recent_results.append(success)

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("HTTPBinaryCacheStore is not started")
        return self._session


class _PassthroughDecompressor:
    def decompress(self, chunk: bytes) -> list[bytes]:
        return [chunk]

    def finish(self) -> list[bytes]:
        return []


class _ZlibDecompressor:
    def __init__(self) -> None:
        self._decompressor = zlib.decompressobj()

    def decompress(self, chunk: bytes) -> list[bytes]:
        return [self._decompressor.decompress(chunk)]

    def finish(self) -> list[bytes]:
        return [self._decompressor.flush()]


class _GzipDecompressor:
    def __init__(self) -> None:
        self._decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)

    def decompress(self, chunk: bytes) -> list[bytes]:
        return [self._decompressor.decompress(chunk)]

    def finish(self) -> list[bytes]:
        return [self._decompressor.flush()]


class _Bzip2Decompressor:
    def __init__(self) -> None:
        self._decompressor = bz2.BZ2Decompressor()

    def decompress(self, chunk: bytes) -> list[bytes]:
        return [self._decompressor.decompress(chunk)]

    def finish(self) -> list[bytes]:
        return []


class _XzDecompressor:
    def __init__(self) -> None:
        self._decompressor = lzma.LZMADecompressor()

    def decompress(self, chunk: bytes) -> list[bytes]:
        return [self._decompressor.decompress(chunk)]

    def finish(self) -> list[bytes]:
        return []


class _ZstdDecompressor:
    def __init__(self) -> None:
        self._decompressor = zstd.ZstdDecompressor().decompressobj()

    def decompress(self, chunk: bytes) -> list[bytes]:
        return [self._decompressor.decompress(chunk)]

    def finish(self) -> list[bytes]:
        return [self._decompressor.flush()]


class _Decompressor(Protocol):
    def decompress(self, chunk: bytes) -> list[bytes]: ...

    def finish(self) -> list[bytes]: ...


def _decompressor(compression: str) -> _Decompressor:
    match compression:
        case "" | "none":
            return _PassthroughDecompressor()
        case "bzip2":
            return _Bzip2Decompressor()
        case "gzip":
            return _GzipDecompressor()
        case "xz":
            return _XzDecompressor()
        case "zlib":
            return _ZlibDecompressor()
        case "zstd":
            return _ZstdDecompressor()
        case _:
            raise RuntimeError(f"unsupported NAR compression: {compression}")


def _parse_narinfo(text: str, expected_path: StorePath | None) -> HTTPNarInfo | None:
    fields: dict[str, str] = {}
    for line in text.splitlines():
        match = _NARINFO_KEY_RE.match(line)
        if match is None:
            continue
        key = match.group(1)
        if key == "Sig":
            continue
        fields[key] = match.group(2)

    path_raw = fields.get("StorePath")
    url = fields.get("URL")
    if not path_raw or not url:
        return None
    # Nix calls such a narinfo corrupt (`nar-info.cc:97-107`); it names no
    # NAR that could be checked.
    if not fields.get("NarHash") or _parse_int(fields.get("NarSize")) == 0:
        log.warning(
            "narinfo_corrupt", store_path=path_raw, nar_hash=fields.get("NarHash"), nar_size=fields.get("NarSize")
        )
        return None

    path = StorePath(path_raw)
    if expected_path is not None and path != expected_path:
        return None

    valid_path_info = ValidPathInfo.from_narinfo(text)
    if not str(valid_path_info.path):
        return None

    return HTTPNarInfo(
        path=path,
        url=url,
        # Absent or empty means bzip2 (`nar-info.cc:58`, `:122`), not none.
        compression=fields.get("Compression") or "bzip2",
        file_hash=fields.get("FileHash", ""),
        file_size=_parse_int(fields.get("FileSize")),
        valid_path_info=valid_path_info,
    )


def _parse_int(raw: str | None) -> int:
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def _normalise_base_url(url: str) -> str:
    stripped = url.strip()
    if not stripped:
        raise ValueError("HTTP binary cache URL must not be empty")
    if not stripped.endswith("/"):
        return f"{stripped}/"
    return stripped
