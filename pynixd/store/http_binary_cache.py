"""A binary cache over HTTP: Nix's `HttpBinaryCacheStore`."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import aiohttp
import anyio
import structlog

from .. import metrics
from ..exceptions import NarGone
from .binary_cache import BinaryCacheStore, default_cache_info, open_codec, parse_cache_info, pull

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Buffer

    from ..config import HTTPBinaryCacheSpec
    from .binary_cache import NarInfo

log = structlog.get_logger(__name__)

_HTTP_CHUNK_SIZE = 1024 * 256


class HTTPBinaryCacheStore(BinaryCacheStore):
    """Read-only Store backed by the Nix HTTP binary cache protocol."""

    def __init__(self, spec: HTTPBinaryCacheSpec) -> None:
        super().__init__(spec)
        self.url = _normalise_base_url(spec.url)
        self.max_concurrent = spec.max_concurrent
        self.health_window = spec.health_window
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: Any = None

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
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._started = False

    def nar_url(self, narinfo: NarInfo) -> str:
        """The cache URL, a slash, and the narinfo's URL, as Nix's `getFile` builds it.

        `urljoin` followed an absolute or `//host` URL out of the cache.
        nixidae/nixidae#1008.
        """
        return f"{self.url.rstrip('/')}/{narinfo.url.lstrip('/')}"

    async def stream_nar(self, narinfo: NarInfo) -> AsyncGenerator[bytes]:
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
            if narinfo.compression == "none":
                async for chunk in response.content.iter_chunked(_HTTP_CHUNK_SIZE):
                    yield chunk
                return
            codec = open_codec(
                narinfo.compression, io.BufferedReader(_SocketReader(response.content), _HTTP_CHUNK_SIZE)
            )
            async for piece in pull(codec):
                yield piece

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
            return default_cache_info(self.priority)
        return parse_cache_info(text, self.priority)

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

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("HTTPBinaryCacheStore is not started")
        return self._session


class _SocketReader(io.RawIOBase):
    """A response body as a blocking file, for a codec on a worker thread."""

    def __init__(self, content: aiohttp.StreamReader) -> None:
        self._content = content

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Buffer) -> int:
        view = memoryview(buffer)
        data = anyio.from_thread.run(self._content.read, len(view))
        view[: len(data)] = data
        return len(data)


def _normalise_base_url(url: str) -> str:
    stripped = url.strip()
    if not stripped:
        raise ValueError("HTTP binary cache URL must not be empty")
    if not stripped.endswith("/"):
        return f"{stripped}/"
    return stripped
