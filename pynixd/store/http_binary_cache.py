"""A binary cache over HTTP: Nix's `HttpBinaryCacheStore`."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import aiohttp
import anyio
import structlog

from ..config import HTTPBinaryCacheSpec
from ..cooldown import Cooldown
from ..exceptions import CacheFault, CacheUnopened, NarGone, StoreDirMismatch
from .binary_cache import BinaryCacheStore, default_cache_info, open_codec, parse_cache_info, pull

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Buffer, Mapping

    from ..config import S3BinaryCacheSpec
    from .binary_cache import NarInfo

log = structlog.get_logger(__name__)

_HTTP_CHUNK_SIZE = 1024 * 256
# A stall, not a total: Nix bounds a transfer by `connect-timeout` and
# `stalled-download-timeout` (`filetransfer.hh:56`, `:66`), never its length.
# A total turns a slow narinfo read into "no substitute". nixidae/nixidae#1265.
_TRANSFER_TIMEOUT = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=60)


class HTTPBinaryCacheStore(BinaryCacheStore):
    """Read-only Store backed by the Nix HTTP binary cache protocol."""

    def __init__(self, spec: HTTPBinaryCacheSpec | S3BinaryCacheSpec, url: str | None = None) -> None:
        """*url* is the cache's base URL, and an `HTTPBinaryCacheSpec` gives its own."""
        super().__init__(spec)
        if url is None:
            if not isinstance(spec, HTTPBinaryCacheSpec):
                raise TypeError(f"{type(spec).__name__} names no URL of its own")
            url = spec.url
        self.url = _normalise_base_url(url)
        self.max_concurrent = spec.max_concurrent
        self.health_window = spec.health_window
        self.drop_unopened = spec.drop_unopened
        self.opening = Cooldown()
        self._opened = True
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
        try:
            info = await self._load_cache_info()
        except CacheUnopened as e:
            if self.drop_unopened:
                await self.close()
                raise
            # NIX-DEVIATION (#27): `getDefaultSubstituters` (`store-registration.cc:88`) drops a cache whose
            # `init` throws, and the next daemon worker opens it again, 30 s for a dead host.
            # `maybeDisable` (`http-binary-cache-store.cc:94`) waits 60 s, but only under `--fallback` and
            # only in one worker. pynixd keeps the configured cache out of every substituter list
            # (`opened`) while a `Cooldown` runs, opens it again after, and doubles the wait at each
            # failure. Measure how often a cache opens on the first try after its cooldown to reverse it.
            # nixidae/nixidae#1257.
            # structlog renders `exc_info` on a warning as a bare `True`, not as a traceback.
            log.warning(
                "http_cache_info_fetch_failed",
                store_id=self.store_id,
                url=self.url,
                error=str(e),
                retry_in=self.opening.failed(),
            )
            info = default_cache_info(self.priority, self.store_dir)
            self._opened = False
            if self._background.open:
                self._background.spawn(self._open_after_cooldown)
        self._take_cache_info(info)
        concurrency = self.max_concurrent
        if concurrency is None:
            concurrency = 100
        self._semaphore = anyio.Semaphore(concurrency)
        self._started = True

    @property
    def opened(self) -> bool:
        return self._opened

    async def _open_after_cooldown(self) -> None:
        while not self._opened:
            await anyio.sleep(self.opening.remaining)
            try:
                info = await self._load_cache_info()
            except CacheUnopened as e:
                log.warning(
                    "http_cache_info_fetch_failed",
                    store_id=self.store_id,
                    url=self.url,
                    error=str(e),
                    retry_in=self.opening.failed(),
                )
                continue
            except StoreDirMismatch:
                # Nix's refusal (`binary-cache-store.cc:58`) is final: the cache stays out.
                log.exception("http_cache_store_dir_mismatch", store_id=self.store_id, url=self.url)
                return
            self._take_cache_info(info)
            self.opening.succeeded()
            self._opened = True
            log.info("http_cache_opened", store_id=self.store_id, url=self.url)

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
            headers=await self._headers(nar_url),
            raise_for_status=False,
            timeout=_TRANSFER_TIMEOUT,
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
        """`nix-cache-info`, or `CacheUnopened` where `BinaryCacheStore::init` throws.

        A 404 throws too: `init` then writes the file (`binary-cache-store.cc:50`), and
        an upload to an HTTP cache fails as "does not appear to be a binary cache"
        (`http-binary-cache-store.cc:76`).
        """
        session = self._require_session()
        url = urljoin(self.url, "nix-cache-info")
        try:
            async with session.get(url, headers=await self._headers(url), raise_for_status=False) as response:
                if response.status == 404:
                    raise CacheUnopened(f"'{self.url}' does not appear to be a binary cache")
                if response.status != 200:
                    raise CacheUnopened(f"{url}: HTTP {response.status}")
                text = await response.text()
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            raise CacheUnopened(f"{url}: {e!r}") from e
        return parse_cache_info(text, self.priority, self.store_dir)

    async def _get_file(self, name: str) -> str | None:
        session = self._require_session()
        url = urljoin(self.url, name)
        async with self._semaphore:
            try:
                async with session.get(
                    url,
                    headers=await self._headers(url),
                    raise_for_status=False,
                    timeout=_TRANSFER_TIMEOUT,
                ) as response:
                    if response.status == 404:
                        return None
                    if response.status != 200:
                        raise CacheFault(f"{url}: HTTP {response.status}")
                    return await response.text()
            except (TimeoutError, aiohttp.ClientError, OSError) as ex:
                raise CacheFault(f"{url}: {ex!r}") from ex

    async def _headers(self, url: str) -> Mapping[str, str]:
        """The headers of a GET of *url*; a cache that signs its requests adds them here."""
        return {}

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
