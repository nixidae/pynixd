"""The GitHub Actions cache service: Twirp over JSON for the entries, Azure Blob storage for their bytes.

Each call is `POST <ACTIONS_RESULTS_URL>twirp/github.actions.results.api.v1.CacheService/<method>`
with the job's runtime token, as `@actions/cache`'s `cacheTwirpClient.ts` sends it. An entry is
created once (`CreateCacheEntry`), written to the signed URL it returns, and sealed
(`FinalizeCacheEntryUpload`); after that it never changes. `GetCacheEntryDownloadURL` gives a
signed URL that reads it. The signed URLs carry their own credentials, so they are never logged.

The limits (GitHub docs, `actions/reference/limits`, read 2026-11-16): 200 entry uploads and 1500
entry downloads per minute per repository. A request past either answers 429 with `Retry-After`.
"""

from __future__ import annotations

import base64
import enum
import json
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final, NoReturn, Self
from urllib.parse import parse_qs, urlsplit

import aiohttp
import structlog

from ..exceptions import CacheFault

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping
    from pathlib import Path

log = structlog.get_logger(__name__)

_CAMEL_HUMP: Final = re.compile(r"[A-Z]")
_SERVICE: Final = "github.actions.results.api.v1.CacheService"
_BLOB_VERSION: Final = "2023-11-03"
"""The Azure Storage REST version each blob request names: block blobs of up to 4000 MiB a block."""

BLOCK_SIZE: Final = 64 * 1024 * 1024
"""One staged block. `@actions/cache` uploads 64 MiB blocks (`cache.ts:650`)."""

_RETRY_AFTER_DEFAULT: Final = 60.0
"""The wait of a 429 that names none: the limits are per minute."""

_URL_MARGIN: Final = 60.0
"""A signed URL is reused until this many seconds before its `se` expiry."""


class CacheMode(enum.StrEnum):
    """`ACTIONS_CACHE_MODE`, as `@actions/cache` reads it (`internal/config.ts`)."""

    READ_WRITE = "write"
    READ = "read"
    WRITE_ONLY = "write-only"
    NONE = "none"

    @classmethod
    def parse(cls, raw: str) -> CacheMode:
        """An unknown or empty mode reads and writes, as `isCacheReadable` and `isCacheWritable` take it."""
        try:
            return cls(raw.strip().lower())
        except ValueError:
            return cls.READ_WRITE

    @property
    def readable(self) -> bool:
        return self in {CacheMode.READ_WRITE, CacheMode.READ}

    @property
    def writable(self) -> bool:
        return self in {CacheMode.READ_WRITE, CacheMode.WRITE_ONLY}


@dataclass(frozen=True, slots=True)
class GhaCredentials:
    """What an action's environment gives a job for the cache, and no `run:` step sees."""

    results_url: str
    runtime_token: str = field(repr=False)
    mode: CacheMode = CacheMode.READ_WRITE

    @classmethod
    def from_environment(cls, env: Mapping[str, str]) -> Self | None:
        """The credentials in *env*, by the names the runner sets; `None` without the v2 service.

        GitHub retired the v1 service in 2025, and `@actions/cache` takes v2 when
        `ACTIONS_CACHE_SERVICE_V2` is set (`internal/config.ts`).
        """
        results_url = env.get("ACTIONS_RESULTS_URL", "")
        token = env.get("ACTIONS_RUNTIME_TOKEN", "")
        if not results_url or not token or not env.get("ACTIONS_CACHE_SERVICE_V2"):
            return None
        return cls(
            results_url=results_url, runtime_token=token, mode=CacheMode.parse(env.get("ACTIONS_CACHE_MODE", ""))
        )

    @classmethod
    def from_file(cls, path: Path) -> Self | None:
        """The credentials a JSON object of those variables holds; `None` when *path* is absent. Blocks."""
        try:
            raw = path.read_text()
        except FileNotFoundError:
            return None
        env = json.loads(raw)
        if not isinstance(env, dict):
            raise ValueError(f"{path}: not a JSON object")
        return cls.from_environment({str(k): str(v) for k, v in env.items()})


class RateLimited(CacheFault):
    """The service refused a request for its rate limit, and names when to ask again."""

    def __init__(self, message: str, retry_after: float) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class EntryRefused(CacheFault):
    """The service answered a request with `ok: false` and no reason pynixd can act on."""


@dataclass(frozen=True, slots=True)
class SignedUrl:
    """A signed blob URL and the monotonic time it stops being reused."""

    url: str = field(repr=False)
    usable_until: float


def signed_url(url: str, now: Callable[[], float] = time.monotonic) -> SignedUrl:
    """*url* with the time to stop using it, read from its `se` expiry; now when it names none."""
    expiry = parse_qs(urlsplit(url).query).get("se", [""])[0]
    try:
        left = datetime.fromisoformat(expiry).timestamp() - datetime.now(UTC).timestamp()
    except ValueError:
        left = 0.0
    return SignedUrl(url=url, usable_until=now() + max(0.0, left - _URL_MARGIN))


@dataclass
class GhaCacheApi:
    """The three calls of the cache service and the blob reads and writes, under one rate gate.

    MNC-DEVIATION (#1398): `api.rs:805-852` trips a breaker at the first 429 and leaves the cache
    off for the rest of the run. pynixd waits out the `Retry-After` the service names, then asks
    again: the limits are per minute, and a job outlasts many minutes. Reverse it if a waiting
    upload queue costs a job more than the paths it uploads save the next one.
    """

    credentials: GhaCredentials
    version: str
    session: aiohttp.ClientSession
    now: Callable[[], float] = time.monotonic
    paused_until: float = 0.0

    @property
    def paused(self) -> float:
        """Seconds left of the last `Retry-After`: no request leaves before then."""
        return max(0.0, self.paused_until - self.now())

    def _refuse_while_paused(self) -> None:
        if (left := self.paused) > 0:
            raise RateLimited(f"the GitHub Actions cache is rate limited for {left:.0f} s more", left)

    def _note_rate_limit(self, status: int, headers: Mapping[str, str], what: str) -> NoReturn:
        try:
            wait = float(headers.get("Retry-After", ""))
        except ValueError:
            wait = _RETRY_AFTER_DEFAULT
        self.paused_until = max(self.paused_until, self.now() + wait)
        log.warning("gha_cache_rate_limited", call=what, status=status, retry_after=wait)
        raise RateLimited(f"{what}: HTTP {status}, rate limited for {wait:.0f} s", wait)

    async def _twirp(self, method: str, body: dict[str, object]) -> dict[str, object]:
        self._refuse_while_paused()
        url = f"{self.credentials.results_url.rstrip('/')}/twirp/{_SERVICE}/{method}"
        headers = {"Authorization": f"Bearer {self.credentials.runtime_token}", "Content-Type": "application/json"}
        try:
            async with self.session.post(url, data=json.dumps(body), headers=headers) as response:
                text = await response.text()
                if response.status == 429:
                    self._note_rate_limit(response.status, response.headers, method)
                if response.status != 200:
                    reply = _json_object(text)
                    code = str(reply.get("code", ""))
                    # The service answers overload as a Twirp `resource_exhausted`, or as a
                    # bare 502 (`api.rs:831-840`).
                    if code == "resource_exhausted" or response.status == 502:
                        self._note_rate_limit(response.status, response.headers, method)
                    if response.status == 409 and code == "already_exists":
                        raise _AlreadyExists(method)
                    raise CacheFault(f"{method}: HTTP {response.status} {code}: {reply.get('msg', text[:200])}")
                return _json_object(text)
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            raise CacheFault(f"{method}: {e!r}") from e

    async def download_url(self, key: str) -> SignedUrl | None:
        """The signed URL of the entry *key*, or `None` when the scopes of this job hold none.

        The key is also the only restore key, and a prefix match of a longer key is not this entry.
        """
        reply = await self._twirp(
            "GetCacheEntryDownloadURL", {"key": key, "restoreKeys": [key], "version": self.version}
        )
        if not reply.get("ok") or reply.get("matched_key", key) != key:
            return None
        url = str(reply.get("signed_download_url", ""))
        if not url:
            raise CacheFault(f"GetCacheEntryDownloadURL for '{key}' answered no URL")
        return signed_url(url, self.now)

    async def create(self, key: str) -> str | None:
        """The signed upload URL of a new entry *key*; `None` when the service will not create it.

        GitHub's service answers a key that exists, finalized or not, with HTTP 409 `already_exists`
        (measured on a runner, 2026-11-16, `ci/gha_cache_probe.py`); `cache.ts:702-733` reads
        `ok: false` as the same. Either way another copy serves or none can, so the caller skips.
        """
        try:
            reply = await self._twirp("CreateCacheEntry", {"key": key, "version": self.version})
        except _AlreadyExists:
            return None
        if reply.get("ok"):
            url = str(reply.get("signed_upload_url", ""))
            if not url:
                raise CacheFault(f"CreateCacheEntry for '{key}' answered no URL")
            return url
        message = str(reply.get("message", ""))
        if message:
            raise EntryRefused(f"CreateCacheEntry for '{key}': {message}")
        return None

    async def finalize(self, key: str, size: int) -> None:
        reply = await self._twirp(
            "FinalizeCacheEntryUpload", {"key": key, "version": self.version, "sizeBytes": str(size)}
        )
        if not reply.get("ok"):
            raise EntryRefused(f"FinalizeCacheEntryUpload for '{key}': {reply.get('message') or 'not ok'}")

    async def put_blob(self, url: str, chunks: AsyncIterator[bytes]) -> int:
        """Write *chunks* as one block blob at the signed *url*, a staged block per `BLOCK_SIZE`; the size."""
        ids: list[str] = []
        size = 0
        pending = bytearray()

        async def stage() -> None:
            block_id = base64.b64encode(f"{len(ids):08d}".encode()).decode()
            await self._blob("PUT", url, {"comp": "block", "blockid": block_id}, bytes(pending))
            ids.append(block_id)
            pending.clear()

        async for chunk in chunks:
            pending += chunk
            size += len(chunk)
            while len(pending) >= BLOCK_SIZE:
                rest = pending[BLOCK_SIZE:]
                del pending[BLOCK_SIZE:]
                await stage()
                pending += rest
        if pending or not ids:
            await stage()
        listing = "".join(f"<Latest>{block_id}</Latest>" for block_id in ids)
        body = f'<?xml version="1.0" encoding="utf-8"?><BlockList>{listing}</BlockList>'
        await self._blob("PUT", url, {"comp": "blocklist"}, body.encode())
        return size

    async def _blob(self, method: str, url: str, params: Mapping[str, str], body: bytes) -> None:
        headers = {"x-ms-version": _BLOB_VERSION, "Content-Length": str(len(body))}
        try:
            async with self.session.request(method, url, params=params, data=body, headers=headers) as response:
                if response.status == 429 or response.status == 503:
                    self._note_rate_limit(response.status, response.headers, f"blob {params.get('comp')}")
                if response.status not in (200, 201):
                    text = await response.text()
                    raise CacheFault(f"blob {params.get('comp')}: HTTP {response.status}: {text[:200]}")
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            raise CacheFault(f"blob {params.get('comp')}: {e!r}") from e

    @asynccontextmanager
    async def open_range(self, url: SignedUrl, start: int, end: int) -> AsyncIterator[aiohttp.StreamReader]:
        """The body of the bytes *start* to *end*, both included, of the blob at *url*.

        Whether a ranged read of a signed URL counts as an entry download is unmeasured.
        """
        headers = {"x-ms-version": _BLOB_VERSION, "x-ms-range": f"bytes={start}-{end}"}
        try:
            async with self.session.get(url.url, headers=headers) as response:
                if response.status not in (200, 206):
                    raise CacheFault(f"blob read: HTTP {response.status}")
                yield response.content
        except (TimeoutError, aiohttp.ClientError, OSError) as e:
            raise CacheFault(f"blob read: {e!r}") from e


class _AlreadyExists(CacheFault):
    """HTTP 409 `already_exists`: the key and version hold an entry, finalized or not."""


def _json_object(text: str) -> dict[str, object]:
    """The reply's top-level fields under their proto names.

    GitHub's service answers `signed_upload_url` (`@actions/cache` `internal/shared/util.ts:65` masks that name); a proto3 JSON writer
    may also send the lowerCamelCase name. A parser takes both, so this one does too.
    """
    try:
        parsed: object = json.loads(text) if text else {}
    except ValueError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {_CAMEL_HUMP.sub(lambda m: f"_{m.group(0).lower()}", str(k)): v for k, v in parsed.items()}
