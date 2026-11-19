"""A binary cache in the GitHub Actions cache: the store of magic-nix-cache, served by pynixd itself.

The reference is DeterminateSystems/magic-nix-cache at `12fb09b61266`. There a second daemon serves
the cache over localhost HTTP and a post-build hook feeds it. Here pynixd is the runner's daemon:
it reads the cache as a substituter and uploads what it builds, with no hook and no second process.
nixidae/nixidae#1377. Each deliberate difference carries an `MNC-DEVIATION (#1398)` marker.

One entry holds one store path: a head line, the narinfo, and the NAR compressed with zstd.
"""

from __future__ import annotations

import hashlib
import io
import json
import secrets
import tempfile
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import aiohttp
import anyio
import anyio.to_thread
import structlog
import zstandard as zstd

from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd_store.store_path import StorePath
from pynixd_store.utils import nix32_encode

from .. import metrics
from ..exceptions import CacheFault, NarGone
from ..serde import QueryPathInfoRequest
from .binary_cache import BinaryCacheStore, NarInfo, default_cache_info, open_codec, pull
from .daemon import DaemonStore
from .gha_cache_api import GhaCacheApi, GhaCredentials, RateLimited, SignedUrl
from .http_binary_cache import _SocketReader
from .local_db import LocalDBStore
from .nar_reader import NarReader

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Iterable

    from ..config import GhaCacheSpec
    from .base import Store

log = structlog.get_logger(__name__)

ENTRY_FORMAT: Final = "pynixd-gha-cache 1"
"""The head line's first words; a change to the entry layout changes them and the version."""

HEAD_READ: Final = 64 * 1024
"""The first read of an entry. A narinfo past it costs a second read."""

_COPY_PIECE: Final = 1024 * 1024


def cache_version(store: str, salt: str) -> str:
    """The `version` of every entry: what makes two entries of one key interchangeable.

    MNC-DEVIATION (#1398): `api.rs:296` hashes the crate's name and version, so each release of
    magic-nix-cache starts an empty cache. pynixd hashes the entry format and the store directory,
    and a release that keeps the format keeps the cache. Reverse it if an entry ever needs a
    pynixd version to read.
    """
    return hashlib.sha256(f"{ENTRY_FORMAT}\0{store}\0{salt}".encode()).hexdigest()


def entry_head(narinfo: str) -> bytes:
    """The head line and the narinfo, which the compressed NAR follows."""
    body = narinfo.encode()
    return f"{ENTRY_FORMAT} {len(body)}\n".encode() + body


def parse_head(data: bytes) -> tuple[int, int] | None:
    """The offset and length of the narinfo in an entry that starts with *data*; `None` for another format."""
    line, newline, _ = data.partition(b"\n")
    if not newline:
        return None
    words = line.decode(errors="replace").split(" ")
    if " ".join(words[:2]) != ENTRY_FORMAT or len(words) != 3 or not words[2].isdigit():
        return None
    return len(line) + 1, int(words[2])


def entry_narinfo(info: ValidPathInfo, file_hash: str, file_size: int) -> str:
    """The narinfo of an entry: Nix's, with the compressed NAR at `nar` in the same entry."""
    lines = info.to_narinfo().splitlines()
    out = []
    for line in lines:
        if line.startswith("URL: "):
            out.append("URL: nar")
        elif line.startswith("Compression: "):
            out.extend(["Compression: zstd", f"FileHash: sha256:{file_hash}", f"FileSize: {file_size}"])
        else:
            out.append(line)
    return "\n".join(out) + "\n"


@dataclass(frozen=True, slots=True)
class _Located:
    """Where an entry's NAR is: its signed URL and the span of the compressed bytes."""

    url: SignedUrl
    nar_start: int
    file_size: int


@dataclass
class UploadCounts:
    """What the upload queue did, for `pynixd gha-cache wait`."""

    uploaded: int = 0
    present: int = 0
    failed: int = 0
    queued: int = 0

    def as_json(self) -> str:
        return json.dumps(
            {"uploaded": self.uploaded, "present": self.present, "failed": self.failed, "queued": self.queued}
        )


class GhaCacheStore(BinaryCacheStore):
    """Substitutes from, and uploads built outputs to, the GitHub Actions cache of this job.

    MNC-DEVIATION (#1398): `main.rs:438` adds the cache as a substituter of priority 1, so Nix asks
    it first for every path, a cache.nixos.org path too, and each miss is an entry download of the
    1500 a minute. pynixd asks it only for a path every other substituter missed (`asked_last`):
    the cache holds only what pynixd built, and no other substituter has that. Reverse it if a
    path the cache holds is often found elsewhere first, slower.
    """

    def __init__(self, spec: GhaCacheSpec) -> None:
        super().__init__(spec)
        self.credentials_file = spec.credentials_file
        self.uploads = spec.uploads
        self._salt = spec.version_salt
        self._max_uploads = spec.max_concurrent_uploads
        self._api: GhaCacheApi | None = None
        self._session: aiohttp.ClientSession | None = None
        self._located: dict[str, _Located] = {}
        self._queue: deque[StorePath] = deque()
        self._seen: set[StorePath] = set()
        self._wake = anyio.Event()
        self._in_flight = 0
        self._idle = anyio.Event()
        self._idle.set()
        self.counts = UploadCounts()
        self.local: Store | None = None
        """The store whose built outputs are uploaded; the server sets it."""

    @property
    def asked_last(self) -> bool:
        return True

    @property
    def joins_every_client(self) -> bool:
        """MNC-DEVIATION (#1398): `main.rs:438` writes the cache into nix.conf's `extra-substituters`,
        and a client that sends its own `substituters` loses it. A Nix client sends nix.conf's list
        too, so here the cache joins every client's list; `use-substitutes = false` still leaves it
        out. Reverse it if a job needs to keep this cache out while it substitutes from others.
        """
        return True

    @property
    def opened(self) -> bool:
        return self._api is not None and self._api.credentials.mode.readable

    @property
    def writable(self) -> bool:
        return self._api is not None and self.uploads and self._api.credentials.mode.writable

    def human_readable_uri(self) -> str:
        return "gha-cache://"

    async def start(self, sync_paths: bool = True) -> None:
        if self._started:
            return
        try:
            credentials = await anyio.to_thread.run_sync(GhaCredentials.from_file, self.credentials_file)
        except (OSError, ValueError):
            log.exception("gha_cache_credentials_unreadable", path=str(self.credentials_file))
            credentials = None
        if credentials is None:
            log.warning("gha_cache_no_credentials", store_id=self.store_id, path=str(self.credentials_file))
        else:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=60)
            )
            self._api = GhaCacheApi(credentials, cache_version(self.store_dir, self._salt), self._session)
            log.info("gha_cache_opened", store_id=self.store_id, mode=str(credentials.mode), uploads=self.writable)
        info = default_cache_info(self.priority, self.store_dir) | {"WantMassQuery": "1"}
        self._take_cache_info(info)
        if self.writable and self._background.open:
            self._background.spawn(self._upload_loop)
        self._started = True

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._api = None
        self._started = False

    def _require_api(self) -> GhaCacheApi:
        if self._api is None:
            raise CacheFault(f"{self.store_id} has no GitHub Actions credentials")
        return self._api

    async def _get_file(self, name: str) -> str | None:
        hash_part, dot, kind = name.partition(".")
        if dot != "." or kind != "narinfo" or "/" in hash_part:
            # Realisations are not uploaded: a content-addressed output's path is in its narinfo.
            return None
        api = self._require_api()
        url = await api.download_url(hash_part)
        if url is None:
            self._located.pop(hash_part, None)
            return None
        async with api.open_range(url, 0, HEAD_READ - 1) as body:
            data = await body.read()
        head = parse_head(data)
        if head is None:
            raise CacheFault(f"entry '{hash_part}' of '{self.store_id}' is not a {ENTRY_FORMAT} entry")
        start, length = head
        if len(data) < start + length:
            async with api.open_range(url, len(data), start + length - 1) as body:
                data += await body.read()
        text = data[start : start + length].decode()
        size = next((int(line[10:]) for line in text.splitlines() if line.startswith("FileSize: ")), 0)
        self._located[hash_part] = _Located(url=url, nar_start=start + length, file_size=size)
        return text

    async def stream_nar(self, narinfo: NarInfo) -> AsyncGenerator[bytes]:
        api = self._require_api()
        hash_part = narinfo.path.hash_part()
        located = self._located.get(hash_part)
        if located is None or located.url.usable_until <= api.now():
            # The narinfo read again gives a fresh signed URL and the span.
            if await self._get_file(f"{hash_part}.narinfo") is None:
                raise NarGone(f"entry '{hash_part}' is no longer in the GitHub Actions cache")
            located = self._located[hash_part]
        end = located.nar_start + located.file_size - 1
        async with api.open_range(located.url, located.nar_start, end) as body:
            codec = open_codec("zstd", io.BufferedReader(_SocketReader(body), 256 * 1024))
            async for piece in pull(codec):
                yield piece

    def take_built_outputs(self, local: Store, paths: Iterable[StorePath]) -> None:
        self.local = local
        self.enqueue(paths)

    def enqueue(self, paths: Iterable[StorePath]) -> None:
        """Queue *paths* for upload; each path once a run.

        MNC-DEVIATION (#1398): `gha.rs:96-111` uploads the closure of every built path, and
        `api.rs:83-86` every path the store gained during the job, substituted ones too. Each
        costs an entry upload of the 200 a minute and space of the 10 GB, for paths cache.nixos.org
        or another substituter already serves. pynixd uploads only the outputs it built. Reverse
        it if jobs fetch many paths from a substituter that is slower than this cache.
        """
        if not self.writable:
            return
        for path in paths:
            if path in self._seen:
                continue
            self._seen.add(path)
            self._queue.append(path)
            self.counts.queued += 1
        if self._queue:
            if self._idle.is_set():
                self._idle = anyio.Event()
            self._wake.set()

    async def wait_for_uploads(self) -> UploadCounts:
        """Return once every queued upload ended, as a job does before its runner goes away."""
        await self._idle.wait()
        return self.counts

    async def _upload_loop(self) -> None:
        limit = anyio.Semaphore(self._max_uploads)
        async with anyio.create_task_group() as tg:
            while True:
                await self._wake.wait()
                self._wake = anyio.Event()
                while self._queue:
                    await limit.acquire()
                    path = self._queue.popleft()
                    self._in_flight += 1
                    tg.start_soon(self._upload_one, path, limit)

    async def _upload_one(self, path: StorePath, limit: anyio.Semaphore) -> None:
        try:
            while True:
                try:
                    await self._upload(path)
                    break
                except RateLimited as e:
                    await anyio.sleep(e.retry_after)
        except Exception:
            self.counts.failed += 1
            metrics.GHA_CACHE_UPLOADS.labels(store_id=self.store_id, result="failed").inc()
            log.warning("gha_cache_upload_failed", store_id=self.store_id, path=str(path), exc_info=True)
        finally:
            self.counts.queued -= 1
            self._in_flight -= 1
            limit.release()
            if not self._queue and self._in_flight == 0:
                self._idle.set()

    async def _upload(self, path: StorePath) -> None:
        api = self._require_api()
        local = self.local
        if local is None:
            raise RuntimeError(f"{self.store_id} uploads, and the server set no local store")
        hash_part = path.hash_part()
        if await api.download_url(hash_part) is not None:
            self.counts.present += 1
            metrics.GHA_CACHE_UPLOADS.labels(store_id=self.store_id, result="present").inc()
            log.debug("gha_cache_entry_present", store_id=self.store_id, path=str(path))
            return
        key, upload_url = await self._new_entry(api, hash_part)
        resp = await local.execute(QueryPathInfoRequest(path=path))
        if not resp.valid or resp.info is None:
            raise RuntimeError(f"built output {path} is not valid in {local.store_id}")
        info = ValidPathInfo(path=path, info=resp.info)
        spool = await anyio.to_thread.run_sync(
            lambda: tempfile.TemporaryFile(prefix="pynixd-gha-")  # noqa: SIM115 -- closed in a thread below
        )
        try:
            file_hash, file_size = await self._compress(local, info, spool)
            narinfo = entry_narinfo(info, file_hash, file_size)
            size = await api.put_blob(upload_url, _entry_chunks(entry_head(narinfo), spool))
        finally:
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(spool.close)
        await api.finalize(key, size)
        self.counts.uploaded += 1
        metrics.GHA_CACHE_UPLOADS.labels(store_id=self.store_id, result="uploaded").inc()
        log.info("gha_cache_uploaded", store_id=self.store_id, path=str(path), nar_size=info.info.nar_size, size=size)

    @staticmethod
    async def _new_entry(api: GhaCacheApi, hash_part: str) -> tuple[str, str]:
        """A new entry `<hash part>-<random>`: its key and signed upload URL.

        A key the service has seen stays taken, finalized or not: an entry a dead job left
        unfinished still answered 409 and a miss 3 h later (`ci/gha_cache_probe.py`, 2026-11-16).
        With the bare hash part as key, that path could never be cached again. `api.rs:352-372`
        adds the same random suffix; lookups match it by prefix (`GhaCacheApi.download_url`).

        MNC-DEVIATION (#1398): `gha.rs:162-196` creates an entry for every path with no lookup
        first, so each job uploads again what an earlier job cached. pynixd looks up first: a
        lookup costs one of the 1500 a minute, a creation one of the 200. Reverse it if lookups
        become the limit.
        """
        for _ in range(5):
            key = f"{hash_part}-{secrets.token_hex(4)}"
            if (url := await api.create(key)) is not None:
                return key, url
        raise CacheFault(f"five random keys for '{hash_part}' were all taken")

    async def _compress(self, local: Store, info: ValidPathInfo, spool: io.BufferedRandom) -> tuple[str, int]:
        """The NAR of *info* from *local*, zstd-compressed into *spool*; the hash and size of what it wrote.

        Compressed to a file first: the narinfo, which precedes the NAR in the entry, names its size.
        """
        if not isinstance(local, (DaemonStore, LocalDBStore)):
            raise TypeError(f"{local.store_id} serves no NARs to upload")
        digest = hashlib.sha256()
        compressor = zstd.ZstdCompressor(level=3).compressobj()

        def write(data: bytes) -> None:
            spool.write(data)
            digest.update(data)

        def feed(chunk: bytes) -> None:
            write(compressor.compress(chunk))

        async with NarReader(local) as nars:
            async for chunk in nars.read(info.path, info.info.nar_size):
                await anyio.to_thread.run_sync(feed, chunk)

        def finish() -> int:
            write(compressor.flush())
            size = spool.tell()
            # A seek flushes the buffered writes: a disk write.
            spool.seek(0)
            return size

        size = await anyio.to_thread.run_sync(finish)
        return nix32_encode(digest.digest()), size


async def _entry_chunks(head: bytes, spool: io.BufferedRandom) -> AsyncIterator[bytes]:
    yield head
    while piece := await anyio.to_thread.run_sync(spool.read, _COPY_PIECE):
        yield piece
