"""A binary cache, whatever carries its files: Nix's `BinaryCacheStore`.

The narinfo, the codecs and the operations live here. A subclass supplies
the transport: `HTTPBinaryCacheStore` (`http_binary_cache.py`) over
`aiohttp`, `LocalBinaryCacheStore` (`local_binary_cache.py`) over a
directory, as `HttpBinaryCacheStore` and `LocalBinaryCacheStore` sit on
`BinaryCacheStore` in Nix. This module imports no transport, so a daemon
whose only cache is a directory loads no `aiohttp` (issue #30).
"""

from __future__ import annotations

import bz2
import gzip
import lzma
import re
from abc import abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

import anyio
import structlog
import zstandard as zstd

from nix_daemon_protocol.store_dir import store_dir
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd_store.store_path import StorePath

from .. import metrics
from ..exceptions import CacheFault, OpNotImplementedError
from ..serde import (
    DrvOutput,
    IsValidPathRequest,
    IsValidPathResponse,
    NarFromPathResponse,
    QueryPathFromHashPartRequest,
    QueryPathFromHashPartResponse,
    QueryPathInfoRequest,
    QueryPathInfoResponse,
    QueryValidPathsRequest,
    QueryValidPathsResponse,
    Realisation,
)
from .base import Store

if TYPE_CHECKING:
    import io
    from collections.abc import AsyncGenerator

    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd_store.drv_parser import Derivation

    from ..config import HTTPBinaryCacheSpec, LocalBinaryCacheSpec
    from ..connection import ClientConn, Connection

log = structlog.get_logger(__name__)

_NARINFO_KEY_RE = re.compile(r"^([A-Za-z]+):\s*(.*)$")
NAR_PIECE = 1024 * 1024
"""The most one `read` of a codec returns."""


@dataclass(frozen=True)
class NarInfo:
    """A parsed `.narinfo` of a binary cache."""

    path: StorePath
    url: str
    compression: str
    file_hash: str
    file_size: int
    valid_path_info: ValidPathInfo

    @property
    def references(self) -> set[StorePath]:
        return {StorePath(str(path)) for path in self.valid_path_info.info.references}


class BinaryCacheStore(Store):
    """A read-only store that answers from `.narinfo` files and streams NARs."""

    def __init__(self, spec: HTTPBinaryCacheSpec | LocalBinaryCacheSpec) -> None:
        super().__init__(spec)
        self.max_fail_ratio = spec.max_fail_ratio
        self.cache_info: dict[str, str] = {}
        self._priority_given = "priority" in spec.model_fields_set
        self._recent_results: deque[bool] = deque(maxlen=spec.health_window)

    def _take_cache_info(self, info: dict[str, str]) -> None:
        """Keep *info*, and rank by its `Priority` unless the spec names one.

        `nix-cache-info` only sets the default priority; `?priority=` wins (`binary-cache-store.cc:67`).
        """
        self.cache_info = info
        if not self._priority_given:
            self.priority = float(info["Priority"])

    @abstractmethod
    async def _get_file(self, name: str) -> str | None:
        """The text of the cache's file *name*, `None` when it has none; `CacheFault` when the read fails."""

    async def _get_narinfo_raw(self, hash_part: str) -> str | None:
        """The text of `<hash_part>.narinfo`, or `None` on a miss or a fault."""
        try:
            text = await self._get_file(f"{hash_part}.narinfo")
        except CacheFault:
            metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="error").inc()
            log.debug("binary_cache_narinfo_failed", store_id=self.store_id, hash_part=hash_part, exc_info=True)
            self._record_result(False)
            return None
        if text is None:
            # A miss, and not a fault. Counted apart from an error because a
            # cache that answers 404 for everything and one that cannot be
            # reached look the same to a build and are a different thing to fix.
            metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="miss").inc()
            return None
        metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="hit").inc()
        self._record_result(True)
        return text

    async def query_realisation(
        self, drv_output: DrvOutput, drv_path: StorePath, *, by_path: bool
    ) -> Realisation | None:
        """The realisation the cache holds for one derivation output, under *drv_output*.

        `queryRealisationUncached` reads one `.doi` file, named as the local
        store's Nix names it: `realisations/<drvOutput>.doi` in Nix 2.34
        (`binary-cache-store.cc:515`), `build-trace-v2/<drv>/<output>.doi` in
        2.35 (`binary-cache-store.cc:667`), which *by_path* selects. Each
        version names its own prefix (`binary-cache-store.hh:95`, `:118`). A file
        that fails to read or parse raises `CacheFault`, as Nix's read throws.
        """
        name = (
            f"build-trace-v2/{drv_path.name}/{drv_output.output_name}.doi"
            if by_path
            else f"realisations/{drv_output}.doi"
        )
        text = await self._get_file(name)
        log.debug("binary_cache_realisation", store_id=self.store_id, file=name, found=text is not None)
        if text is None:
            return None
        try:
            parsed = Realisation.model_validate_json(text)
        except ValueError as ex:
            raise CacheFault(f"while parsing file '{name}' as a realisation for key '{drv_output}'") from ex
        if parsed.out_path is None:
            raise CacheFault(f"realisation '{name}' of '{self.store_id}' names no output path")
        return parsed.model_copy(update={"id": drv_output, "signatures": sorted(parsed.signatures)})

    @abstractmethod
    def stream_nar(self, narinfo: NarInfo) -> AsyncGenerator[bytes]:
        """The decompressed NAR that *narinfo* names; `NarGone` when the cache lost it."""

    async def create_conn(self) -> Connection:
        raise OpNotImplementedError(f"{type(self).__name__} does not use daemon wire connections")

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        raise OpNotImplementedError(f"{type(self).__name__} cannot call {type(request).__name__}")

    async def execute(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        if isinstance(request, IsValidPathRequest):
            return await self.is_valid_path(request, client=client, suppress_last=suppress_last)
        if isinstance(request, QueryPathInfoRequest):
            return await self.query_path_info(request, client=client, suppress_last=suppress_last)
        if isinstance(request, QueryPathFromHashPartRequest):
            return await self.query_path_from_hash_part(request, client=client, suppress_last=suppress_last)
        if isinstance(request, QueryValidPathsRequest):
            return await self.query_valid_paths(request, client=client, suppress_last=suppress_last)
        raise OpNotImplementedError(f"{type(self).__name__} does not support {type(request).__name__}")

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
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
        narinfo = await self.get_narinfo(StorePath(str(request.path)))
        return IsValidPathResponse.fast(valid=narinfo is not None)

    async def query_path_info(
        self, request: QueryPathInfoRequest, client: Any = None, suppress_last: bool = False
    ) -> QueryPathInfoResponse:
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
        narinfo = await self.get_narinfo_by_hash_part(request.path)
        if narinfo is None:
            return None
        return QueryPathFromHashPartResponse(value=StorePath(path=str(narinfo.path)))

    async def query_valid_paths(
        self, request: QueryValidPathsRequest, client: Any = None, suppress_last: bool = False
    ) -> QueryValidPathsResponse:
        """Each path's `.narinfo`, all at once.

        Over HTTPS a serial loop pays the full latency of every miss: a
        thousand-path closure at 50 ms a lookup is nearly a minute. The
        transport bounds the flight (`HTTPBinaryCacheStore`'s semaphore).
        """
        valid: set[StorePath] = set()

        async def check(path: StorePath) -> None:
            if await self.get_narinfo(StorePath(str(path))) is not None:
                valid.add(path)

        async with anyio.create_task_group() as tg:
            for path in request.paths:
                tg.start_soon(check, path)
        return QueryValidPathsResponse(paths=valid)

    async def nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> NarFromPathResponse:
        raise OpNotImplementedError(f"{type(self).__name__} streams NARs through stream_nar()")

    async def get_narinfo(self, path: StorePath) -> NarInfo | None:
        """The `.narinfo` of *path*, or `None` when the cache has another path under its hash."""
        return await self.get_narinfo_by_hash_part(path.hash_part(), expected_path=path)

    async def get_narinfo_by_hash_part(self, hash_part: str, expected_path: StorePath | None = None) -> NarInfo | None:
        fetched = await self.fetch_narinfo(hash_part)
        if fetched is None:
            return None
        _, info = fetched
        if expected_path is not None and info.path != expected_path:
            return None
        return info

    async def fetch_narinfo(self, hash_part: str) -> tuple[str, NarInfo] | None:
        """One `.narinfo` by hash, raw text and parsed answer together.

        The upstream race serves the winner's text with only the URL line
        rewritten, so it needs the bytes the parse would throw away, and
        the path the bytes name. One request serves both. Issue #85.
        """
        raw = await self._get_narinfo_raw(hash_part)
        if raw is None:
            return None
        info = parse_narinfo(raw, expected_path=None)
        if info is None:
            return None
        return raw, info

    def _record_result(self, success: bool) -> None:
        self._recent_results.append(success)


def parse_cache_info(text: str, priority: float) -> dict[str, str]:
    """`nix-cache-info` as a dict, with Nix's defaults for the keys it lacks."""
    result: dict[str, str] = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        result[key.strip()] = value.strip()
    return default_cache_info(priority) | result


def default_cache_info(priority: float) -> dict[str, str]:
    return {"WantMassQuery": "0", "Priority": str(int(priority)), "StoreDir": store_dir()}


class _Codec(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...

    def close(self) -> None: ...


def open_codec(compression: str, raw: io.BufferedReader) -> _Codec:
    """A blocking reader of the NAR inside *raw*, compressed with *compression*."""
    match compression:
        case "none":
            return raw
        case "bzip2":
            return bz2.BZ2File(raw)
        case "gzip":
            return gzip.GzipFile(fileobj=raw)
        case "xz":
            return lzma.LZMAFile(raw)
        case "zstd":
            # Nix writes independent frames (`ZstdMultiFrameCompressionSink`);
            # without `read_across_frames` the NAR ends at the first.
            return zstd.ZstdDecompressor().stream_reader(raw, read_across_frames=True)
        case _:
            raise RuntimeError(f"unsupported NAR compression: {compression}")


async def pull(codec: _Codec) -> AsyncGenerator[bytes]:
    """Each piece of *codec*, read on a worker thread, and close it after.

    **The codec pulls, as Nix's does** (`Source::drainInto`, 8 KiB): each
    `read` returns at most `NAR_PIECE`, whatever the ratio. Pushed a chunk at
    a time, 10 KB of xz came out as one 64 MiB string. The codecs release the
    GIL: four xz streams on four threads ran 3.8x faster than in turn
    (dynhetz, 2026-10-30).
    """
    try:
        while piece := await anyio.to_thread.run_sync(codec.read, NAR_PIECE):
            yield piece
    finally:
        codec.close()


def parse_narinfo(text: str, expected_path: StorePath | None) -> NarInfo | None:
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

    return NarInfo(
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
