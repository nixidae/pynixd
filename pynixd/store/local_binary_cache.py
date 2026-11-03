"""A binary cache in a directory: Nix's `LocalBinaryCacheStore`, `file://`.

`getFile` reads `<dir>/<name>`, and a missing file is a miss
(`local-binary-cache-store.cc`). pynixd only reads: Nix writes a
`nix-cache-info` into a cache that lacks one, and pynixd answers with the
same defaults instead.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import anyio
import structlog

from .. import metrics
from ..exceptions import NarGone
from .binary_cache import BinaryCacheStore, default_cache_info, open_codec, parse_cache_info, pull

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from ..config import LocalBinaryCacheSpec
    from .binary_cache import NarInfo

log = structlog.get_logger(__name__)


class LocalBinaryCacheStore(BinaryCacheStore):
    """Read-only Store over a directory in Nix's binary cache layout."""

    def __init__(self, spec: LocalBinaryCacheSpec) -> None:
        super().__init__(spec)
        self.path: Path = spec.path

    async def start(self, sync_paths: bool = True) -> None:
        if self._started:
            return
        try:
            text = await anyio.Path(self.path / "nix-cache-info").read_text()
        except FileNotFoundError:
            self.cache_info = default_cache_info(self.priority)
        else:
            self.cache_info = parse_cache_info(text, self.priority)
        self._started = True

    async def close(self) -> None:
        self._started = False

    def file(self, name: str) -> Path:
        """`<dir>/<name>`, as `getFile` joins them."""
        return self.path / name.lstrip("/")

    async def stream_nar(self, narinfo: NarInfo) -> AsyncGenerator[bytes]:
        nar = self.file(narinfo.url)
        try:
            handle = await anyio.to_thread.run_sync(_open, nar)
        except FileNotFoundError as exc:
            raise NarGone(f"binary cache '{self.path}' has no file '{narinfo.url}'") from exc
        async for piece in pull(open_codec(narinfo.compression, handle)):
            yield piece

    async def _get_narinfo_raw(self, hash_part: str) -> str | None:
        try:
            text = await anyio.Path(self.file(f"{hash_part}.narinfo")).read_text()
        except FileNotFoundError:
            metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="miss").inc()
            return None
        except OSError:
            metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="error").inc()
            log.debug("local_cache_narinfo_failed", store_id=self.store_id, path=str(self.path), exc_info=True)
            self._record_result(False)
            return None
        metrics.BINARY_CACHE_NARINFO.labels(store_id=self.store_id, result="hit").inc()
        self._record_result(True)
        return text


def _open(path: Path) -> io.BufferedReader:
    return io.BufferedReader(io.FileIO(path, "r"))
