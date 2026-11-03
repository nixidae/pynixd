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

from ..exceptions import CacheFault, NarGone
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
            self._take_cache_info(default_cache_info(self.priority))
        else:
            self._take_cache_info(parse_cache_info(text, self.priority))
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

    async def _get_file(self, name: str) -> str | None:
        try:
            return await anyio.Path(self.file(name)).read_text()
        except FileNotFoundError:
            return None
        except OSError as ex:
            raise CacheFault(f"binary cache '{self.path}': {ex}") from ex


def _open(path: Path) -> io.BufferedReader:
    return io.BufferedReader(io.FileIO(path, "r"))
