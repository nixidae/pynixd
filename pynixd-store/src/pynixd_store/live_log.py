"""The log of a running build, as its clients receive it: one copy, off the Python heap.

Kept as `LogMessage` models, the 231,489 messages of one Chromium build were about 40% of the objects the garbage
collector walked, and a full collection stopped the event loop for 3 s (nixidae/nixidae#1632). Serialized bytes are
no objects to the collector. A log past `SPILL_BYTES` moves to a file-backed map of an unlinked temporary file, so a
build log of a gigabyte costs page cache that the kernel can write back, not resident memory.
"""

from __future__ import annotations

import bisect
import mmap
import tempfile
from typing import IO, TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

SPILL_BYTES: Final = 1 << 20
"""Most build logs stay below this, in memory, and open no file."""

PIECE_BYTES: Final = 256 << 10
"""About what one send to a client carries. A replay of a large log goes out in pieces of this size."""


class LiveLog:
    """Append-only serialized log messages, read back from any message boundary."""

    def __init__(self, spill_dir: Path | None = None) -> None:
        self._spill_dir = spill_dir
        self._memory = bytearray()
        self._file: IO[bytes] | None = None
        self._map: mmap.mmap | None = None
        self._size = 0
        self._marks: list[int] = []
        """Message starts about `PIECE_BYTES` apart, so a piece ends on a boundary without a parse."""
        self.closed = False

    @property
    def size(self) -> int:
        return self._size

    def append(self, message: bytes) -> None:
        """Add one or more whole messages."""
        if self.closed:
            raise RuntimeError("the log of this build is closed")
        start = self._size
        end = start + len(message)
        if self._map is None and end > SPILL_BYTES:
            self._spill(end)
        if self._map is None:
            self._memory += message
        else:
            if end > len(self._map):
                self._map.resize(max(end, 2 * len(self._map)))
            self._map[start:end] = message
        self._size = end
        if start - (self._marks[-1] if self._marks else 0) >= PIECE_BYTES:
            self._marks.append(start)

    def piece(self, offset: int) -> bytes:
        """The messages from *offset*, which is a message boundary, up to the next mark or the end."""
        index = bisect.bisect_right(self._marks, offset)
        end = self._marks[index] if index < len(self._marks) else self._size
        return bytes(self._view()[offset:end])

    def pieces(self) -> Iterator[bytes]:
        """The whole log, a piece at a time, for a reader that runs once the appends have ended."""
        offset = 0
        while offset < self._size:
            piece = self.piece(offset)
            offset += len(piece)
            yield piece

    def get_bytes(self) -> bytes:
        return bytes(self._view()[: self._size])

    def close(self) -> None:
        """Give the memory and the file back. Nothing reads the log after this."""
        self.closed = True
        self._memory = bytearray()
        self._marks = []
        if self._map is not None:
            self._map.close()
            self._map = None
        if self._file is not None:
            self._file.close()
            self._file = None

    def _view(self) -> bytearray | mmap.mmap:
        return self._memory if self._map is None else self._map

    def _spill(self, end: int) -> None:
        # Blocking file calls on the loop, once per build that passes `SPILL_BYTES`: an open, a truncate, a map.
        self._file = tempfile.TemporaryFile(dir=self._spill_dir)  # noqa: SIM115 -- closed in `close`
        capacity = max(2 * end, 2 * SPILL_BYTES)
        self._file.truncate(capacity)
        self._map = mmap.mmap(self._file.fileno(), capacity)
        self._map[: self._size] = self._memory
        self._memory = bytearray()
