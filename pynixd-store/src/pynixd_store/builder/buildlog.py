"""The builder's output as Nix's log lines: `BuildLog::operator()` (`build/build-log.cc:11`)."""

from __future__ import annotations

from collections import deque


class BuildLog:
    """Cuts the builder's bytes into lines.

    A `\\r` that a `\\n` does not follow moves the column back to the start, and
    later bytes overwrite the line from there, as a terminal shows a progress bar.
    `tail` keeps the last *max_tail_lines* lines, which a failure message shows.
    """

    def __init__(self, max_tail_lines: int = 0) -> None:
        self.tail: deque[str] = deque(maxlen=max_tail_lines)
        self._line = bytearray()
        self._pos = 0
        self._pending_cr = False

    def feed(self, data: bytes) -> list[str]:
        lines: list[str] = []
        if not self._pending_cr and b"\r" not in data:
            *whole, rest = data.split(b"\n")
            for part in whole:
                self._write(part)
                lines.append(self._take())
            self._write(rest)
            return lines
        for c in data:
            if self._pending_cr:
                self._pending_cr = False
                if c != 0x0A:
                    self._pos = 0
            if c == 0x0D:
                self._pending_cr = True
            elif c == 0x0A:
                lines.append(self._take())
            else:
                self._write(bytes((c,)))
        return lines

    def flush(self) -> list[str]:
        """The last line, which no newline ended (`BuildLog::flush`)."""
        return [self._take()] if self._line else []

    def _write(self, part: bytes) -> None:
        self._line[self._pos : self._pos + len(part)] = part
        self._pos += len(part)

    def _take(self) -> str:
        del self._line[self._pos :]
        line = self._line.decode(errors="surrogateescape")
        self._line.clear()
        self._pos = 0
        self.tail.append(line)
        return line
