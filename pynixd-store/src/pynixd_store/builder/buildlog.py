"""The builder's output as Nix's log lines: `BuildLog::operator()` (`build/build-log.cc:11`)."""

from __future__ import annotations

from collections import deque

from . import jsonlog

Output = str | jsonlog.Event
"""A log line, or what a `@nix` line asks for."""


class BuildLog:
    """Cuts the builder's bytes into lines.

    A `\\r` that a `\\n` does not follow moves the column back to the start, and
    later bytes overwrite the line from there, as a terminal shows a progress bar.
    `tail` keeps the last *max_tail_lines* lines, which a failure message shows.
    A `@nix` line the handler consumes is no line and stays out of the tail
    (`BuildLog::flushLine`, `build/build-log.cc:39`).
    """

    def __init__(self, max_tail_lines: int = 0) -> None:
        self.tail: deque[str] = deque(maxlen=max_tail_lines)
        self._line = bytearray()
        self._pos = 0
        self._pending_cr = False

    def feed(self, data: bytes) -> list[Output]:
        lines: list[Output] = []
        if not self._pending_cr and b"\r" not in data:
            *whole, rest = data.split(b"\n")
            for part in whole:
                self._write(part)
                lines.extend(self._take())
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
                lines.extend(self._take())
            else:
                self._write(bytes((c,)))
        return lines

    def flush(self) -> list[Output]:
        """The last line, which no newline ended (`BuildLog::flush`)."""
        return self._take() if self._line else []

    def _write(self, part: bytes) -> None:
        self._line[self._pos : self._pos + len(part)] = part
        self._pos += len(part)

    def _take(self) -> list[Output]:
        del self._line[self._pos :]
        line = self._line.decode(errors="surrogateescape")
        self._line.clear()
        self._pos = 0
        events, consumed = jsonlog.handle(line)
        if consumed:
            return list(events)
        self.tail.append(line)
        return [*events, line]
