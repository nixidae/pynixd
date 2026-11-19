"""Catch a file call on the event loop's thread, through Python's audit hooks.

A monkeypatch sees only the calls a test thought of. An audit hook sees every `open`,
`os.*` and `fcntl.*` call that raises an audit event, wherever it is in the code under test.
`os.write`, `os.read` and `os.close` raise none, so a test that needs them patches them too.

A hook cannot be removed: one is installed at import and does nothing while no
`LoopFileCalls` is active.
"""

from __future__ import annotations

import sys
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from types import TracebackType

_WATCHED_PREFIXES = ("open", "os.", "fcntl.", "shutil.")
"""`os.remove`, `os.mkdir`, `os.rename`, `os.scandir`, `fcntl.flock`, `shutil.copyfile`, ..."""

_active: list[LoopFileCalls] = []


@dataclass
class LoopFileCalls:
    """Records each watched audit event raised on the thread that entered it, which is the loop's."""

    thread: int = field(default_factory=threading.get_ident)
    calls: list[tuple[str, str]] = field(default_factory=list)

    def __enter__(self) -> Self:
        self.thread = threading.get_ident()
        _active.append(self)
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        _active.remove(self)


def _hook(event: str, args: tuple[object, ...]) -> None:
    if not _active or not event.startswith(_WATCHED_PREFIXES):
        return
    ident = threading.get_ident()
    for recorder in _active:
        if recorder.thread == ident:
            recorder.calls.append((event, str(args[0]) if args else ""))


sys.addaudithook(_hook)
