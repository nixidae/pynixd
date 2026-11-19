"""Catch a file call on the event loop's thread, through Python's audit hooks.

A monkeypatch sees only the calls a test thought of. An audit hook sees every `open`,
`os.*` and `fcntl.*` call that raises an audit event, wherever it is in the code under test.
`os.stat`, `os.read`, `os.write`, `os.close` and others raise none; `watch_unaudited` makes them
raise one, and until it runs a test that needs them patches them itself.

A hook cannot be removed: one is installed at import and does nothing while no
`LoopFileCalls` is active and no session sink is set.
"""

from __future__ import annotations

import os
import sys
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Self

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

_WATCHED_PREFIXES = ("open", "os.", "fcntl.", "shutil.")
"""`os.remove`, `os.mkdir`, `os.rename`, `os.scandir`, `fcntl.flock`, `shutil.copyfile`, ..."""

UNAUDITED: Final = (
    "stat",
    "lstat",
    "fstat",
    "access",
    "readlink",
    "statvfs",
    "fstatvfs",
    "fsync",
    "fdatasync",
    "read",
    "pread",
    "write",
    "pwrite",
    "close",
    "copy_file_range",
    "sendfile",
    "ftruncate",
)
"""`os` calls that touch a file and raise no audit event. Measured on Python 3.14 for `stat`, `lstat`,
`access`, `readlink` and `statvfs`; `Path.exists` and `os.path.islink` are among them."""

_active: list[LoopFileCalls] = []
_session_sink: list[Callable[[str, tuple[object, ...]], None]] = []


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


def set_session_sink(sink: Callable[[str, tuple[object, ...]], None] | None) -> None:
    """Hand every watched event, from any thread, to *sink*; the sink decides which are the loop's."""
    _session_sink[:] = [] if sink is None else [sink]


def watch_unaudited() -> None:
    """Make each `UNAUDITED` call in `os` raise the audit event `os.<name>` before it runs.

    `pathlib`, `os.path` and `shutil` look the name up in `os` at call time, so they reach the
    wrapper. `shutil` asks the `os.supports_*` sets, which hold function objects, so each wrapper
    joins the sets its original is in.
    """
    for name in UNAUDITED:
        original = getattr(os, name, None)
        if original is None or hasattr(original, "__wrapped__"):
            continue
        wrapper = _audited(f"os.{name}", original)
        for supports in (os.supports_fd, os.supports_dir_fd, os.supports_follow_symlinks, os.supports_effective_ids):
            if original in supports:
                supports.add(wrapper)
        setattr(os, name, wrapper)


def _audited[**P, R](event: str, original: Callable[P, R]) -> Callable[P, R]:
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        sys.audit(event, args[0] if args else None)
        return original(*args, **kwargs)

    wrapper.__wrapped__ = original  # type: ignore[attr-defined]  # what functools.wraps sets
    wrapper.__name__ = original.__name__
    return wrapper


def _hook(event: str, args: tuple[object, ...]) -> None:
    if not event.startswith(_WATCHED_PREFIXES):
        return
    for sink in _session_sink:
        sink(event, args)
    if not _active:
        return
    ident = threading.get_ident()
    for recorder in _active:
        if recorder.thread == ident:
            recorder.calls.append((event, str(args[0]) if args else ""))


sys.addaudithook(_hook)
