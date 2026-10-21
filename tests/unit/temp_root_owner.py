"""A living temporary-roots owner for fixtures.

An unlocked temp file is a stale file by Nix's own definition: `findTempRoots`
unlinks whatever it can lock (`gc.cc:193`). A fixture that writes a temp file
and closes it stages a dead owner, and the mirror under test reaps it. This
helper writes the file and parks a subprocess on its write lock -- the shape
of `createTempRootsFile` (`gc.cc:62-65`) -- so the walk under test sees a
living owner.

The lock is `flock`, as Nix's `lockFile` takes it (`unix/pathlocks.cc:51`).
A POSIX `lockf` here hid that the mirror tested with `lockf`, which never
sees a `flock`: it reaped every live worker's file. nixidae/nixidae#1003.
"""

from __future__ import annotations

import errno
import fcntl
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

_HOLDER = (
    "import fcntl, sys, time; fd = open(sys.argv[1], 'r+b'); fcntl.flock(fd.fileno(), fcntl.LOCK_EX); time.sleep(60)"
)


def _wait_for_lock(path: Path) -> None:
    """Block until another process holds the write lock on `path`."""
    fd = os.open(path, os.O_RDWR | os.O_CLOEXEC)
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in (errno.EACCES, errno.EAGAIN):
                    return
                raise
            fcntl.flock(fd, fcntl.LOCK_UN)
            if time.monotonic() > deadline:
                raise TimeoutError(f"lock holder never took {path}")
            time.sleep(0.01)
    finally:
        os.close(fd)


@contextmanager
def live_temp_root(state_dir: Path, name: str, content: bytes) -> Iterator[None]:
    """A temp-roots file with a living owner, removed with the holder.

    The holder dies on context exit -- killed, then reaped -- so no process
    outlives the test that staged it.
    """
    target = state_dir / "temproots" / name
    target.write_bytes(content)  # noqa: ASYNC240 -- test setup
    holder = subprocess.Popen([sys.executable, "-c", _HOLDER, str(target)])
    try:
        _wait_for_lock(target)
        yield
    finally:
        holder.kill()
        holder.wait()
