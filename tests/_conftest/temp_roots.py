"""What the temporary-roots tests share: Nix's view of a roots file, and a stand-in collector."""

from __future__ import annotations

import fcntl
import os
import shutil
import socket
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


def read_roots(path: Path) -> list[str]:
    """The roots of a temporary roots file, as `findTempRoots` reads them."""
    return [part.decode() for part in path.read_bytes().split(b"\0") if part]


class Collector:
    """The socket half of `LocalStore::collectGarbage`, for one client.

    The real collector holds a write lock on `gc.lock` for as long as it
    runs, and it takes each later root over `gc-socket/socket`. This is what
    pynixd must talk to when it cannot get the shared lock.
    """

    def __init__(self, state: Path) -> None:
        self.state = state
        self.roots: list[str] = []
        self.lock_fd = os.open(state / "gc.lock", os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(self.lock_fd, fcntl.LOCK_EX)
        socket_path = state / "gc-socket" / "socket"
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server.bind(str(socket_path))
        self.server.listen(1)
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        conn, _ = self.server.accept()
        with conn:
            data = b""
            while chunk := conn.recv(4096):
                data += chunk
                while b"\n" in data:
                    line, data = data.split(b"\n", 1)
                    self.roots.append(line.decode())
                    conn.sendall(b"1")

    def stop(self) -> None:
        self.server.close()
        os.close(self.lock_fd)


@pytest.fixture
def short_path() -> Iterator[Path]:
    """A directory whose `gc-socket/socket` fits in `sun_path`, 104 bytes on Darwin; `tmp_path` holds the test's name."""
    path = Path(tempfile.mkdtemp(prefix="/tmp/pynixd-tr-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)
