"""The temporary roots of the collector, written by pynixd itself.

A temporary root keeps a store path alive while a client still needs it, and
it goes away when that client does. Nix writes one file for each process, at
`<state>/temproots/<pid>`, and the process holds a write lock on that file
for as long as the roots must live. Each root is one store path and one NUL
byte, appended to the file.

`LocalStore::findTempRoots` reads that directory. A file it can write-lock
belongs to a process that is gone, so the collector deletes the file. A file
it cannot write-lock gives it one root for each path inside. The name of the
file means nothing to the collector, which uses the name for a log line only,
so pynixd is free to write one file for each client session.

**pynixd forwarded `AddTempRoot` to the upstream daemon, and that is the
defect of issue #20.** The root then belonged to the upstream connection,
and pynixd pools those connections between clients. So the root of a client
outlived that client, and a discarded connection dropped the root of a client
that still ran. A root that pynixd writes itself needs no connection at all,
and its life is exactly the life of the client session.

Nix implements the same three steps in `LocalStore::addTempRoot`, in
`src/libstore/gc.cc`, and this module follows that function. The lock is
`flock(2)`, from `src/libstore/unix/pathlocks.cc`.
"""

from __future__ import annotations

import fcntl
import itertools
import os
import socket
import time
from pathlib import Path

import anyio
import structlog
from anyio.to_thread import run_sync

from .store_path import StorePath

log = structlog.get_logger(__name__)

GC_LOCK_FILE = "gc.lock"
GC_SOCKET_PATH = "gc-socket/socket"
TEMP_ROOTS_DIR = "temproots"

# The collector answers one byte for each root that it takes.
COLLECTOR_ACK = b"1"

# How long to wait before pynixd asks the collector again, and how many times.
# The collector is between two states when it refuses the socket: it holds the
# big lock, and it has not made the socket yet.
#
# NIX-DEVIATION (#27): `addTempRoot` at `gc.cc:95-135` waits 100 ms and tries
# again with no limit. pynixd gives up after 10 s, because a client waits for
# the answer and a daemon that never answers is worse than an error. The cost
# is an error where Nix would still be waiting; reverse it if a collector is
# measured to stay in that state longer than 10 s.
RETRY_DELAY = 0.1
RETRY_LIMIT = 100

_names = itertools.count()


class TempRoots:
    """One temporary roots file, and the paths that it holds.

    One instance belongs to one client session. `close` releases every root
    of that session at once, and it is the reason this class exists: there is
    no operation in the daemon protocol that removes a temporary root, so the
    only way to release one is to let go of the file.

    Every method runs on the session task: `add` from dispatch, `close`
    after the operation loop ends. The two never run together, so the lock
    below guards only the collector path against itself, and not `add`
    against `close`.

    When pynixd cannot write the directory, which happens when it serves
    the system store as an unprivileged user, `add` answers `False` and
    `DaemonProxy.add_temp_root` gives the root to the daemon instead.
    """

    def __init__(self, state: Path) -> None:
        """Prepare a roots file under `state`, and open nothing yet."""
        self.state = state
        self.dir = state / TEMP_ROOTS_DIR
        self.path = self.dir / f"pynixd-{os.getpid()}-{next(_names)}"
        self._fd: int | None = None
        self._gc_lock_fd: int | None = None
        self._socket: socket.socket | None = None
        self._disabled = False
        self._lock = anyio.Lock()

    async def add(self, path: str | StorePath) -> bool:
        """Hold `path` against the collector until `close`, and say whether it is held.

        `False` once this instance has disabled itself: the state directory
        is not writable, and the caller must hold the root some other way.

        The common case does no waiting and takes no lock: the roots file and
        the GC lock are already open, and the shared lock is non-blocking, so
        the write runs on the event loop. A session adds one root for each
        derivation of a build, and the `anyio` lock and a thread hop for each
        of those cost more than the syscalls they carry.

        **The lock is for the collector path alone.** The proxy dispatches
        the operations of a session one at a time and closes the roots after
        the loop, so nothing runs `add` beside `add` and nothing runs `add`
        beside `close`. Only the collector path can wait, so that path keeps
        the lock and the thread hop.

        The collector running is the case that waits, over its socket. That
        runs in a worker thread, so the event loop is free while it does.
        """
        # A `StorePath` is already normalized, so stringifying it is enough:
        # parsing it again costs a prefix strip per root, 7177 times a build.
        root = str(path) if isinstance(path, StorePath) else str(StorePath(str(path)))
        if self._disabled:
            return False
        if self._add_inline(root):
            return not self._disabled
        async with self._lock:
            if self._disabled:
                return False
            await run_sync(self._add, root)
        return not self._disabled

    async def close(self) -> None:
        """Release every root of this session."""
        async with self._lock:
            await run_sync(self._close)

    # ── The blocking half ────────────────────────────────────────────
    #
    # `_add_inline` is the fast path and stays synchronous: an `open` on the
    # first root, a non-blocking `flock`, and a `write`. The rest runs in a
    # worker thread. The socket is the one part that waits, and only while
    # the collector runs.

    def _add_inline(self, root: str) -> bool:
        """Write `root` without waiting, and say whether the thread path is unneeded.

        True means written, or disabled: either way there is nothing for a
        worker thread to do. False means the collector is running, so the
        root needs its socket and the caller must hand the work to a thread.
        """
        try:
            return self._write_root(root)
        except OSError as exc:
            self._disable(exc)
            return True

    def _write_root(self, root: str) -> bool:
        """Write `root` under the shared lock, or report the collector running.

        False says the collector holds the big lock, so the caller must give
        the root to the collector over its socket instead.
        """
        self._ensure_file()
        gc_lock = self._gc_lock()
        if not self._hold_the_gc_lock(gc_lock):
            return False
        try:
            # Under the shared lock, so the collector cannot start between
            # this write and the read of the file that it makes.
            self._write_raw(root)
            return True
        finally:
            # Release the shared lock, but keep the descriptor. The lock is
            # per call; the open and the close are not, and a session adds one
            # root for each derivation of a build. Nix keeps the same
            # descriptor in `LocalStore::_fdGCLock` for the process
            # (`src/libstore/gc.cc`, `addTempRoot`).
            fcntl.flock(gc_lock, fcntl.LOCK_UN)

    def _write_raw(self, root: str) -> None:
        """Append `root` to the roots file.

        The collector reads the file once and then takes later roots over its
        socket, so Nix writes the root to the file in both cases: the file is
        what the next run of the collector reads.
        """
        self._ensure_file()
        fd = self._fd
        if fd is None:
            raise RuntimeError("pynixd: the temporary roots file is not open")
        os.write(fd, root.encode() + b"\0")

    def _ensure_file(self) -> None:
        if self._fd is None:
            self._create()

    def _gc_lock(self) -> int:
        if self._gc_lock_fd is None:
            self._gc_lock_fd = os.open(self.state / GC_LOCK_FILE, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        return self._gc_lock_fd

    def _add(self, root: str) -> None:
        try:
            self._add_or_raise(root)
        except OSError as exc:
            self._disable(exc)

    def _disable(self, exc: OSError) -> None:
        self._disabled = True
        log.warning(
            "temp_root_unavailable",
            temp_roots_file=str(self.path),
            error=str(exc),
            detail=(
                "pynixd cannot write the temporary roots of this store, so it holds no "
                "path against the collector. Give pynixd write access to the state "
                "directory of the store, or run the collector while pynixd is stopped."
            ),
        )

    def _add_or_raise(self, root: str) -> None:
        """The collector is running: the socket takes the root, then the file.

        `_write_root` failed its lock, so the collector holds the big lock. It
        read the `temproots` directory before this root existed, so the socket
        is how it learns about the root now. The file still gets it as well,
        for the next run of the collector.
        """
        for _ in range(RETRY_LIMIT):
            if self._tell_the_collector(root):
                self._write_raw(root)
                return
            if self._write_root(root):
                return
            time.sleep(RETRY_DELAY)

        raise RuntimeError(f"pynixd: the collector did not take the temporary root {root!r}")

    def _hold_the_gc_lock(self, gc_lock: int) -> bool:
        """True when the collector is not running, and pynixd may write.

        The shared lock lasts until the caller releases it with `LOCK_UN`,
        which `_write_root` does once the root is written. The collector takes
        the same file for writing, so it waits for every reader.
        """
        try:
            fcntl.flock(gc_lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        return True

    def _create(self) -> None:
        """Make the roots file, and take the write lock that owns it."""
        self.dir.mkdir(parents=True, exist_ok=True)
        while True:
            # A file of this name is stale. The name holds the pid of this
            # process and a counter that never gives the same number twice.
            self.path.unlink(missing_ok=True)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
            if os.fstat(fd).st_size == 0:
                self._fd = fd
                return
            # The collector deleted the file before the lock arrived, and it
            # wrote one byte to say so. Make another file.
            os.close(fd)

    def _tell_the_collector(self, root: str) -> bool:
        """Give the root to the collector over its socket.

        The collector holds the big lock while it runs, so it reads the
        `temproots` directory once and takes every later root over this
        socket. It answers one byte for each root that it took.

        False asks the caller to try again. The collector may have stopped
        between the refused lock and this call, and it may not have made the
        socket yet.
        """
        if self._socket is None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.connect(str(self.state / GC_SOCKET_PATH))
            except (FileNotFoundError, ConnectionRefusedError):
                sock.close()
                return False
            self._socket = sock

        try:
            self._socket.sendall(root.encode() + b"\n")
            ack = self._socket.recv(1)
        except OSError:
            ack = b""

        if ack == COLLECTOR_ACK:
            return True
        self._socket.close()
        self._socket = None
        return False

    def _close(self) -> None:
        if self._socket is not None:
            self._socket.close()
            self._socket = None
        if self._gc_lock_fd is not None:
            os.close(self._gc_lock_fd)
            self._gc_lock_fd = None
        if self._fd is not None:
            # Unlink before the close. A collector that already opened the
            # file reads the roots and keeps them, which is the safe answer.
            # A collector that opens it after this gets ENOENT and skips it,
            # which is also right, because the session is over.
            self.path.unlink(missing_ok=True)
            os.close(self._fd)  # This releases the write lock.
            self._fd = None
