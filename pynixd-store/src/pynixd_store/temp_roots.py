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

**The event loop never touches a roots file, `gc.lock` or the collector's
socket.** A Nix daemon worker serves one client, so it writes inline and a
slow disk stalls that client alone. pynixd serves every client on one loop:
sg1's pynixd-0 wrote 3,344 roots inline on Ceph RBD, one `os.write` took
3.4 s, and the liveness probe killed the pod. A `RootWriter` thread per state
directory does every file operation. A session queues a job and awaits its
outcome; the thread takes every job already queued, writes the roots of each
file with one `flock` and one `write`, and wakes the loop once for the pass.
Measured by `internal.tests.temp-roots` on uvloop, 7,177 roots added one at a
time: a thread call per root 244 ms, the writer 138 ms, with the longest loop
stall 0.7 ms; a filesystem frozen for 2 s stalled the inline loop 1.95 s and
the writer's 2.7 ms.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import fcntl
import itertools
import os
import queue
import socket
import threading
import time
from typing import TYPE_CHECKING, Final, Self

import anyio
import anyio.to_thread
import structlog

from .store_path import StorePath

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path
    from types import TracebackType

log = structlog.get_logger(__name__)

GC_LOCK_FILE = "gc.lock"
GC_SOCKET_PATH = "gc-socket/socket"
TEMP_ROOTS_DIR = "temproots"

# The collector answers one byte for each root that it takes.
COLLECTOR_ACK = b"1"

PYNIXD_TEMP_ROOTS_PREFIX = "pynixd-"
"""The name of pynixd's own temporary-roots files starts with this."""

RETRY_DELAY = 0.1
"""How long a root waits before it asks the collector again, with no limit, as `addTempRoot` does (`gc.cc:95-135`).

A collector can hold the big lock with no socket for long. A daemon worker
that aborts keeps its lock while the kernel dumps its core: 18 s on dynhetz,
and a limit of 10 s failed the evaluation that sent the root
(nixidae/nixidae#1386). The wait sleeps on the event loop, so the session's
cancellation ends it.
"""

WAIT_REPORTED_AFTER = 5.0
"""A root that waits this long for the collector logs it once."""

SHUTDOWN_SECONDS: Final = 30.0
"""How long the writer's owner waits for queued jobs at exit; a frozen filesystem can hold them for longer."""

_names = itertools.count()


class Outcome(enum.Enum):
    HELD = "held"
    """Written, under the shared GC lock."""
    COLLECTING = "collecting"
    """The collector holds the GC lock: the root must go over its socket."""
    DISABLED = "disabled"
    """The state directory refused a write; the caller must hold the root some other way."""


class Kind(enum.Enum):
    WRITE = "write"
    COLLECT = "collect"
    CLOSE = "close"


class WriterClosedError(RuntimeError):
    """A job arrived after the writer stopped taking jobs."""


class RootWriteError(RuntimeError):
    """The writer thread raised while it did this job; `__cause__` holds what it raised."""


@dataclasses.dataclass(slots=True)
class _Job:
    owner: TempRoots
    kind: Kind
    roots: list[str]
    done: anyio.Event
    outcome: Outcome | None = None
    error: BaseException | None = None


@dataclasses.dataclass(slots=True, eq=False)
class _Run:
    """One `async with` of a `RootWriter`: the thread, its queue, and whether it takes jobs."""

    loop: asyncio.AbstractEventLoop
    jobs: queue.SimpleQueue[_Job | None] = dataclasses.field(default_factory=queue.SimpleQueue)
    # Guards `closed` and the put together, so no job can land behind the stop marker.
    accepting: threading.Lock = dataclasses.field(default_factory=threading.Lock)
    closed: bool = False
    thread: threading.Thread = dataclasses.field(default_factory=threading.Thread)


class RootWriter:
    """The thread that does every file operation of one store's temporary roots.

    `async with RootWriter(state) as writer:` runs it; `writer.session()` gives the `TempRoots` of
    one client session. At exit the thread finishes every queued job, and the owner waits for it
    up to `SHUTDOWN_SECONDS`. One writer per state directory, not per process: a frozen
    filesystem holds only the sessions of the store on it.
    """

    def __init__(self, state: Path) -> None:
        self.state = state
        self.passes = 0
        """Passes the thread made, each one wake-up of the loop. A test reads it."""
        self._run: _Run | None = None

    @property
    def thread_ident(self) -> int | None:
        return self._run.thread.ident if self._run is not None else None

    def join(self) -> None:
        """Block until the last run's thread ends; for a worker thread, never the loop."""
        if self._run is not None and self._run.thread.ident is not None:
            self._run.thread.join()

    def session(self, *, read_only: bool = False) -> TempRoots:
        return TempRoots(self.state, self, read_only=read_only)

    async def __aenter__(self) -> Self:
        # A store opens again after it closed (a server restarted in one process), and a thread
        # starts once: each `async with` is a run with its own thread, queue and stop flag. A
        # thread that outlived its shutdown keeps its own run and never touches the next one.
        if self._run is not None and not self._run.closed:
            raise RuntimeError(f"the temporary roots writer of {self.state} is already running")
        # Waking a task from the thread must not wait for the loop: the writer exists to keep a
        # stalled write off the loop, and a wake that waits for the loop ties the two together
        # again. `anyio.from_thread.run_sync` waits, and so does `BlockingPortal.start_task_soon`
        # (`from_thread.py`, `_spawn_task_from_thread` calls `run_sync`). The loop's own
        # thread-safe call does not; aiosqlite's worker thread wakes its callers the same way.
        run = _Run(asyncio.get_running_loop())
        run.thread = threading.Thread(
            target=self._thread_main, args=(run,), name=f"temp-roots {self.state}", daemon=True
        )
        self._run = run
        run.thread.start()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        run = self._run
        if run is None:
            return
        with run.accepting:
            run.closed = True
            run.jobs.put(None)
        with anyio.CancelScope(shield=True), anyio.move_on_after(SHUTDOWN_SECONDS):
            await anyio.to_thread.run_sync(_join, run.thread, abandon_on_cancel=True)
        if run.thread.is_alive():
            log.warning(
                "temp_roots_writer_outlived_shutdown",
                state=str(self.state),
                waited_s=SHUTDOWN_SECONDS,
                detail="the filesystem holds a write; its sessions stay waiting until it returns",
            )

    async def submit(self, owner: TempRoots, kind: Kind, roots: list[str]) -> Outcome:
        """Queue one job and wait for its outcome.

        The job is queued before the first wait. A cancelled caller stops waiting and the job
        still runs: a root written after its session gave up stays held until that session's
        close, which runs after it.
        """
        run = self._run
        if run is None:
            # Nothing would ever take the job, and its caller would wait forever.
            raise RuntimeError(f"the temporary roots writer of {self.state} runs outside `async with`")
        job = _Job(owner, kind, roots, anyio.Event())
        with run.accepting:
            if run.closed:
                raise WriterClosedError(f"the temporary roots writer of {self.state} has stopped")
            run.jobs.put(job)
        await job.done.wait()
        if job.error is not None:
            raise RootWriteError(f"{kind.value} of {len(roots)} temporary roots failed") from job.error
        if job.outcome is None:
            raise RuntimeError(f"the writer finished a {kind.value} job with no outcome")
        return job.outcome

    # ── The thread ────────────────────────────────────────────────────

    def _thread_main(self, run: _Run) -> None:
        try:
            self._serve(run)
        finally:
            self._refuse_the_rest(run)

    def _refuse_the_rest(self, run: _Run) -> None:
        """Stop taking jobs, and fail every job still queued, so no caller waits on a thread that is gone."""
        with run.accepting:
            run.closed = True
        left: list[_Job] = []
        while True:
            try:
                job = run.jobs.get_nowait()
            except queue.Empty:
                break
            if job is not None:
                job.error = WriterClosedError(f"the temporary roots writer of {self.state} stopped first")
                left.append(job)
        if left:
            _wake(run, left)

    def _serve(self, run: _Run) -> None:
        stopping = False
        while not stopping:
            first = run.jobs.get()
            if first is None:
                break
            jobs = [first]
            while True:
                try:
                    job = run.jobs.get_nowait()
                except queue.Empty:
                    break
                if job is None:
                    stopping = True
                    break
                jobs.append(job)
            try:
                self._perform(jobs)
            except BaseException as exc:
                for job in jobs:
                    if job.outcome is None and job.error is None:
                        job.error = exc
                _wake(run, jobs)
                raise
            self.passes += 1
            if not _wake(run, jobs):
                return

    def _perform(self, jobs: list[_Job]) -> None:
        """Every job of one pass, each file's jobs in the order they were queued.

        A file's queued writes become one write; a collector attempt or a close first writes
        what that file queued before it.
        """
        pending: dict[TempRoots, list[_Job]] = {}
        for job in jobs:
            if job.kind is Kind.WRITE:
                pending.setdefault(job.owner, []).append(job)
                continue
            self._write(job.owner, pending.pop(job.owner, []))
            self._each(job)
        for owner, writes in pending.items():
            self._write(owner, writes)

    @staticmethod
    def _write(owner: TempRoots, writes: list[_Job]) -> None:
        if not writes:
            return
        try:
            outcome = owner.write_now([root for job in writes for root in job.roots])
        except Exception as exc:  # noqa: BLE001 -- handed to each waiting caller, which raises it
            for job in writes:
                job.error = exc
            return
        for job in writes:
            job.outcome = outcome

    @staticmethod
    def _each(job: _Job) -> None:
        try:
            if job.kind is Kind.COLLECT:
                job.outcome = job.owner.collect_now(job.roots[0])
            else:
                job.owner.close_now()
                job.outcome = Outcome.HELD
        except Exception as exc:  # noqa: BLE001 -- handed to the waiting caller, which raises it
            job.error = exc


def _wake(run: _Run, jobs: list[_Job]) -> bool:
    """Wake every caller of one pass with one call into the loop; False when the loop is gone."""
    try:
        run.loop.call_soon_threadsafe(_set_all, jobs)
    except RuntimeError:
        # The loop closed under the writer: nobody is left to wake, and nothing can queue more.
        return False
    return True


def _join(thread: threading.Thread) -> None:
    thread.join()


def _set_all(jobs: list[_Job]) -> None:
    for job in jobs:
        job.done.set()


class TempRoots:
    """One temporary roots file, and the paths that it holds.

    One instance belongs to one client session, made by `RootWriter.session`. `close` releases
    every root of that session at once, and it is the reason this class exists: there is no
    operation in the daemon protocol that removes a temporary root, so the only way to release
    one is to let go of the file.

    The coroutines run on the loop and queue jobs; the `*_now` methods and everything below them
    run on the writer's thread alone.

    When pynixd cannot write the directory, which happens when it serves the system store as an
    unprivileged user, `add` answers `False` and `DaemonProxy.add_temp_root` gives the root to
    the daemon instead.
    """

    def __init__(self, state: Path, writer: RootWriter, *, read_only: bool = False) -> None:
        """Prepare a roots file under `state`, and open nothing yet.

        *read_only* is a read-only store: `add` holds every root and writes nothing, as
        `LocalStore::addTempRoot` (`gc.cc:86`) does, because nothing can be deleted from that store.
        """
        self.state = state
        self.read_only = read_only
        self.dir = state / TEMP_ROOTS_DIR
        self.path = self.dir / f"{PYNIXD_TEMP_ROOTS_PREFIX}{os.getpid()}-{next(_names)}"
        self._writer = writer
        self._fd: int | None = None
        self._gc_lock_fd: int | None = None
        self._socket: socket.socket | None = None
        self._disabled = False
        self._released = False

    async def add(self, path: str | StorePath) -> bool:
        """Hold `path` against the collector until `close`, and say whether it is held."""
        return await self.add_many([path])

    async def add_many(self, paths: Iterable[str | StorePath]) -> bool:
        """Hold every one of *paths* until `close`; False when the state directory refused them.

        Raises `RuntimeError` after `close`: a write then would make a file nobody releases.
        """
        # A `StorePath` is already normalized, so stringifying it is enough:
        # parsing it again costs a prefix strip per root, 7177 times a build.
        roots = [str(path) if isinstance(path, StorePath) else str(StorePath(str(path))) for path in paths]
        if self.read_only or not roots:
            return True
        if self._released:
            raise RuntimeError(f"temporary roots {self.path} are closed")
        if self._disabled:
            return False
        outcome = await self._writer.submit(self, Kind.WRITE, roots)
        if outcome is Outcome.COLLECTING:
            for root in roots:
                outcome = await self._collect(root)
                if outcome is Outcome.DISABLED:
                    break
        return outcome is Outcome.HELD

    async def _collect(self, root: str) -> Outcome:
        """Give *root* to a running collector, asking again with no limit, as Nix's `addTempRoot` does."""
        started = time.monotonic()
        reported = False
        while (outcome := await self._writer.submit(self, Kind.COLLECT, [root])) is Outcome.COLLECTING:
            waited = time.monotonic() - started
            if not reported and waited >= WAIT_REPORTED_AFTER:
                reported = True
                log.warning(
                    "temp_root_waits_for_collector",
                    root=root,
                    waited_s=round(waited, 1),
                    detail="a process holds gc.lock for writing and its roots socket does not answer",
                )
            await anyio.sleep(RETRY_DELAY)
        if reported:
            log.info("temp_root_collector_answered", root=root, waited_s=round(time.monotonic() - started, 1))
        return outcome

    async def close(self) -> None:
        """Release every root of this session, after every write it queued.

        The close is queued before the first wait, so a cancelled caller still releases the file.
        """
        if self._released:
            return
        try:
            await self._writer.submit(self, Kind.CLOSE, [])
        except WriterClosedError:
            # The writer stopped taking jobs first. Its thread may still be inside a write of this
            # file, so the close waits for the thread to end before it touches the file.
            with anyio.CancelScope(shield=True):
                await anyio.to_thread.run_sync(self._writer.join)
                await anyio.to_thread.run_sync(self.close_now)

    # ── On the writer's thread ────────────────────────────────────────

    def write_now(self, roots: list[str]) -> Outcome:
        self._refuse_after_close()
        if self._disabled:
            return Outcome.DISABLED
        if not self._add_many_inline(roots):
            return Outcome.COLLECTING
        return Outcome.DISABLED if self._disabled else Outcome.HELD

    def collect_now(self, root: str) -> Outcome:
        self._refuse_after_close()
        if self._disabled:
            return Outcome.DISABLED
        if not self._try_the_collector(root):
            return Outcome.COLLECTING
        return Outcome.DISABLED if self._disabled else Outcome.HELD

    def close_now(self) -> None:
        self._released = True
        self._close()

    def _refuse_after_close(self) -> None:
        """A write queued behind the close would make a new file that nothing ever releases."""
        if self._released:
            raise RuntimeError(f"temporary roots {self.path} are closed")

    def _add_many_inline(self, roots: list[str]) -> bool:
        """Write *roots* under the shared lock: False when the collector runs and each root needs its socket."""
        try:
            self._ensure_file()
            gc_lock = self._gc_lock()
            if not self._hold_the_gc_lock(gc_lock):
                return False
            try:
                # Under the shared lock, so the collector cannot start between
                # this write and the read of the file that it makes.
                self._write_bytes(b"".join(root.encode() + b"\0" for root in roots))
                return True
            finally:
                # Release the shared lock, but keep the descriptor. Nix keeps the
                # same descriptor in `LocalStore::_fdGCLock` for the process
                # (`src/libstore/gc.cc`, `addTempRoot`).
                fcntl.flock(gc_lock, fcntl.LOCK_UN)
        except OSError as exc:
            self._disable(exc)
            return True

    def _write_root(self, root: str) -> bool:
        """Write `root` under the shared lock, or report the collector running."""
        self._ensure_file()
        gc_lock = self._gc_lock()
        if not self._hold_the_gc_lock(gc_lock):
            return False
        try:
            self._write_raw(root)
            return True
        finally:
            fcntl.flock(gc_lock, fcntl.LOCK_UN)

    def _write_raw(self, root: str) -> None:
        """Append `root` to the roots file.

        The collector reads the file once and then takes later roots over its
        socket, so Nix writes the root to the file in both cases: the file is
        what the next run of the collector reads.
        """
        self._write_bytes(root.encode() + b"\0")

    def _write_bytes(self, data: bytes) -> None:
        self._ensure_file()
        fd = self._fd
        if fd is None:
            raise RuntimeError("pynixd: the temporary roots file is not open")
        view = memoryview(data)
        while view:
            view = view[os.write(fd, view) :]

    def _ensure_file(self) -> None:
        if self._fd is None:
            self._create()

    def _gc_lock(self) -> int:
        if self._gc_lock_fd is None:
            self._gc_lock_fd = os.open(self.state / GC_LOCK_FILE, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
        return self._gc_lock_fd

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

    def _try_the_collector(self, root: str) -> bool:
        """One attempt while the collector runs: the socket takes the root, then the file.

        The shared lock failed, so the collector holds the big lock. It read
        the `temproots` directory before this root existed, so the socket is
        how it learns about the root now. The file still gets it as well, for
        the next run of the collector. False asks the caller to try again;
        True is held, or disabled.
        """
        try:
            if self._tell_the_collector(root):
                self._write_raw(root)
                return True
            return self._write_root(root)
        except OSError as exc:
            self._disable(exc)
            return True

    def _hold_the_gc_lock(self, gc_lock: int) -> bool:
        """True when the collector is not running, and pynixd may write.

        The shared lock lasts until the caller releases it with `LOCK_UN`. The
        collector takes the same file for writing, so it waits for every reader.
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
