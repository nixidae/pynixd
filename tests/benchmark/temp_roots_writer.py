"""One thread per store for every file operation on its temporary roots.

A candidate for `pynixd_store.temp_roots`, measured by `internal.tests.temp-roots`. It lives
beside the benchmark until the benchmark decides it.

The event loop never touches a roots file, the GC lock or the collector's socket. A session
queues a job and awaits its outcome. The thread takes every job already queued, writes the
roots of each file with one `flock` and one `write`, then wakes the loop once for the whole
pass. Writes, collector attempts and the close of one file run in the order their session
queued them, so a close can never run before a write it queued first, and no write can
recreate a file after its close.

One thread per state directory, not per process: a frozen or stalled filesystem holds only the
sessions of the store on it.
"""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import queue
import threading
import time
from typing import TYPE_CHECKING, Final, Self

import anyio
import anyio.to_thread
import structlog

from pynixd_store.store_path import StorePath
from pynixd_store.temp_roots import RETRY_DELAY, WAIT_REPORTED_AFTER, TempRoots

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path
    from types import TracebackType

log = structlog.get_logger(__name__)

SHUTDOWN_SECONDS: Final = 30.0
"""How long the writer's owner waits for queued jobs at exit; a frozen filesystem can hold them for longer."""


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
    owner: WrittenRoots
    kind: Kind
    roots: list[str]
    done: anyio.Event
    outcome: Outcome | None = None
    error: BaseException | None = None


class RootWriter:
    """The thread that does every file operation of one store's temporary roots.

    `async with RootWriter(state) as writer:` runs it; `writer.session()` gives a `TempRoots` for
    one client session. At exit the thread finishes every queued job, and the owner waits for it
    up to `SHUTDOWN_SECONDS`.
    """

    def __init__(self, state: Path) -> None:
        self.state = state
        self.passes = 0
        """Passes the thread made, each one wake-up of the loop. A test reads it."""
        self._jobs: queue.SimpleQueue[_Job | None] = queue.SimpleQueue()
        # Guards `_closed` and the put together, so no job can land behind the stop marker.
        self._accepting = threading.Lock()
        self._closed = False
        self._thread = threading.Thread(target=self._run, name=f"temp-roots {state}", daemon=True)
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def thread_ident(self) -> int | None:
        return self._thread.ident

    def join(self) -> None:
        """Block until the thread ends; for a worker thread, never the loop."""
        if self._thread.ident is not None:
            self._thread.join()

    def session(self, *, read_only: bool = False) -> WrittenRoots:
        return WrittenRoots(self.state, self, read_only=read_only)

    async def __aenter__(self) -> Self:
        # Waking a task from the thread must not wait for the loop. `anyio.from_thread.run_sync`
        # blocks the thread until the loop runs the call, which ties the writer to the stall it
        # exists to avoid, so this uses the running asyncio loop's thread-safe call (asyncio and
        # uvloop both have it).
        self._loop = asyncio.get_running_loop()
        self._thread.start()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        with self._accepting:
            self._closed = True
            self._jobs.put(None)
        with anyio.CancelScope(shield=True), anyio.move_on_after(SHUTDOWN_SECONDS):
            await anyio.to_thread.run_sync(self.join, abandon_on_cancel=True)
        if self._thread.is_alive():
            log.warning(
                "temp_roots_writer_outlived_shutdown",
                state=str(self.state),
                waited_s=SHUTDOWN_SECONDS,
                detail="the filesystem holds a write; its sessions stay waiting until it returns",
            )

    async def submit(self, owner: WrittenRoots, kind: Kind, roots: list[str]) -> Outcome:
        """Queue one job and wait for its outcome.

        A cancelled caller stops waiting, and the job still runs: a root written after its
        session gave up stays held until that session's close, which runs after it.
        """
        job = _Job(owner, kind, roots, anyio.Event())
        with self._accepting:
            if self._closed:
                raise WriterClosedError(f"the temporary roots writer of {self.state} has stopped")
            self._jobs.put(job)
        await job.done.wait()
        if job.error is not None:
            raise RootWriteError(f"{kind.value} of {len(roots)} temporary roots failed") from job.error
        if job.outcome is None:
            raise RuntimeError(f"the writer finished a {kind.value} job with no outcome")
        return job.outcome

    # ── The thread ────────────────────────────────────────────────────

    def _run(self) -> None:
        try:
            self._serve()
        finally:
            self._refuse_the_rest()

    def _refuse_the_rest(self) -> None:
        """Stop taking jobs, and fail every job still queued, so no caller waits on a thread that is gone."""
        with self._accepting:
            self._closed = True
        left: list[_Job] = []
        while True:
            try:
                job = self._jobs.get_nowait()
            except queue.Empty:
                break
            if job is not None:
                job.error = WriterClosedError(f"the temporary roots writer of {self.state} stopped first")
                left.append(job)
        if left:
            self._wake(left)

    def _serve(self) -> None:
        stopping = False
        while not stopping:
            first = self._jobs.get()
            if first is None:
                break
            jobs = [first]
            while True:
                try:
                    job = self._jobs.get_nowait()
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
                self._wake(jobs)
                raise
            self.passes += 1
            if not self._wake(jobs):
                return

    def _perform(self, jobs: list[_Job]) -> None:
        """Every job of one pass, each file's jobs in the order they were queued.

        A file's queued writes become one write; a collector attempt or a close first writes
        what that file queued before it.
        """
        pending: dict[WrittenRoots, list[_Job]] = {}
        for job in jobs:
            if job.kind is Kind.WRITE:
                pending.setdefault(job.owner, []).append(job)
                continue
            self._write(job.owner, pending.pop(job.owner, []))
            self._each(job)
        for owner, writes in pending.items():
            self._write(owner, writes)

    @staticmethod
    def _write(owner: WrittenRoots, writes: list[_Job]) -> None:
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

    def _wake(self, jobs: list[_Job]) -> bool:
        """Wake every caller of one pass with one call into the loop; False when the loop is gone."""
        if self._loop is None:
            raise RuntimeError("the temporary roots writer runs outside `async with`")
        try:
            self._loop.call_soon_threadsafe(_set_all, jobs)
        except RuntimeError:
            # The loop closed under the writer: nobody is left to wake, and nothing can queue more.
            return False
        return True


def _set_all(jobs: list[_Job]) -> None:
    for job in jobs:
        job.done.set()


class WrittenRoots(TempRoots):
    """`TempRoots` whose file operations all run on a `RootWriter`'s thread."""

    def __init__(self, state: Path, writer: RootWriter, *, read_only: bool = False) -> None:
        super().__init__(state, read_only=read_only)
        self._writer = writer
        self._released = False

    async def add(self, path: str | StorePath) -> bool:
        return await self.add_many([path])

    async def add_many(self, paths: Iterable[str | StorePath]) -> bool:
        """Hold every one of *paths* until `close`; False when the state directory refused them.

        Raises `RuntimeError` after `close`: a write then would make a file nobody releases.
        """
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
                log.warning("temp_root_waits_for_collector", root=root, waited_s=round(waited, 1))
            await anyio.sleep(RETRY_DELAY)
        return outcome

    async def close(self) -> None:
        """Release every root of this session, after every write it queued."""
        if self._released:
            return
        with anyio.CancelScope(shield=True):
            try:
                await self._writer.submit(self, Kind.CLOSE, [])
            except WriterClosedError:
                # The writer stopped taking jobs first. Its thread may still be inside a write of
                # this file, so the close waits for the thread to end before it touches the file.
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
