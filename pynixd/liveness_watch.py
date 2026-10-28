"""Wake promptly when the stable roots move.

`StableRootsWatch` watches the state dir through watchdog -- inotify on
Linux, FSEvents on macOS, picked per platform -- and sets a dirty event
on anything that may have moved the roots. The observer lives on its own
thread and pumps a plain queue; the async side blocks in `queue.get`
inside `to_thread` and sets the flag, so no anyio object ever crosses a
thread. The periodic check re-walks everything as usual, so this buys
latency, not correctness: a deployment retargets a link and the mirror
recomputes in milliseconds instead of at the next interval, and a quiet
tree costs no walks at all. A missed event degrades to polling, never to
a stale mirror, because the anchor check still walks the whole trees.

What wakes the check, and why each is safe:

- create/delete/move under either tree: every entry there is a root or
  a directory of roots. The watch is recursive, so a new subdirectory is
  covered from birth; the check re-walks the whole tree anyway.
- the state dir itself: only a tree appearing or leaving counts, so the
  churn under `temproots/` never wakes anyone. This closes the startup
  gap where a tree does not exist yet.

What never wakes it: symlinked directories are not descended into, the
way neither Nix (`findRoots` reads `symlink_status`, `gc.cc:250`) nor
the walk does -- watchdog follows the same rule with `recursive=True`,
which does not cross links -- and `/proc` cannot be watched at all, so
the volatile roots stay polled like before. File content changes carry
no signal either: roots move by replacement, never by rewrite.
"""

from __future__ import annotations

import os
import queue
from pathlib import Path

import anyio
import structlog
from anyio.to_thread import run_sync
from watchdog.events import FileSystemEvent, FileSystemEventHandler, FileSystemMovedEvent
from watchdog.observers import Observer

from .liveness import STABLE_TREES

log = structlog.get_logger(__name__)


def _as_str(path: str | bytes) -> str:
    """Watchdog reports bytes paths when asked to watch bytes; we never ask."""
    return os.fsdecode(path) if isinstance(path, bytes) else path


_WAKE_POLL_SECONDS = 0.5
"""Upper bound on shutdown latency, and nothing else.

The take path blocks in `queue.get` on a worker thread, and abandoning
that thread on cancel would leak it: `to_thread` waits for the worker by
default, and a bare `get` never returns on a quiet tree. Polling with a
timeout keeps every thread joinable and every teardown bounded; an event
still wakes the check at once.
"""


class DirtyFlag:
    """A re-armable dirty flag: `anyio.Event` fires once, this does not.

    The watcher sets it; the check clears it after waking and before
    working, so a change during the check wakes the next pass at once.
    """

    def __init__(self) -> None:
        self._event = anyio.Event()

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()

    def clear(self) -> None:
        self._event = anyio.Event()


class _RootsHandler(FileSystemEventHandler):
    """Queue a token for every event that may have moved the roots.

    Runs on the observer's thread and touches nothing but the queue, so
    it needs no loop and no synchronisation beyond the queue itself.
    """

    def __init__(self, state_dir: Path, wake: queue.Queue[bool]) -> None:
        super().__init__()
        self.state_dir = state_dir
        self.wake = wake

    def on_any_event(self, event: FileSystemEvent) -> None:
        if self._relevant(_as_str(event.src_path)):
            self.wake.put(True)
            return
        if isinstance(event, FileSystemMovedEvent) and self._relevant(_as_str(event.dest_path)):
            self.wake.put(True)

    def _relevant(self, path: str) -> bool:
        """True when *path* is a tree, or under one.

        Anything else under the state dir -- `temproots/`, `db/`,
        `daemon-socket/` -- is churn the check must sleep through.
        """
        try:
            rel = Path(path).relative_to(self.state_dir)
        except ValueError:
            return False
        if len(rel.parts) == 0:
            # The watched root itself (a directory-modified event on it):
            # it names no child, and every child change reports itself,
            # so there is nothing to attribute. Coalescing backends may
            # hide the child behind this, and then the periodic poll --
            # the anchor, not the watch -- catches it.
            return False
        if len(rel.parts) == 1:
            return rel.parts[0] in STABLE_TREES
        return rel.parts[0] in STABLE_TREES


class StableRootsWatch:
    """A recursive watch over the stable root trees, as a dirty flag."""

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir

    async def run(self, dirty: DirtyFlag) -> None:
        """Watch until cancelled, setting *dirty* on every relevant change."""
        wake: queue.Queue[bool] = queue.Queue()
        observer = Observer()
        observer.schedule(_RootsHandler(self.state_dir, wake), str(self.state_dir), recursive=True)
        try:
            observer.start()
        except OSError as exc:
            log.warning("stable_roots_no_state_dir", state_dir=str(self.state_dir), error=str(exc))
            await anyio.sleep_forever()
            return
        try:
            while True:
                try:
                    woke = await run_sync(wake.get, True, _WAKE_POLL_SECONDS)
                except queue.Empty:
                    continue
                if woke:
                    dirty.set()
        finally:
            observer.stop()
            await run_sync(observer.join)  # type: ignore[call-arg]  # zuban-bug: run_sync counts join's defaulted timeout as required; mypy accepts the call
