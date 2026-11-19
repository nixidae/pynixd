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
  a directory of roots. The watch on each tree is recursive, so a new
  subdirectory is covered from birth; the check re-walks the whole tree
  anyway.
- the state dir itself: only a tree appearing or leaving counts, so the
  churn under `temproots/` never wakes anyone. This closes the startup
  gap where a tree does not exist yet.

The state dir watch is not recursive. A recursive one puts an inotify
watch on every directory under it, and `builds/` alone holds hundreds
(578 measured on dynhetz), none of them a root (nixidae/nixidae#1142).
A tree that appears gets its recursive watch then; one that leaves loses
it, since the kernel drops a watch on a deleted directory.

What never wakes it: symlinked directories are not descended into, the
way neither Nix (`findRoots` reads `symlink_status`, `gc.cc:250`) nor
the walk does -- watchdog follows the same rule with `recursive=True`,
which does not cross links -- and `/proc` cannot be watched at all, so
the volatile roots stay polled like before. File content changes carry
no signal either: roots move by replacement, never by rewrite.
"""

from __future__ import annotations

import queue
import stat
import threading
from pathlib import Path

import anyio
import structlog
from anyio.to_thread import run_sync
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import ObservedWatch

from pynixd_store.liveness import STABLE_TREES

log = structlog.get_logger(__name__)


type _Identity = tuple[int, int]

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


class _TreeHandler(FileSystemEventHandler):
    """Queue a token for every event inside a tree: each may move a root.

    Runs on the observer's thread and touches nothing but the queue, so
    it needs no loop and no synchronisation beyond the queue itself.
    """

    def __init__(self, wake: queue.Queue[bool]) -> None:
        super().__init__()
        self.wake = wake

    def on_any_event(self, event: FileSystemEvent) -> None:
        self.wake.put(True)


class _StateDirHandler(FileSystemEventHandler):
    """Follow the trees appearing and leaving in the state dir.

    Anything else there -- `temproots/`, `db/`, `daemon-socket/` -- is
    churn the check must sleep through. The event does not say which tree
    moved: FSEvents reports only `modified` on the state dir itself, with
    no `created` for the child. So every event compares each tree with
    the one its watch follows.
    """

    def __init__(self, watch: StableRootsWatch, wake: queue.Queue[bool]) -> None:
        super().__init__()
        self.watch = watch
        self.wake = wake

    def on_any_event(self, event: FileSystemEvent) -> None:
        if self.watch.sync():
            self.wake.put(True)


class StableRootsWatch:
    """A recursive watch over each stable root tree, as a dirty flag."""

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self._observer = Observer()
        self._handler: _TreeHandler | None = None
        self._trees: dict[str, tuple[ObservedWatch, _Identity]] = {}
        self._lock = threading.Lock()

    def _identity(self, tree: str) -> _Identity | None:
        """The tree's directory as (device, inode), or `None` when it is no directory to watch.

        Never a symlink: Nix does not descend one (`gc.cc:313`).
        """
        try:
            st = (self.state_dir / tree).lstat()
        except OSError:
            return None
        return (st.st_dev, st.st_ino) if stat.S_ISDIR(st.st_mode) else None

    def sync(self) -> bool:
        """Watch each tree that is there now, and drop the watch of each one that left; whether any moved.

        A watch whose directory was deleted is dead, and watchdog keys
        watches by path, so a tree that comes back needs the old one gone
        first. A new inode tells a tree that came back from one that stayed.
        """
        moved = False
        with self._lock:
            for tree in STABLE_TREES:
                now = self._identity(tree)
                old = self._trees.get(tree)
                if (old[1] if old else None) == now:
                    continue
                moved = True
                if old is not None:
                    del self._trees[tree]
                    self._observer.unschedule(old[0])
                if now is None or self._handler is None:
                    continue
                try:
                    watch = self._observer.schedule(self._handler, str(self.state_dir / tree), recursive=True)
                except OSError as exc:
                    # Gone again before the watch landed; its delete event follows.
                    log.debug("stable_roots_tree_vanished", tree=tree, error=str(exc))
                    continue
                self._trees[tree] = (watch, now)
        return moved

    def _start(self, wake: queue.Queue[bool]) -> bool:
        """Start the observer; whether it runs."""
        self._handler = _TreeHandler(wake)
        self._observer.schedule(_StateDirHandler(self, wake), str(self.state_dir), recursive=False)
        self.sync()
        try:
            self._observer.start()
        except OSError as exc:
            log.warning("stable_roots_no_state_dir", state_dir=str(self.state_dir), error=str(exc))
            return False
        return True

    def _stop(self) -> None:
        self._observer.stop()
        self._observer.join()

    async def run(self, dirty: DirtyFlag) -> None:
        """Watch until cancelled, setting *dirty* on every relevant change."""
        wake: queue.Queue[bool] = queue.Queue()
        # Off the loop: `start` walks each tree to put a watch on every directory in it, and the
        # trees are as large as the machine's roots. `run_sync` waits for the thread when cancelled.
        if not await run_sync(self._start, wake):
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
            await run_sync(self._stop)
