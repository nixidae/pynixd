"""The controller's postStart hook in a runner build pod (nixidae/nixidae#1560, spike).

`python -I -m pynixd.provisioning.runner_ready READY` returns once READY exists. The kubelet creates the pod's
next container, the runner, only after the hook returns, so the runner starts once `runner_wait` touched READY:
after it wrote the files the runner mounts by subPath.
"""

from __future__ import annotations

import queue
import sys
from pathlib import Path

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer


class _Created(FileSystemEventHandler):
    def __init__(self, path: Path, seen: queue.SimpleQueue[None]) -> None:
        self.path = path
        self.seen = seen

    def on_any_event(self, event: FileSystemEvent) -> None:
        if self.path.exists():
            self.seen.put(None)


def wait_for(path: Path) -> None:
    """Return once *path* exists. The kubelet bounds a hook only by the pod's deadline."""
    seen: queue.SimpleQueue[None] = queue.SimpleQueue()
    observer = Observer()
    observer.schedule(_Created(path, seen), str(path.parent))
    observer.start()
    try:
        # Armed before this check, so a file made in between is not missed.
        if not path.exists():
            seen.get()
    finally:
        observer.stop()
        observer.join()


def main() -> None:
    (ready,) = sys.argv[1:]
    wait_for(Path(ready))


if __name__ == "__main__":
    main()
