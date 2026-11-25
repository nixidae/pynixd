"""Which path the cold tier works on next: the state of its upload queue, with no I/O. nixidae/nixidae#1600.

The queue holds every local path that neither the bucket nor a stable store covers. A path moves through two
stages. The NAR stage puts the NAR in the bucket, and any number of paths run it at once. The publish stage
puts the narinfo, which is the commit, and a path reaches it only when each of its references is covered: a
narinfo in the bucket means its whole closure is there or in a stable store (`cold_tier`).

Two lanes order the NAR stage: `URGENT` for what the collector waits on, `BACKGROUND` for the rest. Each lane
is in insertion order. A path whose NAR is in the bucket and whose references are not waits as *gated*, and
the publish of its last uncovered reference releases it. A path that cannot go up is *parked* until the next
start, and so is each referrer gated on it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


class Lane(Enum):
    URGENT = "urgent"
    BACKGROUND = "background"


class Stage(Enum):
    NAR = "nar"
    PUBLISH = "publish"


@dataclass(frozen=True, slots=True)
class Work:
    path: str
    stage: Stage


class UploadQueue:
    def __init__(self) -> None:
        self._lanes: dict[Lane, dict[str, None]] = {Lane.URGENT: {}, Lane.BACKGROUND: {}}
        self._lane: dict[str, Lane] = {}
        """Every path the queue holds, by the lane it goes in: queued, busy, gated or publishable."""
        self._busy: set[str] = set()
        self._gated: dict[str, set[str]] = {}
        """A staged path and the references it waits on."""
        self._waiters: dict[str, set[str]] = {}
        """A reference and the gated paths that wait on it."""
        self._publishable: dict[Lane, dict[str, None]] = {Lane.URGENT: {}, Lane.BACKGROUND: {}}
        self._parked: set[str] = set()
        self._covered: set[str] = set()
        """What this process saw covered. A query of the record that ran before the cover lands can name it."""

    def __contains__(self, path: str) -> bool:
        return path in self._lane

    def depth(self) -> dict[str, int]:
        """The paths in each state, for the metrics."""
        return {
            Lane.URGENT.value: len(self._lanes[Lane.URGENT]),
            Lane.BACKGROUND.value: len(self._lanes[Lane.BACKGROUND]),
            "busy": len(self._busy),
            "gated": len(self._gated),
            "publishable": sum(len(lane) for lane in self._publishable.values()),
            "parked": len(self._parked),
        }

    def pending(self, paths: Iterable[str]) -> set[str]:
        """The paths of *paths* the queue holds and has not parked: what a wait on coverage can still get."""
        return {path for path in paths if path in self._lane and path not in self._parked}

    def add(self, paths: Iterable[str], lane: Lane = Lane.BACKGROUND) -> int:
        """Queue *paths* for their NAR stage at the end of *lane*; a path the queue holds moves up to *lane*.

        A covered or parked path stays out. Answers how many paths are new.
        """
        added = 0
        raised: list[str] = []
        for path in paths:
            if path in self._covered or path in self._parked:
                continue
            if path in self._lane:
                if lane is Lane.URGENT:
                    raised.append(path)
                continue
            self._lane[path] = lane
            self._lanes[lane][path] = None
            added += 1
        self.raise_urgent(raised)
        return added

    def raise_urgent(self, paths: Iterable[str]) -> None:
        """Move *paths* to the urgent lane, with what a gated one among them waits on."""
        todo = list(paths)
        while todo:
            path = todo.pop()
            if self._lane.get(path) is not Lane.BACKGROUND:
                continue
            self._lane[path] = Lane.URGENT
            if self._lanes[Lane.BACKGROUND].pop(path, 0) is None:
                self._lanes[Lane.URGENT][path] = None
            elif self._publishable[Lane.BACKGROUND].pop(path, 0) is None:
                self._publishable[Lane.URGENT][path] = None
            todo.extend(self._gated.get(path, ()))

    def take(self) -> Work | None:
        """The next work: a publish before a NAR, and urgent before background. `None` when nothing waits."""
        for lane in Lane:
            if self._publishable[lane]:
                path = next(iter(self._publishable[lane]))
                del self._publishable[lane][path]
                self._busy.add(path)
                return Work(path, Stage.PUBLISH)
        for lane in Lane:
            if self._lanes[lane]:
                path = next(iter(self._lanes[lane]))
                del self._lanes[lane][path]
                self._busy.add(path)
                return Work(path, Stage.NAR)
        return None

    def staged(self, path: str, uncovered: set[str]) -> list[str]:
        """*path*'s NAR is in the bucket and *uncovered* are the references the record does not cover.

        With none left, *path* is publishable. Otherwise it waits on them, and each one the queue does not
        hold joins *path*'s lane. A parked reference parks *path*. Answers the paths it parked.
        """
        self._busy.discard(path)
        lane = self._lane[path]
        waiting = uncovered - self._covered - {path}
        if waiting & self._parked:
            return self.park(path)
        if not waiting:
            self._publishable[lane][path] = None
            return []
        self._gated[path] = waiting
        for ref in waiting:
            self._waiters.setdefault(ref, set()).add(path)
        self.add(waiting, lane)
        return []

    def covered(self, path: str) -> None:
        """*path* is in the bucket or a stable store: the referrers that waited only on it become publishable."""
        self._busy.discard(path)
        self._forget(path)
        self._covered.add(path)
        for waiter in self._waiters.pop(path, set()):
            waiting = self._gated[waiter]
            waiting.discard(path)
            if not waiting:
                del self._gated[waiter]
                self._publishable[self._lane[waiter]][waiter] = None

    def retry(self, work: Work) -> None:
        """Put *work* back at the head of its lane, after a fault that asking again can mend."""
        self._busy.discard(work.path)
        lanes = self._publishable if work.stage is Stage.PUBLISH else self._lanes
        lane = lanes[self._lane[work.path]]
        lanes[self._lane[work.path]] = {work.path: None, **lane}

    def drop(self, path: str) -> list[str]:
        """*path* left the local store: it goes, and what waits on it is parked. Answers the parked paths.

        Not parked itself: registered again, it has a new row and the next fill queues it.
        """
        parked: list[str] = []
        for waiter in self._waiters.pop(path, set()):
            parked.extend(self.park(waiter))
        self._busy.discard(path)
        self._forget(path)
        return parked

    def park(self, path: str) -> list[str]:
        """*path* cannot go up in this process; neither can what waits on it. Answers every path parked."""
        parked: list[str] = []
        todo = [path]
        while todo:
            one = todo.pop()
            if one in self._parked:
                continue
            self._busy.discard(one)
            self._forget(one)
            self._parked.add(one)
            parked.append(one)
            todo.extend(self._waiters.pop(one, ()))
        return parked

    def _forget(self, path: str) -> None:
        lane = self._lane.pop(path, None)
        if lane is not None:
            self._lanes[lane].pop(path, None)
            self._publishable[lane].pop(path, None)
        for ref in self._gated.pop(path, ()):
            waiters = self._waiters.get(ref)
            if waiters is not None:
                waiters.discard(path)
                if not waiters:
                    del self._waiters[ref]
