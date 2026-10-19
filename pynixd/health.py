"""What "healthy" means for pynixd, and the loop measurement behind it.

Two things a probe has to tell apart, and a TCP accept on the data port tells
neither (nixidae/nixidae#432, #53):

- **Wedged.** The kernel completes `connect()` from the listen backlog whether
  or not the application ever accepts, so a TCP probe passes against a process
  that has stopped serving entirely.
- **Busy.** A transfer that does not yield stalls the loop, and a stalled loop
  answers nothing. The probe then fails against a process that is working
  correctly, and the restart destroys the evidence.

So health here is two questions. Is the loop running callbacks, and is every
interface the configuration asked for actually serving? Neither is answerable
from outside the process.

`EVENT_LOOP_LAG` is exported whether or not it is over the threshold, because
the useful form of this number is a graph that rises before anything restarts.
"""

from __future__ import annotations

import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import anyio
import structlog

from . import metrics
from .constants import STALL_TRACEBACK_MAX_BYTES

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path
    from typing import TextIO

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class HealthReport:
    """The verdict, and why.

    `checks` carries every check by name so a failing probe says which one
    failed in its own response body. A probe that only says "unhealthy" moves
    the diagnosis to a log nobody has yet.
    """

    ok: bool
    checks: Mapping[str, str] = field(default_factory=dict)

    def as_text(self) -> str:
        head = "ok" if self.ok else "unhealthy"
        body = "\n".join(f"{name}: {state}" for name, state in sorted(self.checks.items()))
        return f"{head}\n{body}\n" if body else f"{head}\n"


class StallWatchdog:
    """Dump every thread's Python stack when the loop stops beating.

    The stall this exists for is not reproducible and destroys its own
    evidence: a push sat at 100% CPU for about ten minutes with no progress,
    and the liveness probe then restarted the container. Nothing recorded what
    it was doing, and a benchmark has never produced it.

    So the diagnosis has to come from the process it happens in. The loop
    writes a timestamp every lag sample; this thread watches that timestamp
    and dumps stacks when it goes stale.

    **A thread, because the loop cannot report on itself.** Anything scheduled
    on the loop runs after the stall, where the stack no longer shows what
    held it.

    It works for the case that matters. CPU-bound Python releases the GIL
    every switch interval, so this thread is scheduled even while the loop
    never yields. A stall inside a C call that holds the GIL for its whole
    duration would defeat it, and no dump appearing is itself a fact worth
    having.

    One dump per stall, not one per check, or a ten-minute stall writes
    hundreds.
    """

    #: The default cap on the file sink. `stall_traceback_max_bytes` overrides
    #: it, because whoever points the sink at a volume is the person who knows
    #: what that volume can spare, and a constant can only be changed by a
    #: release.
    MAX_SINK_BYTES = STALL_TRACEBACK_MAX_BYTES

    def __init__(
        self,
        threshold: float,
        *,
        interval: float = 1.0,
        stream: TextIO | None = None,
        path: Path | None = None,
        max_bytes: int | None = None,
    ) -> None:
        self.threshold = threshold
        self._interval = interval
        self._stream = stream
        self._path = path
        self.max_bytes = self.MAX_SINK_BYTES if max_bytes is None else max_bytes
        self._last_beat = time.monotonic()
        self._armed = True
        self._stop = threading.Event()
        self.dumps = 0
        self.sink_errors = 0
        self.sink_skipped = 0
        self._reported_full = False

    def beat(self) -> None:
        """Record that the loop is still running callbacks."""
        self._last_beat = time.monotonic()
        self._armed = True

    def _check(self, now: float) -> bool:
        """Whether a dump is due. Separate from the thread so a test can drive it."""
        if not self._armed or self.threshold <= 0:
            return False
        if now - self._last_beat < self.threshold:
            return False
        self._armed = False
        return True

    def dump(self) -> None:
        """Write every thread's stack.

        `sys._current_frames` rather than `faulthandler.dump_traceback`, which
        needs a real file descriptor and so cannot write to a buffer a test
        holds. faulthandler's advantage is that it runs without the GIL, and
        that does not apply here: this is a Python thread, so it holds the GIL
        to run at all.
        """
        stalled_for = time.monotonic() - self._last_beat
        stream = self._stream if self._stream is not None else sys.stderr
        names = {t.ident: t.name for t in threading.enumerate()}
        lines = [
            f"pynixd: {datetime.now(tz=UTC).isoformat()} event loop has not run a "
            f"callback for {stalled_for:.1f}s; stacks of every thread follow",
        ]
        for ident, frame in sys._current_frames().items():  # noqa: SLF001
            lines.append(f"Thread {names.get(ident, '<unknown>')} ({ident}):")
            lines.extend(line.rstrip("\n") for line in traceback.format_stack(frame))
        text = "\n".join(lines)
        print(text, file=stream, flush=True)
        self._write_sink(text)
        self.dumps += 1

    def _write_sink(self, text: str) -> None:
        """Also write the dump somewhere a container restart cannot take.

        stderr reaches the container log, and that is the first place to look.
        It is not a place to rely on: a stall that ends in a restart races the
        log shipper, and a pod that is deleted and recreated takes the previous
        container's log with it. This is the failure the watchdog exists for,
        so its only sink must not be the thing that failure destroys.

        Best effort by construction. A dump that cannot be filed must never
        take down the process it is reporting on, so every error here is
        counted and swallowed.

        **Swallowed is not silent.** A dump that stopped landing looks exactly
        like a stall that never happened, which is the same fault as writing
        only to stderr. So both refusals say so on stderr and name the path:
        the one that could not write, and the one that is full. Once each,
        because the next dump would repeat it.

        **The cap keeps the first dumps, not the last.** It appends until the
        file is full and then refuses, rather than rotating. The first stall
        after a deployment is usually the one that explains the stalls after
        it, and a rotation would be the thing that threw it away.
        """
        if self._path is None:
            return
        stream = self._stream if self._stream is not None else sys.stderr
        try:
            if self.max_bytes and self._path.exists() and self._path.stat().st_size >= self.max_bytes:
                self.sink_skipped += 1
                if not self._reported_full:
                    self._reported_full = True
                    print(
                        f"pynixd: the stall dump file {self._path} has reached "
                        f"{self.max_bytes} bytes; further dumps go to stderr only",
                        file=stream,
                        flush=True,
                    )
                return
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(text + "\n\n")
        except OSError as exc:
            self.sink_errors += 1
            print(f"pynixd: could not write the stall dump to {self._path}: {exc}", file=stream, flush=True)

    def run(self) -> None:
        while not self._stop.wait(self._interval):
            if self._check(time.monotonic()):
                self.dump()

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self.run, name="pynixd-stall-watchdog", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()


class LoopLagMonitor:
    """Sample how long the event loop goes without running a ready callback.

    The measurement is the delay a `sleep` overshoots by: the loop was asked to
    wake this task after `interval` and did not, so the difference is time it
    spent not scheduling. That is exactly what a probe experiences.

    `window_max` is what health asks about, and it decays -- a stall a minute
    ago must not hold the process unhealthy once it is serving again.
    `lifetime_max` never decays, because the largest stall a process ever had
    is what an operator wants after the fact.
    """

    def __init__(
        self,
        interval: float = 0.25,
        window: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        watchdog: StallWatchdog | None = None,
    ) -> None:
        self._interval = interval
        self._window = window
        self.watchdog = watchdog
        # Injectable so the decay rule can be tested by moving time rather than
        # by waiting for it. A test that sleeps asserts on the machine.
        self._clock = clock
        self._samples: list[tuple[float, float]] = []
        self.lifetime_max = 0.0
        self._stop = anyio.Event()
        self.started = anyio.Event()
        """Set once a sample exists, so a waiter need not guess at startup."""
        self._sampled = anyio.Event()

    @property
    def window_max(self) -> float:
        cutoff = self._clock() - self._window
        self._samples = [(t, lag) for t, lag in self._samples if t >= cutoff]
        return max((lag for _, lag in self._samples), default=0.0)

    async def wait_for_sample(self) -> None:
        """Return once another sample lands.

        `start_soon` schedules a task; it does not run one. Anything that wants
        the monitor to have observed something has to wait for the observation,
        not for a duration.
        """
        self._sampled = anyio.Event()
        await self._sampled.wait()

    async def run(self) -> None:
        """Sample until stopped. Intended to run as a background task."""
        while not self._stop.is_set():
            t0 = self._clock()
            await anyio.sleep(self._interval)
            lag = self._clock() - t0 - self._interval
            if lag < 0:
                # A clock that went backwards, or a sleep that returned early.
                # Neither is a stall, and recording it would lower the maximum.
                continue
            self._samples.append((self._clock(), lag))
            if self.watchdog is not None:
                self.watchdog.beat()
            metrics.EVENT_LOOP_LAG_SAMPLES.observe(lag)
            if lag > self.lifetime_max:
                self.lifetime_max = lag
                # Wall clock, so the peak can be lined up against a pod event
                # or somebody else's log. `self._clock` is monotonic and means
                # nothing outside this process.
                metrics.EVENT_LOOP_LAG_MAX_AT.set(time.time())
                metrics.EVENT_LOOP_LAG_MAX.set(self.lifetime_max)
            metrics.EVENT_LOOP_LAG.set(self.window_max)
            self.started.set()
            self._sampled.set()

    def stop(self) -> None:
        self._stop.set()
