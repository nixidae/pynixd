"""Measure what a liveness probe experiences: time the loop ran nothing.

A TCP probe is answered by the loop accepting a connection, and `/healthz` by
the loop running a handler. Either way, a stretch of work that does not
suspend answers neither -- so the probe fails against a process that is busy
rather than wedged, and the restart destroys the evidence (nixidae/nixidae#432, #53).

`await` alone does not suspend. A coroutine that returns from a buffer
finishes without reaching the loop, and asyncio's `drain` returns straight
away below the high-water mark, so a read-and-write loop can run to the end of
a payload while every line of it looks like it awaits.

This mirrors `pynixd.health.LoopLagMonitor`, which is the production measure
of the same thing. It is separate so that a test measures the loop it runs on
rather than asserting against the process's own monitor.
"""

from __future__ import annotations

import time

import anyio


class LoopLag:
    """Record the longest gap between the loop running a ready callback.

    `max_lag_s` is what a probe sees. `SAMPLE_S` bounds the resolution: a stall
    shorter than it can be missed entirely.
    """

    SAMPLE_S = 0.005

    def __init__(self) -> None:
        self.max_lag_s = 0.0
        self.samples = 0
        self._stop = False

    async def run(self) -> None:
        while not self._stop:
            t0 = time.perf_counter()
            await anyio.sleep(self.SAMPLE_S)
            lag = time.perf_counter() - t0 - self.SAMPLE_S
            self.max_lag_s = max(self.max_lag_s, lag)
            self.samples += 1

    def stop(self) -> None:
        self._stop = True
