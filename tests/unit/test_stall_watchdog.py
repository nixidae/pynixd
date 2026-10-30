"""The watchdog that catches a stall in the act.

A push on nixlab2 sat at 100% CPU for about ten minutes with no progress, then
resumed. Nothing recorded what it was doing: a probe restarts the container,
the restart clears the lifetime-max gauge, and no benchmark here has ever
reproduced it. The transfer fixes cannot be the cause either, because only
42.9 MiB moved in total -- the cost is not proportional to the bytes.

So the evidence has to be taken from inside the process while it is stuck.
The last test is the one that matters: it stalls a real event loop with a
real CPU-bound Python loop and asserts a dump came out. Everything the
watchdog claims rests on the GIL being released during that stall, and only
running it proves that.
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import os
import sys
import threading
import time
from pathlib import Path

import anyio

from pynixd.health import LoopLagMonitor, StallWatchdog


def test_a_beating_loop_is_never_dumped() -> None:
    dog = StallWatchdog(threshold=1.0, stream=io.StringIO())
    now = time.monotonic()
    dog.beat()

    assert dog._check(now + 0.5) is False
    assert dog.dumps == 0


def test_a_stalled_loop_is_dumped_once() -> None:
    """One dump per stall. A ten-minute stall must not write hundreds."""
    out = io.StringIO()
    dog = StallWatchdog(threshold=1.0, stream=out)
    dog.beat()
    now = time.monotonic()

    assert dog._check(now + 2.0) is True
    dog.dump()
    # Still stalled, still past the threshold, and already reported.
    assert dog._check(now + 3.0) is False
    assert dog._check(now + 600.0) is False

    dog.beat()
    assert dog._check(time.monotonic() + 2.0) is True


def test_zero_disables_it() -> None:
    dog = StallWatchdog(threshold=0.0, stream=io.StringIO())
    dog.beat()
    assert dog._check(time.monotonic() + 3600.0) is False


def test_the_dump_also_goes_to_a_file(tmp_path: Path) -> None:
    """stderr alone is not a sink for this.

    The stall this catches usually ends in a liveness restart, which races the
    log shipper, and a pod deleted and recreated takes the previous
    container's log with it. An instrument whose only sink is destroyed by the
    event it records is not an instrument.
    """
    sink = tmp_path / "state" / "stall.log"
    out = io.StringIO()
    dog = StallWatchdog(threshold=0.01, stream=out, path=sink)
    dog.dump()

    assert sink.exists(), "the dump did not reach the file"
    text = sink.read_text()
    assert "has not run a callback" in text
    # A wall clock time, so a dump can be lined up against a pod event. The
    # stall duration alone cannot be.
    assert text.lstrip().startswith("pynixd: 2")
    # Both sinks, not one or the other.
    assert "has not run a callback" in out.getvalue()

    dog.beat()
    dog.dump()
    assert sink.read_text().count("has not run a callback") == 2, "the second dump replaced the first"
    assert dog.sink_errors == 0


def test_a_sink_it_cannot_write_never_raises(tmp_path: Path) -> None:
    """A dump that cannot be filed must not take down what it reports on."""
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")
    out = io.StringIO()
    dog = StallWatchdog(threshold=0.01, stream=out, path=blocker / "stall.log")

    dog.dump()

    assert dog.dumps == 1, "the dump itself must still count"
    assert dog.sink_errors == 1
    assert "could not write the stall dump" in out.getvalue()


def test_a_full_sink_stops_growing_and_says_so(tmp_path: Path) -> None:
    """A process that stalls for months must not fill the store's volume.

    And it must not go quiet about it. A dump that stopped landing looks
    exactly like a stall that never happened, which is the fault that writing
    to stderr alone had. The refusal names the path, once.
    """
    sink = tmp_path / "stall.log"
    sink.write_bytes(b"x" * StallWatchdog.MAX_SINK_BYTES)
    out = io.StringIO()
    dog = StallWatchdog(threshold=0.01, stream=out, path=sink)

    dog.dump()

    assert sink.stat().st_size == StallWatchdog.MAX_SINK_BYTES
    assert dog.sink_errors == 0
    assert dog.sink_skipped == 1
    assert str(sink) in out.getvalue()
    assert "further dumps go to stderr only" in out.getvalue()
    # The dump itself still reached stderr; only the file copy was refused.
    assert "has not run a callback" in out.getvalue()

    dog.beat()
    dog.dump()
    assert dog.sink_skipped == 2
    assert out.getvalue().count("further dumps go to stderr only") == 1, "the notice repeated"


def test_the_cap_is_configurable(tmp_path: Path) -> None:
    """A constant can only be changed by a release.

    Whoever points the sink at a volume is the person who knows what that
    volume can spare, and it is not whoever wrote the default.
    """
    sink = tmp_path / "stall.log"
    out = io.StringIO()
    dog = StallWatchdog(threshold=0.01, stream=out, path=sink, max_bytes=64)

    dog.dump()
    first = sink.stat().st_size
    assert first > 64, "the first dump is never refused, whatever the cap"

    dog.beat()
    dog.dump()
    assert sink.stat().st_size == first, "the second dump was not refused"
    assert dog.sink_skipped == 1
    assert "64 bytes" in out.getvalue()


def test_zero_removes_the_cap(tmp_path: Path) -> None:
    sink = tmp_path / "stall.log"
    dog = StallWatchdog(threshold=0.01, stream=io.StringIO(), path=sink, max_bytes=0)

    dog.dump()
    dog.beat()
    dog.dump()

    assert dog.sink_skipped == 0
    assert sink.read_text().count("has not run a callback") == 2


def test_the_cap_keeps_the_first_dumps(tmp_path: Path) -> None:
    """Not the last. Rotation would throw away the one that explains the rest.

    The first stall after a deployment is usually the one worth having, so the
    file fills and then refuses rather than rolling.
    """
    sink = tmp_path / "stall.log"
    dog = StallWatchdog(threshold=0.01, stream=io.StringIO(), path=sink, max_bytes=64)
    dog.dump()
    first = sink.read_text()

    for _ in range(3):
        dog.beat()
        dog.dump()

    assert sink.read_text() == first, "a later dump displaced the first"


def test_the_dump_names_the_stalled_thread() -> None:
    out = io.StringIO()
    dog = StallWatchdog(threshold=0.01, stream=out)
    dog.dump()
    text = out.getvalue()

    assert "has not run a callback" in text
    # The stack itself, which is what carries the diagnosis.
    assert "Thread" in text or "File " in text


async def test_it_fires_against_a_real_cpu_bound_stall() -> None:
    """The claim that matters, run rather than argued.

    The watchdog is a thread, and a thread only helps if it is scheduled while
    the loop is not. CPU-bound Python releases the GIL every switch interval,
    so it is -- but that is the whole premise, and a stall inside a C call that
    holds the GIL would defeat it. This is the negative control for the
    premise.
    """
    out = io.StringIO()
    dog = StallWatchdog(threshold=0.2, interval=0.05, stream=out)
    monitor = LoopLagMonitor(interval=0.02, watchdog=dog)
    dog.beat()
    thread = dog.start()
    task = asyncio.create_task(monitor.run())
    try:
        await monitor.wait_for_sample()
        assert dog.dumps == 0, "a loop that is running must not be dumped"

        # Block the loop the way the reported failure does: Python, on the
        # loop, without awaiting.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            sum(range(2000))
    finally:
        monitor.stop()
        dog.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        thread.join(timeout=5)

    assert dog.dumps >= 1, "a one second CPU-bound stall produced no dump"
    assert "has not run a callback" in out.getvalue()
    assert threading.current_thread() is threading.main_thread()


async def test_a_c_call_holding_the_gil_is_named_while_it_runs(tmp_path: Path) -> None:
    """The watchdog thread cannot run during such a call; `faulthandler` can.

    101 of 107 dumps on dynhetz showed the loop idle, because the Python
    thread dumped after the call returned. nixidae/nixidae#1139. In a child
    process: a thread of another test that exits during the dump segfaults
    the suite (nixidae/nixidae#1143).
    """
    log = tmp_path / "stall.log"
    child = Path(__file__).with_name("stall_gil_child.py")
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    result = await anyio.run_process([sys.executable, str(child), str(log)], env=env, check=False)

    assert result.returncode == 0, result.stderr.decode()
    assert "_hold_the_gil" in log.read_text(), "no dump named the call while it ran"


def test_the_gil_dump_is_off_by_default() -> None:
    dog = StallWatchdog(threshold=0.2, interval=0.05, stream=sys.stderr)
    thread = dog.start()
    try:
        assert dog._fault_stream is None
    finally:
        dog.stop()
        thread.join(timeout=5)
