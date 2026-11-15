"""Stall a loop with the GIL dump armed; argv: the log, the stall to make.

A child process of `test_stall_watchdog`, so the dump's SIGALRM and the
child's threads meet no other test. Each stall prints a line to stdout
that the test reads back.

- `c-call`: one C call holds the GIL past the threshold.
- `churn`: many short C-call stalls while 20 threads start and exit.
- `gc`: one full collection over a large heap.
- `syscall`: a sleep with the GIL released; SIGALRM must not cut it short.
"""

from __future__ import annotations

import gc
import sys
import threading
import time
from pathlib import Path

import anyio

from pynixd.health import LoopLagMonitor, StallWatchdog


def _hold_the_gil(n: int = 50_000_000) -> None:
    # One C call, about 0.5 s on dynhetz at the default, with the GIL held
    # throughout: no other Python thread runs until it returns.
    sum(range(n))


async def _c_call(dog: StallWatchdog) -> None:
    monitor = LoopLagMonitor(interval=0.02, watchdog=dog)
    async with anyio.create_task_group() as tg:
        tg.start_soon(monitor.run)
        await monitor.wait_for_sample()
        _hold_the_gil()
        monitor.stop()
        tg.cancel_scope.cancel()


def _churn(dog: StallWatchdog, stalls: int) -> None:
    stop = threading.Event()

    def spin() -> None:
        while not stop.is_set():
            t = threading.Thread(target=lambda: None)
            t.start()
            t.join()

    spinners = [threading.Thread(target=spin) for _ in range(20)]
    for t in spinners:
        t.start()
    try:
        for _ in range(stalls):
            dog.beat()
            _hold_the_gil(300_000)  # a few ms, past the 1 ms threshold
    finally:
        stop.set()
        for t in spinners:
            t.join()
    print(f"survived {stalls} stalls")


def _gc() -> None:
    heap = [{"n": n} for n in range(3_000_000)]
    gc.collect()
    print(f"collected over {len(heap)} objects")


def _syscall(dog: StallWatchdog) -> None:
    dog.beat()
    started = time.monotonic()
    time.sleep(0.3)
    print(f"slept {time.monotonic() - started:.3f}s")


async def main(log: Path, mode: str) -> None:
    with log.open("w") as out:
        threshold = 0.001 if mode == "churn" else 0.05
        dog = StallWatchdog(threshold=threshold, interval=3600, stream=out, gil_dump=True)
        thread = dog.start()
        try:
            match mode:
                case "c-call":
                    await _c_call(dog)
                case "churn":
                    _churn(dog, 2000)
                case "gc":
                    _gc()
                case "syscall":
                    _syscall(dog)
                case _:
                    raise SystemExit(f"unknown stall {mode!r}")
        finally:
            dog.stop()
            thread.join(timeout=5)
    print(f"long collections {dog.long_collections}")


if __name__ == "__main__":
    anyio.run(main, Path(sys.argv[1]), sys.argv[2])
