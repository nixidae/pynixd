"""Stall a loop inside one C call with the GIL dump armed; the dump goes to argv[1].

A child process of `test_stall_watchdog`, so no thread of another test can
exit during the dump (`StallWatchdog` says why that crashes).
"""

from __future__ import annotations

import sys
from pathlib import Path

import anyio

from pynixd.health import LoopLagMonitor, StallWatchdog


def _hold_the_gil() -> None:
    # One C call, about 0.5 s on dynhetz, with the GIL held throughout: no
    # other Python thread runs until it returns.
    sum(range(50_000_000))


async def main(log: Path) -> None:
    with log.open("w") as out:
        dog = StallWatchdog(threshold=0.2, interval=0.05, stream=out, gil_dump=True)
        monitor = LoopLagMonitor(interval=0.02, watchdog=dog)
        thread = dog.start()
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(monitor.run)
                await monitor.wait_for_sample()
                _hold_the_gil()
                monitor.stop()
                tg.cancel_scope.cancel()
        finally:
            dog.stop()
            thread.join(timeout=5)


if __name__ == "__main__":
    anyio.run(main, Path(sys.argv[1]))
