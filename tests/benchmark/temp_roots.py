"""How each way of writing temporary roots treats the event loop, on a directory the caller names.

Runs in a guest (`tests/derivations/temp-roots`) or by hand on any machine, as root when
`--freeze` is given. Each run is one approach, one caller pattern and one round. It
measures the longest stretch the event loop could not run, the latency of each root,
and the wall time, and checks that the roots file holds what was added.

Approaches:

- `inline`: `InlineRoots.add` (`temp_roots_reference`): the write runs on the event loop.
- `hop`: `InlineRoots.add_many([root])` for each root: one worker thread call each.
- `batch`: `InlineRoots.add_many(roots)`: one worker thread call for a set that arrives together.
- `writer`: `pynixd_store.temp_roots.RootWriter`, the shipped code: one thread per store does every
  file operation, and writes what it finds queued in one write per file.

Patterns:

- `build`: one session adds the roots of a build one at a time (7,177 on dynhetz, nixidae/nixidae#1519).
- `burst`: one session gets a set at once (3,344, the sg1 substitution that killed pynixd-0).
- `sessions`: concurrent sessions, each adding its own roots one at a time to its own file.

stdout: one JSON object per run, then a summary table on stderr. Run from the tree root as
`python -m tests.benchmark.temp_roots`. `--loop uvloop` is the default because pynixd runs on it
(`pynixd/__main__.py`).
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import enum
import json
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Final

import anyio
import uvloop

from pynixd_store.temp_roots import RootWriter, TempRoots
from tests.benchmark.temp_roots_reference import InlineRoots

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

BUILD_ROOTS: Final = 7177
BURST_ROOTS: Final = 3344
SESSIONS: Final = 16
SESSION_ROOTS: Final = 450
TICK: Final = 0.001
"""The loop-stall probe sleeps this long; a stall is how much later than that it woke."""


Roots = TempRoots | InlineRoots


class Approach(enum.StrEnum):
    INLINE = "inline"
    HOP = "hop"
    BATCH = "batch"
    WRITER = "writer"


class Loop(enum.StrEnum):
    ASYNCIO = "asyncio"
    UVLOOP = "uvloop"


class Pattern(enum.StrEnum):
    BUILD = "build"
    BURST = "burst"
    SESSIONS = "sessions"


def roots_for(prefix: str, count: int) -> list[str]:
    return [f"/nix/store/{index:032d}-{prefix}" for index in range(count)]


def read_roots(path: Path) -> list[str]:
    """The roots of a temporary roots file, as Nix's `findTempRoots` reads them."""
    return [part.decode() for part in path.read_bytes().split(b"\0") if part]


# ── Measuring ─────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class Result:
    scenario: str
    loop: Loop
    round_index: int
    approach: Approach
    pattern: Pattern
    roots: int
    wall_ms: float
    latency_p50_ms: float
    latency_p99_ms: float
    latency_max_ms: float
    stall_max_ms: float
    stall_p99_ms: float
    writer_passes: int | None


def _ms(seconds: float) -> float:
    return round(seconds * 1000, 3)


def _p99(values: Sequence[float]) -> float:
    if len(values) < 2:  # noqa: PLR2004 -- quantiles needs two points
        return max(values, default=0.0)
    return statistics.quantiles(values, n=100, method="inclusive")[98]


class Stalls:
    """A task that sleeps `TICK` again and again and records how late it woke."""

    def __init__(self) -> None:
        self.stalls: list[float] = []
        self._stop = anyio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            before = time.monotonic()
            await anyio.sleep(TICK)
            self.stalls.append(time.monotonic() - before - TICK)

    def stop(self) -> None:
        self._stop.set()


async def _one_at_a_time(add: Callable[[str], Awaitable[bool]], roots: list[str], latencies: list[float]) -> None:
    for root in roots:
        started = time.monotonic()
        if not await add(root):
            raise RuntimeError(f"root not held: {root}")
        latencies.append(time.monotonic() - started)


def _adder(approach: Approach, store: Roots) -> Callable[[str], Awaitable[bool]]:
    if approach is Approach.HOP:
        return lambda root: store.add_many([root])
    return store.add


async def _burst(approach: Approach, store: Roots, roots: list[str], latencies: list[float]) -> None:
    if approach in {Approach.BATCH, Approach.WRITER}:
        started = time.monotonic()
        if not await store.add_many(roots):
            raise RuntimeError("burst not held")
        latencies.append(time.monotonic() - started)
        return
    await _one_at_a_time(_adder(approach, store), roots, latencies)


def applies(approach: Approach, pattern: Pattern) -> bool:
    """`batch` is a way to add a set that arrives together; a caller with one root has nothing to batch."""
    return approach is not Approach.BATCH or pattern is Pattern.BURST


async def measure(  # noqa: PLR0913 -- one run's coordinates
    scenario: str, loop: Loop, round_: int, approach: Approach, pattern: Pattern, state: Path, freeze: Freeze | None
) -> Result:
    async with contextlib.AsyncExitStack() as stack:
        writer = await stack.enter_async_context(RootWriter(state)) if approach is Approach.WRITER else None
        return await _measure_with(scenario, loop, round_, approach, pattern, state, freeze, writer)


async def _measure_with(  # noqa: PLR0913 -- one run's coordinates
    scenario: str,
    loop: Loop,
    round_: int,
    approach: Approach,
    pattern: Pattern,
    state: Path,
    freeze: Freeze | None,
    writer: RootWriter | None,
) -> Result:
    def make() -> Roots:
        return writer.session() if writer is not None else InlineRoots(state)

    if pattern is Pattern.SESSIONS:
        stores = [make() for _ in range(SESSIONS)]
        expected = [roots_for(f"s{index}", SESSION_ROOTS) for index in range(SESSIONS)]
    else:
        stores = [make()]
        expected = [roots_for("b", BUILD_ROOTS if pattern is Pattern.BUILD else BURST_ROOTS)]

    latencies: list[float] = []
    stalls = Stalls()
    try:
        if freeze is not None:
            # Before the probe starts: `fsfreeze --freeze` syncs the filesystem, which takes the
            # loop ~100 ms that no approach caused.
            freeze.start()
        async with anyio.create_task_group() as tg:
            tg.start_soon(stalls.run)
            await anyio.sleep(0.02)
            started = time.monotonic()
            if pattern is Pattern.BUILD:
                await _one_at_a_time(_adder(approach, stores[0]), expected[0], latencies)
            elif pattern is Pattern.BURST:
                await _burst(approach, stores[0], expected[0], latencies)
            else:
                async with anyio.create_task_group() as sessions:
                    for store, roots in zip(stores, expected, strict=True):
                        sessions.start_soon(_one_at_a_time, _adder(approach, store), roots, latencies)
            wall = time.monotonic() - started
            await anyio.sleep(0.02)
            stalls.stop()
        for store, roots in zip(stores, expected, strict=True):
            found = read_roots(store.path)
            if found != roots:
                raise RuntimeError(
                    f"{approach}/{pattern}: {store.path} holds {len(found)} roots, expected {len(roots)}"
                )
    finally:
        if freeze is not None:
            freeze.end()
        for store in stores:
            await store.close()

    return Result(
        scenario=scenario,
        loop=loop,
        round_index=round_,
        approach=approach,
        pattern=pattern,
        roots=sum(len(roots) for roots in expected),
        wall_ms=_ms(wall),
        latency_p50_ms=_ms(statistics.median(latencies)),
        latency_p99_ms=_ms(_p99(latencies)),
        latency_max_ms=_ms(max(latencies)),
        stall_max_ms=_ms(max(stalls.stalls)),
        stall_p99_ms=_ms(_p99(stalls.stalls)),
        writer_passes=writer.passes if writer is not None else None,
    )


class Freeze:
    """Freeze a filesystem for a fixed time, thawed by a process of its own.

    The thaw is started before the freeze: an approach that writes on the event loop blocks
    the whole program until the thaw, so the program itself could never thaw it.
    """

    def __init__(self, mount: Path, seconds: float) -> None:
        self.mount = mount
        self.seconds = seconds
        self._thaw: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        self._thaw = subprocess.Popen(  # noqa: S603 -- fixed argv
            ["/bin/sh", "-c", f"sleep {self.seconds}; fsfreeze --unfreeze {self.mount}"]  # noqa: S607 -- the guest's sh
        )
        subprocess.run(["fsfreeze", "--freeze", str(self.mount)], check=True)  # noqa: S603, S607 -- PATH's util-linux

    def end(self) -> None:
        if self._thaw is not None:
            self._thaw.wait()
            self._thaw = None
        # Already thawed in the normal case; this is for a run that failed before the thaw ran.
        subprocess.run(["fsfreeze", "--unfreeze", str(self.mount)], check=False, capture_output=True)  # noqa: S603, S607


def summary(results: Sequence[Result]) -> str:
    rows = [
        f"{'scenario':10} {'pattern':9} {'approach':8} {'wall ms':>9} {'p50 us':>8} {'p99 us':>9} {'max ms':>8} "
        f"{'stall max ms':>12} {'stall p99 ms':>12}"
    ]
    keys = sorted(
        {(r.scenario, r.pattern, r.approach) for r in results}, key=lambda k: (k[0], k[1], list(Approach).index(k[2]))
    )
    for scenario, pattern, approach in keys:
        runs = [r for r in results if (r.scenario, r.pattern, r.approach) == (scenario, pattern, approach)]
        rows.append(
            f"{scenario:10} {pattern:9} {approach:8} "
            f"{statistics.median(r.wall_ms for r in runs):9.1f} "
            f"{statistics.median(r.latency_p50_ms for r in runs) * 1000:8.1f} "
            f"{statistics.median(r.latency_p99_ms for r in runs) * 1000:9.1f} "
            f"{max(r.latency_max_ms for r in runs):8.1f} "
            f"{max(r.stall_max_ms for r in runs):12.2f} "
            f"{statistics.median(r.stall_p99_ms for r in runs):12.2f}"
        )
    return "\n".join(rows)


async def main(args: argparse.Namespace) -> None:
    freeze = Freeze(args.freeze, args.freeze_seconds) if args.freeze is not None else None
    approaches = [Approach(name) for name in args.approaches]
    patterns = [Pattern(name) for name in args.patterns]
    results: list[Result] = []
    for round_ in range(args.rounds):
        # Rotate the order each round, so a slow moment of the host lands on a different approach.
        order = approaches[round_ % len(approaches) :] + approaches[: round_ % len(approaches)]
        for pattern in patterns:
            for approach in order:
                if not applies(approach, pattern):
                    continue
                state = args.dir / f"{args.scenario}-{round_}-{pattern}-{approach}"
                state.mkdir(parents=True)
                try:
                    result = await measure(args.scenario, args.loop, round_, approach, pattern, state, freeze)
                finally:
                    shutil.rmtree(state, ignore_errors=True)
                results.append(result)
                print(json.dumps(dataclasses.asdict(result)), flush=True)
    print(summary(results), file=sys.stderr)


def parse(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dir", type=Path, required=True, help="where the state directories go")
    parser.add_argument("--scenario", required=True, help="a label for the results")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--approaches", nargs="+", default=list(Approach), choices=list(Approach))
    parser.add_argument("--patterns", nargs="+", default=list(Pattern), choices=list(Pattern))
    parser.add_argument("--freeze", type=Path, help="freeze this mount at the start of each run (needs root)")
    parser.add_argument("--freeze-seconds", type=float, default=2.0)
    parser.add_argument("--loop", type=Loop, default=Loop.UVLOOP, choices=list(Loop))
    return parser.parse_args(argv)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        args = parse(sys.argv[1:])
        options = {"loop_factory": uvloop.new_event_loop} if args.loop is Loop.UVLOOP else {}
        anyio.run(main, args, backend="asyncio", backend_options=options)
