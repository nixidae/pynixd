"""Loopback throughput of the transport reader/writer pair, as a script.

Baseline for the connection-bytes investigation: how many small messages
and how many payload bytes one `UnixNixReader` / `UnixNixWriter` pair
moves per second over a unix socket, before any counting lands on the
transport boundary. The after-change run repeats this script unchanged;
a difference in the numbers is the price of the counters.

A script and not a pytest: `tests/_conftest` boots a session pynixd
server for every pytest run, which needs cgroups the host session does
not grant -- and the numbers have to come from this shared machine
anyway, not from a guest VM. Run it straight:

    nix develop --file . --command 'python3 tests/benchmark/wire_throughput.py'

Unix only, on purpose: the two transports share the algorithm and differ
only in the underlying read call and the readahead size, so the added
lines cost the same constant per transport read on both. Unix is the
fastest transport, which makes it the harshest relative measure -- the
same absolute cost over a smaller base. SSH can only price cheaper.

Small messages drain per message: the adversarial case for anything
counted per transport write, and the shape of a chatty op. Large
payloads drain once at the end: the bulk shape of a NAR move.

Baseline, shared machine, medians of 3 runs of 3 iters (2026-10-07):

    case         loop      msgs/s    MiB/s
    write_small  asyncio     17,326        0.1
    write_small  uvloop   1,253,703        9.6
    read_small   asyncio  3,038,662       23.2
    read_small   uvloop   3,055,684       23.3
    write_large  asyncio      1,210    1,209.8
    write_large  uvloop       1,287    1,287.1
    read_large   asyncio      1,596    1,595.6
    read_large   uvloop       1,912    1,911.6
"""

from __future__ import annotations

import asyncio
import contextlib
import resource
import statistics
import sys
import tempfile
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import anyio
import structlog
from anyio.abc import SocketStream

# The working copies in front of the environment, like the shell hook does
# with `$PWD` -- but this script also runs where `PWD` is unset (a systemd
# unit), so it anchors on its own location instead.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "nix-daemon-protocol" / "src"))

from pynixd.unix_socket import UnixLink, as_link  # noqa: E402
from pynixd.wire import UnixNixReader, UnixNixWriter  # noqa: E402

log = structlog.get_logger(__name__)

_MIB = 1024 * 1024

SMALL_COUNT = 100_000
LARGE_COUNT = 64
LARGE_SIZE = _MIB
ITERS = 3


async def _blackhole(stream: SocketStream, total: int) -> None:
    """Read and discard exactly *total* bytes."""
    got = 0
    while got < total:
        try:
            chunk = await stream.receive(64 * 1024)
        except anyio.EndOfStream:
            raise EOFError(f"peer closed after {got} of {total} bytes") from None
        got += len(chunk)


@contextlib.asynccontextmanager
async def _serving(sock: Path, handler: Callable[[SocketStream], Awaitable[None]]) -> AsyncIterator[UnixLink]:
    """A unix-socket server that runs *handler* on one connection; yields the client's end."""
    listener = await anyio.create_unix_listener(sock)

    async def _one() -> None:
        async with await listener.accept() as stream:
            await handler(stream)

    async with listener, anyio.create_task_group() as tg:
        tg.start_soon(_one)
        link = await as_link(await anyio.connect_unix(sock))
        async with link.stream:
            yield link


def _cpu() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return r.ru_utime + r.ru_stime


async def _write_small(sock: Path) -> tuple[int, int]:
    """100k 8-byte messages, drained per message. Returns (msgs, payload bytes)."""
    total = SMALL_COUNT * 8
    async with _serving(sock, lambda s: _blackhole(s, total)) as client:
        writer = UnixNixWriter(client)
        for _ in range(SMALL_COUNT):
            writer.write_uint64(1)
            await writer.drain()
    return SMALL_COUNT, total


async def _read_small(sock: Path) -> tuple[int, int]:
    """100k 8-byte messages read one `read_uint64` at a time."""
    blob = (1).to_bytes(8, "little") * SMALL_COUNT

    async def _pump(stream: SocketStream) -> None:
        await stream.send(blob)

    async with _serving(sock, _pump) as client:
        reader = UnixNixReader(client)
        for _ in range(SMALL_COUNT):
            assert await reader.read_uint64() == 1
    return SMALL_COUNT, len(blob)


async def _write_large(sock: Path) -> tuple[int, int]:
    """64 1 MiB payloads through `write_bytes`, one drain at the end."""
    total = LARGE_COUNT * LARGE_SIZE
    async with _serving(sock, lambda s: _blackhole(s, total)) as client:
        writer = UnixNixWriter(client)
        payload = bytes(LARGE_SIZE)
        for _ in range(LARGE_COUNT):
            writer.write_bytes(payload)
        await writer.drain()
    return LARGE_COUNT, total


async def _read_large(sock: Path) -> tuple[int, int]:
    """64 1 MiB payloads read one `read_bytes` at a time."""

    async def _pump(stream: SocketStream) -> None:
        for _ in range(LARGE_COUNT):
            await stream.send(LARGE_SIZE.to_bytes(8, "little") + bytes(LARGE_SIZE))

    async with _serving(sock, _pump) as client:
        reader = UnixNixReader(client)
        for _ in range(LARGE_COUNT):
            data = await reader.read_bytes()
            assert len(data) == LARGE_SIZE
    return LARGE_COUNT, LARGE_COUNT * LARGE_SIZE


_CASES: dict[str, Callable[[Path], Awaitable[tuple[int, int]]]] = {
    "write_small": _write_small,
    "read_small": _read_small,
    "write_large": _write_large,
    "read_large": _read_large,
}


async def _one(loop: str, case: str, tmp: Path) -> dict[str, float]:
    """One timed run of one case. Returns the measured rates."""
    t0, c0 = time.perf_counter(), _cpu()
    msgs, payload = await _CASES[case](tmp / f"{loop}-{case}.sock")
    wall, cpu = time.perf_counter() - t0, _cpu() - c0
    return {
        "msgs_per_s": msgs / wall,
        "mib_per_s": payload / _MIB / wall,
        "cpu_s": cpu,
    }


async def _amain(iters: int) -> dict[str, dict[str, list[float]]]:
    """Every case, *iters* timed runs. Returns all samples."""
    samples: dict[str, dict[str, list[float]]] = {}
    with tempfile.TemporaryDirectory(prefix="wire-throughput-") as tmp:
        for case in _CASES:
            samples[case] = {"msgs_per_s": [], "mib_per_s": []}
            for _ in range(iters):
                rates = await _one(asyncio.get_running_loop().__class__.__name__, case, Path(tmp))
                samples[case]["msgs_per_s"].append(rates["msgs_per_s"])
                samples[case]["mib_per_s"].append(rates["mib_per_s"])
                log.info("bench_wire_throughput", case=case, **{k: round(v, 2) for k, v in rates.items()})
    return samples


def main(iters: int) -> None:
    """Run every case under asyncio and uvloop, then print the medians."""
    all_samples: dict[str, dict[str, dict[str, list[float]]]] = {}
    for loop, use_uvloop in (("asyncio", False), ("uvloop", True)):
        all_samples[loop] = anyio.run(_amain, iters, backend="asyncio", backend_options={"use_uvloop": use_uvloop})
    print(f"{'case':<12} {'loop':<8} {'msgs/s':>12} {'MiB/s':>10}")  # noqa: T201
    for case in _CASES:
        for loop in ("asyncio", "uvloop"):
            med_msgs = statistics.median(all_samples[loop][case]["msgs_per_s"])
            med_mib = statistics.median(all_samples[loop][case]["mib_per_s"])
            print(f"{case:<12} {loop:<8} {med_msgs:>12,.0f} {med_mib:>10,.1f}")  # noqa: T201


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else ITERS)
