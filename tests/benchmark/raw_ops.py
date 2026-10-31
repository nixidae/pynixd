"""Raw daemon-protocol throughput, through each daemon.

The client is pynixd's own `LocalSocketStore`, so its Python cost is present in
both runs and cancels in the ratio. That is the whole point of the comparison:
the only thing that differs is the daemon on the far end of the socket.

`sys.argv`: the count, then the two sockets -- nix-daemon first, pynixd second --
then which socket to pump: `both`, `first` or `second`. The pump runs `REPEATS`
times and reports the best whole pass: the container shares the host's CPU, so
a single pass carries whatever else the host was doing, and the best pass is
one real pass rather than the fastest column of each.
"""

from __future__ import annotations

import random
import sys
import time
from pathlib import Path

import anyio

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.config import LocalSocketStoreSpec
from pynixd.serde import AddTempRootRequest, IsValidPathRequest, StorePath
from pynixd.store import LocalSocketStore

ALPHABET = "0123456789abcdfghijklmnpqrsvwxyz"

REPEATS = 3
"""How many times each pump runs. The best whole pass is the one reported."""


def paths(n: int) -> list[StorePath]:
    rng = random.Random(0)
    made = []
    for i in range(n):
        digest = "".join(rng.choice(ALPHABET) for _ in range(32))
        made.append(StorePath(path=f"/nix/store/{digest}-rawbench-{i}"))
    return made


def spec(socket_path: Path) -> LocalSocketStoreSpec:
    return LocalSocketStoreSpec(
        store_id=LOCAL_STORE_ID,
        store_path=Path("/"),
        socket_path=socket_path,
        managed=False,
        probe=False,
        monitor=False,
    )


async def one_pass(socket_path: Path, sample: list[StorePath], tag: str) -> tuple[float, tuple[float, float, float]]:
    """One IsValidPath, one AddTempRoot, and one AddToStore pass.

    Answers the total seconds with the three rates. The AddToStore sample is
    smaller: each call writes a real store path, and the pass is the fixed
    per-operation cost that the system build pays, not the bulk rate of a
    large NAR. `tag` names the pass, so every pass writes paths no earlier
    pass wrote: the second pass must not measure the first pass's dedup.
    """
    async with LocalSocketStore(spec(socket_path)) as client:
        await client.execute(IsValidPathRequest(path=sample[0]))
        await client.execute(AddTempRootRequest(path=sample[0]))

        start = time.perf_counter()
        for p in sample:
            await client.execute(IsValidPathRequest(path=p))
        middle = time.perf_counter()
        for p in sample:
            await client.execute(AddTempRootRequest(path=p))
        store_start = time.perf_counter()
        texts = sample[: max(1, len(sample) // 10)]
        for i, p in enumerate(texts):
            await client.add_text_to_store(f"rawbench-{tag}-{i}", f"rawbench payload {p}", set())
        end = time.perf_counter()

        n = len(sample)
        return (
            end - start,
            (
                n / (middle - start),
                n / (store_start - middle),
                len(texts) / (end - store_start),
            ),
        )


async def measure(socket_path: Path, sample: list[StorePath], label: str) -> None:
    best_time = float("inf")
    best = (0.0, 0.0, 0.0)
    for repeat in range(REPEATS):
        elapsed, rates = await one_pass(socket_path, sample, f"{label}-{repeat}")
        if elapsed < best_time:
            best_time = elapsed
            best = rates

    n = len(sample)
    print(
        f"{label:10} n={n}  isvalidpath {best[0]:>8.0f} op/s"
        f"   addtemproot {best[1]:>8.0f} op/s"
        f"   addtostore {best[2]:>8.0f} op/s",
        flush=True,
    )


async def main() -> None:
    sample = paths(int(sys.argv[1]))
    only = sys.argv[4] if len(sys.argv) > 4 else "both"
    if only in ("both", "first"):
        await measure(Path(sys.argv[2]), sample, "nix-daemon")
    if only in ("both", "second"):
        await measure(Path(sys.argv[3]), sample, "pynixd")


if __name__ == "__main__":
    anyio.run(main, backend="asyncio")
