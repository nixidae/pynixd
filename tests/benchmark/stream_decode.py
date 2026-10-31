"""Decode throughput for the daemon stderr/log stream.

The server profile of the system build is CPU-bound in the wire decode: one
`BuildPathsWithResults` streams about a million activity messages from the
managed daemon, and `logs.read_stream` accounts for almost all of the server's
wall time. This builds that stream -- N `LogStartActivity` records, each with
four integer fields, and a `LogNext` every fifth -- and times `read_stream`
over it.

`BytesReader` has no transport, so the number is the cost of the decode and
nothing else. Run it in a guest, where a container shares the host's CPU.
"""

from __future__ import annotations

import sys
import time

import anyio

import nix_daemon_protocol
from nix_daemon_protocol import PROTOCOL_VERSION
from nix_daemon_protocol.constants import STDERR_LAST, STDERR_NEXT, STDERR_START_ACTIVITY
from nix_daemon_protocol.context import ReadContext
from nix_daemon_protocol.io import BytesReader, BytesWriter
from nix_daemon_protocol.logs import read_stream

FIELDS = 4
"""Integer fields on each activity. The profile measured four."""

ACTIVITY_TYPE_REALISE = 102
"""`ActivityType.REALISE`, the one a build spends its time on."""


def build_stream(activities: int) -> bytes:
    w = BytesWriter()
    for i in range(activities):
        w.write_uint64(STDERR_START_ACTIVITY)
        w.write_uint64(i)  # act_id
        w.write_uint64(0)  # level
        w.write_uint64(ACTIVITY_TYPE_REALISE)
        w.write_string("building /nix/store/0000000000000000000000000000-x")
        w.write_uint64(FIELDS)
        for k in range(FIELDS):
            w.write_uint64(0)  # FieldType.INT
            w.write_uint64(k)
        w.write_uint64(0)  # parent
        if i % 5 == 0:
            w.write_uint64(STDERR_NEXT)
            w.write_string("  building '/nix/store/0000000000000000000000000000-x.drv'...")
    w.write_uint64(STDERR_LAST)
    return w.get_bytes()


async def count(data: bytes) -> int:
    ctx = ReadContext(reader=BytesReader(data), version=PROTOCOL_VERSION)
    n = 0
    async for _ in read_stream(ctx):
        n += 1
    return n


async def main() -> None:
    activities = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 7

    data = build_stream(activities)
    total = await count(data)
    await count(data)  # warm

    best: float | None = None
    for _ in range(rounds):
        start = time.perf_counter()
        await count(data)
        elapsed = time.perf_counter() - start
        best = elapsed if best is None else min(best, elapsed)

    assert best is not None
    print(f"module {nix_daemon_protocol.__file__}")
    print(f"activities={activities} messages={total} bytes={len(data)}")
    print(f"best={best * 1e3:.1f} ms  {best / total * 1e6:.1f} us/msg  {total / best:.0f} msg/s  ({rounds} rounds)")


if __name__ == "__main__":
    anyio.run(main, backend="asyncio")
