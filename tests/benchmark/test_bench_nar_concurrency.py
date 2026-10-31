"""What one client cannot show: the loop under N clients at once.

Every earlier measurement of a NAR transfer here used one client, and that is
why it concluded a push "does not starve the loop". With one client the whole
client pipeline -- NAR serialisation, hashing, syscalls, crypto -- costs about
as much per MiB as pynixd's forwarding does, so pynixd's reader waits on the
socket, the loop gets scheduled anyway, and lag stays near 20 ms whether the
transfer loop yields or not.

**A cluster never has one client.** Nodes pull closures while somebody pushes,
and nixlab2 reported `pynixd_event_loop_lag_max_seconds 7.912` against the
14-45 ms measured here. That gap is the thing this file exists to close.

The mechanism the single-client test cannot reach: with N transfers in
flight, the loop is never idle. A transfer that runs to completion without
suspending does not just delay a probe by its own duration -- it serializes
every other transfer behind it, so the stall a probe sees grows with N rather
than staying at one transfer's cost. The suspension is what interleaves them.

So `max_loop_lag_ms` against `clients` is the number to read, not
`cpu_ms_per_mib`. Measured on this machine, uvloop, 16 MiB per client:

    shape   clients=1   clients=4   clients=8
    push       3.6 ms     20.7 ms      73.3 ms
    pull       5.6 ms     34.0 ms     276.9 ms

**That 276.9 is a tail and not a measurement.** Three repeats of the same
cell gave 72.3, 100.7 and 59.2 ms. Take a median of several runs before
concluding anything, which is the same warning `test_bench_nar_profile.py`
carries and the same trap it fell into.

On medians, the per-chunk drain and checkpoint cost about 15% of lag and 5-10%
of throughput at this size (no-fix pull-8: 62.7, 55.6, 64.0 ms at 141 MiB/s;
with-fix: 72.3, 100.7, 59.2 ms at 121-141 MiB/s). **They are not a latency
optimisation at 16 MiB per client.** What they buy is a bounded transport
buffer and the guarantee that one large transfer cannot hold the loop, which
this size is too small to exercise -- a 16 MiB forward is about 100 ms of loop
time, so serialising eight of them is under a second either way.

Raise `_PER_CLIENT_KIB` to find the size where the guarantee starts paying.

**This still does not have what nixlab2 has.** Its store is on Ceph RBD, and
every store read there is a network round trip that a loopback test serves
from page cache. Treat a low figure here as "not reproduced", never as "does
not happen".
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

import anyio
import pytest
import structlog

from tests._conftest.all_of import all_of
from tests.conftest import (
    CLIENT_BIN,
    STORE_PREFIX,
    LoopLag,
    rmtree_robust,
    run_subproc,
    ssh_admin_uri,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from pynixd import Server

log = structlog.get_logger(__name__)

_MIB = 1024 * 1024

# Per client, and small on purpose: the question is how many transfers share
# the loop, not how big one is. Eight clients of 16 MiB is 128 MiB, the same
# total the few-large shape moves with two.
_PER_CLIENT_KIB = 16 * 1024
_CLIENT_COUNTS = (1, 4, 8)

_SRC = STORE_PREFIX / "nar-concurrency-src"
_CONTENT = STORE_PREFIX / "nar-concurrency-content"


@pytest.fixture
def conc_store() -> Iterator[Path]:
    rmtree_robust(_SRC)
    rmtree_robust(_CONTENT)
    _SRC.mkdir(parents=True, exist_ok=True)
    _CONTENT.mkdir(parents=True, exist_ok=True)
    yield _SRC
    rmtree_robust(_SRC)
    rmtree_robust(_CONTENT)


async def _seed(src: Path, count: int, size_kib: int) -> list[str]:
    """One distinct path per client, so no two clients push the same bytes."""
    paths: list[str] = []
    for i in range(count):
        blob = _CONTENT / f"conc-{i}"
        blob.write_bytes(os.urandom(size_kib * 1024))
        rc, stdout, stderr, _ = await run_subproc(
            [str(CLIENT_BIN), "store", "add-path", "--store", str(src), str(blob)],
        )
        assert rc == 0, f"add-path failed:\n{stderr}"
        paths.append(stdout.strip())
        blob.unlink()
    return paths


async def _run_concurrently(server: Server, label: str, clients: int, cmds: list[list[str]]) -> float:
    """Run every command at once and report what the loop did. Returns max lag in ms."""
    lag = LoopLag()
    wall0 = time.perf_counter()
    async with anyio.create_task_group() as tg:
        tg.start_soon(lag.run)
        try:
            results = await all_of(*(run_subproc(cmd) for cmd in cmds))
        finally:
            lag.stop()
    wall = time.perf_counter() - wall0

    for rc, _, stderr, _ in results:
        assert rc == 0, f"a client failed:\n{stderr}"

    sent_mib = clients * _PER_CLIENT_KIB / 1024
    max_lag_ms = lag.max_lag_s * 1000
    log.info(
        "bench_nar_concurrency",
        shape=label,
        clients=clients,
        sent_mib=round(sent_mib, 2),
        wall_s=round(wall, 3),
        # The number this file exists for. Compare it across `clients`: a loop
        # that interleaves holds it roughly flat, one that serializes does not.
        max_loop_lag_ms=round(max_lag_ms, 1),
        lag_per_client_ms=round(max_lag_ms / clients, 1),
        loop_samples=lag.samples,
        mib_per_s=round(sent_mib / wall, 1) if wall else None,
    )
    _ = server
    return max_lag_ms


@pytest.mark.benchmark
@pytest.mark.parametrize("clients", _CLIENT_COUNTS)
async def test_concurrent_pushes(pynixd_server: Server, conc_store: Path, clients: int) -> None:
    """N clients pushing at once, each its own path, through op 39.

    Each client runs `nix store add-path`, so every one of them is a separate
    `forward_framed` on the same loop.
    """
    blobs = []
    for i in range(clients):
        blob = _CONTENT / f"push-{i}"
        blob.write_bytes(os.urandom(_PER_CLIENT_KIB * 1024))
        blobs.append(blob)

    cmds = [[str(CLIENT_BIN), "store", "add-path", "--store", ssh_admin_uri(pynixd_server), str(b)] for b in blobs]
    await _run_concurrently(pynixd_server, "concurrent-push", clients, cmds)
    _ = conc_store


@pytest.mark.benchmark
@pytest.mark.parametrize("clients", _CLIENT_COUNTS)
async def test_concurrent_pulls(pynixd_server: Server, conc_store: Path, clients: int) -> None:
    """N clients pulling at once, through op 38.

    This is the shape a cluster makes when several pods start together, and it
    is the loop whose source is a local Unix socket -- always ready, so it
    reaches the event loop least often of the three.
    """
    paths = await _seed(conc_store, clients, _PER_CLIENT_KIB)

    # Put them into pynixd first; the pull is what is measured.
    rc, _, stderr, _ = await run_subproc(
        [
            str(CLIENT_BIN),
            "copy",
            "--no-check-sigs",
            "--from",
            str(_SRC),
            "--to",
            ssh_admin_uri(pynixd_server),
            *paths,
        ],
    )
    assert rc == 0, f"seeding copy failed:\n{stderr}"

    dsts = []
    for i in range(clients):
        dst = STORE_PREFIX / f"nar-concurrency-pull-{i}"
        rmtree_robust(dst)
        dst.mkdir(parents=True, exist_ok=True)
        dsts.append(dst)

    cmds = [
        [
            str(CLIENT_BIN),
            "copy",
            "--no-check-sigs",
            "--from",
            ssh_admin_uri(pynixd_server),
            "--to",
            str(dst),
            path,
        ]
        for dst, path in zip(dsts, paths, strict=True)
    ]
    try:
        await _run_concurrently(pynixd_server, "concurrent-pull", clients, cmds)
    finally:
        for dst in dsts:
            rmtree_robust(dst)
