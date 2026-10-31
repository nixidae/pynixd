"""CPU and memory profile of the server-side NAR forward path.

This drives `AddMultipleToStore._forward_stream`, the handler a client reaches
with `nix copy --to ssh-ng://...`. That is the path a nixkube node uses, and it
is the one that pegged a cluster at 999 millicores and 1850 MB.

`test_bench_nar.py` does not cover it. Every test there drives
`stream_paths_store_to_store`, which is pynixd acting as a *client* against
another store -- a different loop, with a different drain pattern.

Three shapes, because they fail differently. Many small paths put the cost in
per-path Python work, so read `cpu_ms_per_path`. Few large paths put it in the
byte loop, so read `peak_mib` against `sent_mib`: a streaming forward holds
roughly one chunk, and a buffering one holds the whole payload. One path of
many files varies what is *inside* a NAR, which the forward never parses -- it
measured about twice the per-byte cost of few-large, so it does not explain
nixidae/nixidae#487's core-pegged-with-no-progress.

The metrics are reported, not asserted. Bounds belong here once the numbers are
known -- a guessed threshold either passes forever or fails on a slower runner.
What is asserted is that the copy succeeded and the paths arrived, so the
measurement is of a transfer that really happened.

`tracemalloc` inflates CPU somewhat, so compare `cpu_ms_per_mib` between shapes
in one run rather than against a production figure. `peak_mib` is the number
that carries the diagnosis and it is unaffected.

Every test here is also sampled by the autouse `profiler` fixture, which writes
`pyinstrument.txt` beside the test's log directory. That is where the attributed
hotspot is.

What it attributed, on the few-large shape, 128 MiB, 3.435s of CPU:

    3.073  AddMultipleToStoreHandler._forward_stream
    2.992    FramedReader.readexactly          <- 97% of the handler
    1.450      SSHNixReader.readexactly
    1.172      SSHNixReader.read_uint64        <- one 8-byte read per frame

**The cost is on the read side, not the write side.** `FramedReader.readexactly`
copies each byte three to four times: `_buf.extend(data)` into the accumulator,
then a slice and a `bytes()` out of it, then `_compact`'s `del _buf[:pos]`. Over
128 MiB that is about half a gigabyte of memcpy. `read_uint64` is the frame
length prefix, taken as its own socket read per frame.

Adding backpressure to the write loop moved `peak_over_sent` on few-large from
0.979 to 0.267 and left CPU unchanged at 3.07s, which is the same conclusion
from the other direction.

**`max_loop_lag_ms` is the number nixidae/nixidae#432 and #53 turn on**, because a TCP
liveness probe is answered by the loop accepting a connection. `checkpoint()`
in the two transfer loops took the worst shape from 214.6 ms to 54.4 ms for no
measurable CPU.

What the residual 45-90 ms is *not*, both measured rather than reasoned:

    subprocess spawn   ~9 ms   (scratch probe, /bin/true on this loop)
    gc pause           ~2 ms   (`max_gc_pause_ms` below, all three shapes)

A gen-2 collection *can* stall this loop for 503 ms with two million live
objects, so the collector stays worth watching as allocation grows even though
it is not what these runs hit. The untested candidate for the floor is the SSH
handshake: every `nix copy` opens a connection and the key exchange is
CPU-bound crypto on this loop.

**Three ops, not one.** `nix copy --to ssh-ng://` is AddMultipleToStore (op
44) at protocol 1.32 and above, `remote-store.cc:508`. A single-path add is
AddToStoreNar (op 39) through `wire.forward_framed`,
`remote-store.cc:451`, and a pull is NarFromPath (op 38) through
`wire.forward_raw`. The three take different loops, and only op 44's was
measured before.

Op 39 reads Nix's 32 KiB `FramedSink` frames (`serialise.hh:71,724`), so it
did 32 times the per-turn work op 44 does at 1 MiB. A frame boundary carries
no meaning -- `FramedSource` reassembles the frames into one byte stream
(`serialise.hh:694`) -- so the forward gathers them into chunks. One 256 MiB
path, this machine:

    loop     state                    ms/MiB   peak_mib   wall_s
    asyncio  no drain, per frame        13.5       58.8     3.83
    asyncio  drain+checkpoint per frame 27.2*      n/a      n/a
    asyncio  coalesced to 1 MiB         10.0       22.1     2.96
    uvloop   no drain, per frame         8.6      206.9     2.69
    uvloop   drain+checkpoint per frame 12.7*      n/a      n/a
    uvloop   coalesced to 1 MiB          7.8       18.9     2.35

    * measured at 64 MiB, and never shipped: the drain landed and the
      coalescing landed on top of it.

**`peak_mib` is the finding, not `ms/MiB`.** uvloop held 206.9 MiB of a
256 MiB push, and the figure scales with the closure, so a multi-gigabyte
push is that many gigabytes resident. asyncio held less only because it was
slow enough for the daemon to keep up.

**`max_loop_lag_ms` did not move.** It read 14-22 ms before the fix and
14-45 ms after. One client cannot starve this loop: the whole client pipeline
-- NAR serialisation, hashing, syscalls, crypto -- costs about as much per MiB
as pynixd's forwarding does, so pynixd's reader waits on the socket anyway and
the loop gets scheduled regardless of whether the transfer yields.

Not the crypto specifically, which an earlier note here claimed. The cipher is
0.27 ms/MiB (`pynixd/constants.py`) against roughly 7 ms/MiB of Python, so it
is ~4% and cannot be the thing that paces this.

The starvation is real -- `tests/unit/test_forward_framed_stream.py` measures
0 turns of the loop for a whole payload -- but one client is the wrong shape to
find it in. `test_bench_nar_concurrency.py` is the right one.

**Both loops, because a throughput claim about pynixd is a claim about the
loop under it.** ./conftest.py parametrises `anyio_backend`, and `loop` in the
log line is the class that actually ran rather than the one asked for. One run:

    shape                loop      wall_s  srv_cpu  ms/MiB  max_lag_ms
    few-large            asyncio    5.247    3.014    23.5        97.9
    few-large            uvloop     3.142    1.821    14.2        54.0
    one-path-many-files  asyncio    2.620    1.184    37.9        50.6
    one-path-many-files  uvloop     1.747    0.698    22.3        45.4
    many-small           asyncio    2.857    1.559   124.7       144.7
    many-small           uvloop     1.961    1.215    97.2       328.2

uvloop is about 1.6-1.7x on CPU and wall, on every shape.

**Read `max_lag_ms` as a tail, not as a measurement.** The uvloop/many-small
cell above is 328.2 ms; the same cell measured 54.4 ms and 44.7 ms on the runs
either side of it. uvloop is not the slower loop for latency -- that one figure
is noise, and taking it at face value would have been a wrong conclusion drawn
from a real number. CPU and wall repeat to within a few percent across runs.

Take a median of several runs before concluding anything about a loop from the
tail.
"""

from __future__ import annotations

import asyncio
import gc
import os
import resource
import time
import tracemalloc
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import pytest
import structlog

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

    from pynixd import Server

log = structlog.get_logger(__name__)

# Fixed sizes, never derived from the host, so two runs are comparable and a
# number can be quoted in an issue.
_MANY_SMALL = (200, 64)
_FEW_LARGE = (2, 64 * 1024)
# Files inside one path, not paths. nixidae/nixidae#487 reports 8871 in a single 49 MiB
# path; 4000 of 8 KiB is the same shape at a size a test can afford.
_ONE_PATH_MANY_FILES = (4000, 8)

_SRC_STORE = STORE_PREFIX / "nar-profile-src"
_CONTENT_DIR = STORE_PREFIX / "nar-profile-content"

_MIB = 1024 * 1024


@pytest.fixture
def src_store() -> Iterator[Path]:
    """A source store of this repository's own, per the /tmp/pynixd-stores rule."""
    rmtree_robust(_SRC_STORE)
    rmtree_robust(_CONTENT_DIR)
    _SRC_STORE.mkdir(parents=True, exist_ok=True)
    _CONTENT_DIR.mkdir(parents=True, exist_ok=True)
    yield _SRC_STORE
    rmtree_robust(_SRC_STORE)
    rmtree_robust(_CONTENT_DIR)


async def _add_tree(src: Path, files: int, size_kib: int) -> list[str]:
    """Add ONE path holding `files` files, and return it.

    A different axis from `_add_paths`. That one varies how many paths a
    transfer names; this varies how many files one NAR contains. The forward
    loop moves bytes and never looks inside a NAR, so its cost should not
    depend on this at all -- which is exactly what makes it worth measuring.

    A cluster reported 999m of CPU and no progress pushing 8871 files inside a
    single 49 MiB path (nixidae/nixidae#487). The forward path cannot explain that, so
    either the cost is elsewhere or this shape is cheap and the search moves on.
    """
    tree = _CONTENT_DIR / f"tree-{files}x{size_kib}k"
    tree.mkdir(parents=True, exist_ok=True)
    blob = os.urandom(size_kib * 1024)
    for i in range(files):
        # Fan out, because one directory of several thousand entries measures
        # the filesystem as much as it measures the NAR.
        sub = tree / f"d{i // 256:04d}"
        sub.mkdir(exist_ok=True)
        (sub / f"f{i:05d}").write_bytes(blob)

    rc, stdout, stderr, _ = await run_subproc(
        [str(CLIENT_BIN), "store", "add-path", "--store", str(src), str(tree)],
    )
    assert rc == 0, f"add-path failed:\n{stderr}"
    return [stdout.strip()]


async def _add_paths(src: Path, count: int, size_kib: int) -> list[str]:
    """Add `count` paths of `size_kib` each to `src`, and return them.

    The content is random so that no two paths deduplicate into one, which
    would make the shape smaller than it claims to be.
    """
    paths: list[str] = []
    for i in range(count):
        blob = _CONTENT_DIR / f"blob-{size_kib}k-{i}"
        blob.write_bytes(os.urandom(size_kib * 1024))
        rc, stdout, stderr, _ = await run_subproc(
            [str(CLIENT_BIN), "store", "add-path", "--store", str(src), str(blob)],
        )
        assert rc == 0, f"add-path failed:\n{stderr}"
        paths.append(stdout.strip())
        blob.unlink()
    return paths


class _GCPauses:
    """Time every garbage collection that runs inside the block.

    A collection is stop-the-world, so it blocks the event loop exactly the way
    a synchronous stretch of work does, and nothing in the handler can yield
    during one. Measured on this machine, a gen-2 pass over two million live
    objects stalls the loop for 503 ms -- against 9 ms for spawning a process.

    That is the link between allocation churn and a failing liveness probe:
    every copy in the read path is an allocation, and allocations are what
    schedule the next collection.
    """

    def __init__(self) -> None:
        self.max_pause_s = 0.0
        self.total_s = 0.0
        self.collections = 0
        self._start: float | None = None

    def __call__(self, phase: str, info: dict[str, int]) -> None:
        if phase == "start":
            self._start = time.perf_counter()
            return
        if self._start is None:
            return
        elapsed = time.perf_counter() - self._start
        self._start = None
        self.max_pause_s = max(self.max_pause_s, elapsed)
        self.total_s += elapsed
        self.collections += 1


@contextmanager
def _measure(label: str, *, count: int, sent_bytes: int, lag: LoopLag | None = None) -> Iterator[None]:
    """Report wall time, CPU and peak allocation for the block.

    CPU is split. `RUSAGE_SELF` is pynixd, because the server runs in this
    process; `RUSAGE_CHILDREN` is the `nix` client, which is a subprocess. A
    single total would hide which side burns the core.
    """
    pauses = _GCPauses()
    gc.callbacks.append(pauses)
    tracemalloc.start()
    tracemalloc.reset_peak()
    self0 = resource.getrusage(resource.RUSAGE_SELF)
    kids0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    wall0 = time.perf_counter()

    try:
        yield
    finally:
        gc.callbacks.remove(pauses)

    wall = time.perf_counter() - wall0
    self1 = resource.getrusage(resource.RUSAGE_SELF)
    kids1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    server_cpu = (self1.ru_utime - self0.ru_utime) + (self1.ru_stime - self0.ru_stime)
    client_cpu = (kids1.ru_utime - kids0.ru_utime) + (kids1.ru_stime - kids0.ru_stime)
    sent_mib = sent_bytes / _MIB

    # The loop that actually ran, not the one the parameter asked for. uvloop's
    # loop class lives in the `uvloop` package and asyncio's in `asyncio`, so
    # this reports a substitution that did not take rather than repeating the
    # request back.
    loop_impl = type(asyncio.get_running_loop()).__module__.partition(".")[0]

    log.info(
        "bench_nar_forward_profile",
        loop=loop_impl,
        shape=label,
        paths=count,
        sent_mib=round(sent_mib, 2),
        wall_s=round(wall, 3),
        server_cpu_s=round(server_cpu, 3),
        client_cpu_s=round(client_cpu, 3),
        # The two that locate the fault.
        cpu_ms_per_path=round(server_cpu * 1000 / count, 3),
        cpu_ms_per_mib=round(server_cpu * 1000 / sent_mib, 3) if sent_mib else None,
        peak_mib=round(peak / _MIB, 2),
        # Streaming keeps this near zero; buffering drives it towards 1.
        peak_over_sent=round(peak / sent_bytes, 3) if sent_bytes else None,
        # What a TCP liveness probe sees. The live default is
        # periodSeconds 10, failureThreshold 6, so 60s of silence restarts the
        # pod -- but a probe also fails on a much shorter stall if it lands in
        # one, and the restart then destroys the evidence.
        max_loop_lag_ms=round(lag.max_lag_s * 1000, 1) if lag else None,
        loop_samples=lag.samples if lag else None,
        # A collection is stop-the-world, so it blocks the loop whatever the
        # handler does. Compare `max_gc_pause_ms` with `max_loop_lag_ms`: when
        # they agree, the stall is the collector and not the transfer.
        max_gc_pause_ms=round(pauses.max_pause_s * 1000, 1),
        gc_total_ms=round(pauses.total_s * 1000, 1),
        gc_collections=pauses.collections,
    )


async def _copy_and_measure(
    server: Server,
    src: Path,
    label: str,
    count: int,
    size_kib: int,
) -> None:
    paths = await _add_paths(src, count, size_kib)
    await _copy_paths_and_measure(
        server,
        paths,
        label,
        count=count,
        sent_bytes=count * size_kib * 1024,
    )


async def _copy_paths_and_measure(
    server: Server,
    paths: list[str],
    label: str,
    *,
    count: int,
    sent_bytes: int,
) -> None:
    cmd = [
        str(CLIENT_BIN),
        "copy",
        "--no-check-sigs",
        "--from",
        str(_SRC_STORE),
        "--to",
        ssh_admin_uri(server),
        *paths,
    ]

    lag = LoopLag()
    async with anyio.create_task_group() as tg:
        tg.start_soon(lag.run)
        try:
            with _measure(label, count=count, sent_bytes=sent_bytes, lag=lag):
                rc, _, stderr, _ = await run_subproc(cmd)
        finally:
            lag.stop()

    assert rc == 0, f"nix copy failed:\n{stderr}"

    # A copy that silently moved nothing would report a flattering profile.
    rc_check, stdout_check, stderr_check, _ = await run_subproc(
        [str(CLIENT_BIN), "path-info", "--store", ssh_admin_uri(server), *paths],
    )
    assert rc_check == 0, f"path-info failed:\n{stderr_check}"
    # Every path named, not `count` -- `count` is the unit the shape varies,
    # which is paths for some shapes and files inside one path for others.
    assert len(stdout_check.split()) >= len(paths)


async def _add_path_and_measure(
    server: Server,
    label: str,
    size_kib: int,
) -> None:
    """Push ONE path with `nix store add-path`, which is op 39 and not op 44.

    `nix copy` sends AddMultipleToStore at protocol 1.32 and above
    (`remote-store.cc:508`), so every other test in this file measures op 44.
    A single-path add goes through `RemoteStore::addToStore`
    (`remote-store.cc:451`), which is AddToStoreNar and `wire.forward_framed`
    -- a different loop with a different frame size. Nix frames it with a 32
    KiB `FramedSink` (`serialise.hh:71,724`) against op 44's 1 MiB chunks, so
    per-frame cost shows up here at 32 times the rate.
    """
    blob = _CONTENT_DIR / f"op39-{size_kib}k"
    blob.write_bytes(os.urandom(size_kib * 1024))

    cmd = [
        str(CLIENT_BIN),
        "store",
        "add-path",
        "--store",
        ssh_admin_uri(server),
        str(blob),
    ]

    lag = LoopLag()
    async with anyio.create_task_group() as tg:
        tg.start_soon(lag.run)
        try:
            with _measure(label, count=1, sent_bytes=size_kib * 1024, lag=lag):
                rc, stdout, stderr, _ = await run_subproc(cmd)
        finally:
            lag.stop()

    assert rc == 0, f"nix store add-path failed:\n{stderr}"
    assert stdout.strip().startswith("/nix/store/"), stdout
    blob.unlink()


async def _pull_and_measure(
    server: Server,
    paths: list[str],
    label: str,
    *,
    sent_bytes: int,
) -> None:
    """Pull the paths back out of pynixd, which is NarFromPath (op 38).

    The serving direction. A node that starts a pod reads its closure this
    way, so this is the loop a liveness probe competes with while pods come
    up, rather than while somebody pushes.
    """
    dst = STORE_PREFIX / "nar-profile-pull"
    rmtree_robust(dst)
    dst.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(CLIENT_BIN),
        "copy",
        "--no-check-sigs",
        "--from",
        ssh_admin_uri(server),
        "--to",
        str(dst),
        *paths,
    ]

    lag = LoopLag()
    async with anyio.create_task_group() as tg:
        tg.start_soon(lag.run)
        try:
            with _measure(label, count=len(paths), sent_bytes=sent_bytes, lag=lag):
                rc, _, stderr, _ = await run_subproc(cmd)
        finally:
            lag.stop()

    assert rc == 0, f"nix copy --from failed:\n{stderr}"
    rmtree_robust(dst)


@pytest.mark.benchmark
async def test_profile_nar_forward_op39_one_large(pynixd_server: Server, src_store: Path) -> None:  # noqa: ARG001
    """One 64 MiB path through `forward_framed`, the op 39 loop.

    Read `peak_over_sent` and `max_loop_lag_ms` against the few-large shape
    above: the two loops move the same bytes and should now agree, and
    `cpu_ms_per_mib` says what the 32 KiB frame costs against a 1 MiB chunk.
    """
    _, size_kib = _FEW_LARGE
    await _add_path_and_measure(pynixd_server, "op39-one-large", size_kib)


@pytest.mark.benchmark
async def test_profile_nar_serve_few_large(pynixd_server: Server, src_store: Path) -> None:
    """The serving direction: 2 paths of 64 MiB pulled back out, op 38.

    `forward_raw` reads from the local daemon over a Unix socket, which is
    ready almost every time it is asked, so this loop suspends least of the
    three and is the one a probe loses to while pods start.
    """
    count, size_kib = _FEW_LARGE
    paths = await _add_paths(src_store, count, size_kib)
    await _copy_paths_and_measure(
        pynixd_server,
        paths,
        "serve-push-setup",
        count=count,
        sent_bytes=count * size_kib * 1024,
    )
    await _pull_and_measure(
        pynixd_server,
        paths,
        "serve-few-large",
        sent_bytes=count * size_kib * 1024,
    )


@pytest.mark.benchmark
async def test_profile_nar_forward_many_small(pynixd_server: Server, src_store: Path) -> None:
    """Many small paths: the cost per path, not per byte.

    200 paths of 64 KiB. Read `cpu_ms_per_path`.
    """
    count, size_kib = _MANY_SMALL
    await _copy_and_measure(pynixd_server, src_store, "many-small", count, size_kib)


@pytest.mark.benchmark
async def test_profile_nar_forward_few_large(pynixd_server: Server, src_store: Path) -> None:
    """Few large paths: the cost per byte, and whether the forward streams.

    2 paths of 64 MiB. Read `peak_over_sent`: a forward that streams holds about
    one chunk whatever the payload, and one that buffers holds all of it.
    """
    count, size_kib = _FEW_LARGE
    await _copy_and_measure(pynixd_server, src_store, "few-large", count, size_kib)


@pytest.mark.benchmark
async def test_profile_nar_forward_one_path_many_files(pynixd_server: Server, src_store: Path) -> None:
    """One path holding many files: does the forward care what is inside a NAR?

    4000 files of 8 KiB in a single store path, about 31 MiB. Read
    `cpu_ms_per_mib` against the few-large shape. The forward loop moves bytes
    and never parses the NAR, so the two should agree; a large gap says the
    cost is somewhere that does look inside.

    This exists for nixidae/nixidae#487, where a cluster pegged a core with no progress
    pushing 8871 files inside one 49 MiB path.
    """
    files, size_kib = _ONE_PATH_MANY_FILES
    paths = await _add_tree(src_store, files, size_kib)
    await _copy_paths_and_measure(
        pynixd_server,
        paths,
        "one-path-many-files",
        count=files,
        sent_bytes=files * size_kib * 1024,
    )
