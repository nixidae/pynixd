"""Benchmarks for NAR streaming and path copying performance."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import structlog

from nix_daemon_protocol.ids import StoreId
from pynixd import wire
from pynixd.config import LocalSocketStoreSpec
from pynixd.serde import QueryAllValidPathsRequest, QueryPathInfoRequest
from pynixd.store import DaemonStore, LocalSocketStore
from pynixd.store_path import StorePath
from tests.conftest import CLIENT_BIN, rmtree_robust, run_subproc, serde_path

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from nix_daemon_protocol.query_all_valid_paths import QueryAllValidPathsResponse
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

from pynixd.store.transfer import stream_paths_store_to_store

log = structlog.get_logger(__name__)


BENCH_DST = Path("/tmp/pynixd-bench-dst")

_CHUNK_SIZES_KB = [64, 256, 1024, 4096]

_STORE_TYPES = ["local-socket"]

_CONCURRENCY_LEVELS = [1, 4, 8]


async def _make_store(store_type: str) -> DaemonStore:
    """Create a store that reads from the system store."""
    if store_type == "local-socket":
        return LocalSocketStore(LocalSocketStoreSpec(store_id=StoreId("local-socket")))
    raise ValueError(f"Unknown store type: {store_type}")


@pytest.fixture(params=_STORE_TYPES)
async def bench_store(request: pytest.FixtureRequest) -> AsyncIterator[DaemonStore]:
    """Parametrized store fixture — yields one store per type."""
    s = await _make_store(request.param)
    yield s
    await s.close()


@pytest.fixture
async def dst_store() -> AsyncIterator[LocalSocketStore]:
    rmtree_robust(BENCH_DST)
    BENCH_DST.mkdir(parents=True, exist_ok=True)  # noqa: ASYNC240

    s = LocalSocketStore(
        LocalSocketStoreSpec(store_id=StoreId("bench-dst"), store_path=BENCH_DST, nix_bin=str(CLIENT_BIN)),
    )
    yield s
    await s.close()


def _set_chunk_size(kb: int) -> int:
    old = wire._CHUNK_SIZE
    wire._CHUNK_SIZE = kb * 1024
    return old


def _store_label(s: DaemonStore) -> str:
    if isinstance(s, LocalSocketStore):
        return "local"
    return "unknown"


async def _pick_a_path(s: DaemonStore, need_no_refs: bool = False) -> StorePath:
    resp: QueryAllValidPathsResponse = await s.execute(QueryAllValidPathsRequest())
    paths = list(resp.paths)
    # Prefer something sizeable but not insane
    for p in paths:
        if p.endswith(".drv"):
            continue
        info_resp = await s.execute(QueryPathInfoRequest(path=p))
        info = info_resp.info
        if info and 100_000 < info.nar_size < 10_000_000:
            if need_no_refs and info.references - {p}:
                continue
            return p
    return paths[0]


async def _pick_small_paths(s: DaemonStore, limit: int = 100) -> list[tuple[StorePath, ValidPathInfo]]:
    """Pick small paths by using .drv files directly (always small, no per-path queries)."""
    resp = await s.execute(QueryAllValidPathsRequest())
    picked = []
    for p in resp.paths:
        if not p.endswith(".drv"):
            continue
        info_resp = await s.execute(QueryPathInfoRequest(path=p))
        if info_resp.info:
            picked.append((p, info_resp.info.to_valid_path_info(p)))
            if len(picked) >= limit:
                break
    return picked


async def _create_big_path(size_mb: int) -> StorePath:
    """Use nix-store --add to create a deterministic large path in system store."""
    tmp = Path(f"/tmp/pynixd-bench-big-{size_mb}m")
    if not tmp.exists():  # noqa: ASYNC240
        with tmp.open("wb") as f:  # noqa: ASYNC230
            f.write(os.urandom(size_mb * 1024 * 1024))

    rc, stdout, _, _ = await run_subproc([str(CLIENT_BIN), "store", "add-path", str(tmp)])
    assert rc == 0
    return StorePath(stdout.strip())


@pytest.mark.benchmark
@pytest.mark.parametrize("chunk_kb", _CHUNK_SIZES_KB)
async def test_bench_nar_streaming_latency(bench_store: DaemonStore, dst_store: DaemonStore, chunk_kb: int) -> None:
    """Benchmark: stream a single ~1MB path via stream_paths_store_to_store."""
    store_path = await _pick_a_path(bench_store)
    info_resp = await bench_store.execute(QueryPathInfoRequest(path=serde_path(store_path)))
    info = info_resp.info.to_valid_path_info(serde_path(store_path)) if info_resp.info else None
    assert info is not None

    label = _store_label(bench_store)
    old = _set_chunk_size(chunk_kb)
    try:
        start = time.monotonic()
        await stream_paths_store_to_store(bench_store, dst_store, [store_path])
        elapsed = time.monotonic() - start
    finally:
        wire._CHUNK_SIZE = old

    log.info(
        "bench_nar_latency",
        store=label,
        chunk_kb=chunk_kb,
        size_kb=info.info.nar_size // 1024,
        elapsed_ms=int(elapsed * 1000),
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("chunk_kb", _CHUNK_SIZES_KB)
async def test_bench_nar_streaming_throughput(bench_store: DaemonStore, dst_store: DaemonStore, chunk_kb: int) -> None:
    """Benchmark: stream a 100MB NAR via stream_paths_store_to_store at various chunk sizes."""
    store_path = await _create_big_path(100)
    info_resp = await bench_store.execute(QueryPathInfoRequest(path=serde_path(store_path)))
    info = info_resp.info.to_valid_path_info(serde_path(store_path)) if info_resp.info else None
    assert info is not None

    label = _store_label(bench_store)
    old = _set_chunk_size(chunk_kb)
    try:
        start = time.monotonic()
        await stream_paths_store_to_store(bench_store, dst_store, [store_path])
        elapsed = time.monotonic() - start
    finally:
        wire._CHUNK_SIZE = old

    mbps = 100 / elapsed
    log.info(
        "bench_nar_throughput",
        store=label,
        chunk_kb=chunk_kb,
        elapsed_s=round(elapsed, 2),
        mb_per_s=round(mbps, 2),
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("chunk_kb", _CHUNK_SIZES_KB)
async def test_bench_copy_paths_latency(bench_store: DaemonStore, dst_store: DaemonStore, chunk_kb: int) -> None:
    """Benchmark: stream a single ~1MB path via copy_paths."""
    store_path = await _pick_a_path(bench_store, need_no_refs=True)
    info_resp = await bench_store.execute(QueryPathInfoRequest(path=serde_path(store_path)))
    info = info_resp.info
    assert info is not None

    label = _store_label(bench_store)
    old = _set_chunk_size(chunk_kb)
    try:
        start = time.monotonic()
        await stream_paths_store_to_store(bench_store, dst_store, [store_path])
        elapsed = time.monotonic() - start
    finally:
        wire._CHUNK_SIZE = old

    log.info(
        "bench_copy_paths_latency",
        store=label,
        chunk_kb=chunk_kb,
        size_kb=info.nar_size // 1024,
        elapsed_ms=int(elapsed * 1000),
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("chunk_kb", _CHUNK_SIZES_KB)
async def test_bench_copy_paths_throughput(bench_store: DaemonStore, dst_store: DaemonStore, chunk_kb: int) -> None:
    """Benchmark: stream a 100MB NAR via copy_paths at various chunk sizes."""
    store_path = await _create_big_path(100)
    info_resp = await bench_store.execute(QueryPathInfoRequest(path=serde_path(store_path)))
    info = info_resp.info
    assert info is not None

    label = _store_label(bench_store)
    old = _set_chunk_size(chunk_kb)
    try:
        start = time.monotonic()
        await stream_paths_store_to_store(bench_store, dst_store, [store_path])
        elapsed = time.monotonic() - start
    finally:
        wire._CHUNK_SIZE = old

    mbps = 100 / elapsed
    log.info(
        "bench_copy_paths_throughput",
        store=label,
        chunk_kb=chunk_kb,
        elapsed_s=round(elapsed, 2),
        mb_per_s=round(mbps, 2),
    )


@pytest.mark.benchmark
@pytest.mark.parametrize("chunk_kb", _CHUNK_SIZES_KB)
async def test_bench_copy_paths_batch(bench_store: DaemonStore, dst_store: DaemonStore, chunk_kb: int) -> None:
    """Benchmark: stream 100 small paths (.drv files) in a single batch.

    Tests the efficiency of AddMultipleToStore for small files.
    Uses .drv files since they are always small and abundant.
    """
    picked = await _pick_small_paths(bench_store, 100)
    assert len(picked) >= 100, f"Need 100+ small paths, found {len(picked)}"
    paths = [p for p, _ in picked]

    label = _store_label(bench_store)
    old = _set_chunk_size(chunk_kb)
    try:
        start = time.monotonic()
        await stream_paths_store_to_store(bench_store, dst_store, paths)
        elapsed = time.monotonic() - start
    finally:
        wire._CHUNK_SIZE = old

    log.info(
        "bench_copy_paths_batch",
        store=label,
        chunk_kb=chunk_kb,
        count=len(paths),
        elapsed_ms=int(elapsed * 1000),
        paths_per_s=int(len(paths) / elapsed),
    )
