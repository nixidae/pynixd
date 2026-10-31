"""Performance benchmarks for pynixd operation dispatch and local DB."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import structlog

from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec
from pynixd.serde import IsValidPathRequest, QueryAllValidPathsRequest, QueryPathInfoRequest
from pynixd.store import LocalSocketStore, Store
from tests._conftest.all_of import all_of
from tests.conftest import CLIENT_BIN, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

log = structlog.get_logger(__name__)

_STORE_TYPES = ["local-socket"]


async def _make_store(store_type: str) -> Store:
    """Create a store that reads from the system store."""
    if store_type == "local-socket":
        return LocalSocketStore(LocalSocketStoreSpec(store_id=StoreId("local-socket")))
    raise ValueError(f"Unknown store type: {store_type}")


@pytest.fixture(params=_STORE_TYPES)
async def bench_store(request: pytest.FixtureRequest) -> AsyncIterator[Store]:
    """Parametrized store fixture."""
    s = await _make_store(request.param)
    yield s
    await s.close()


@pytest.mark.benchmark
async def test_bench_query_all_valid_paths(bench_store: Store) -> None:
    """Benchmark QueryAllValidPaths latency."""
    start = time.perf_counter()
    resp = await bench_store.execute(QueryAllValidPathsRequest())
    paths = resp.paths
    elapsed = time.perf_counter() - start

    log.info(
        "bench_query_all_valid_paths",
        store=type(bench_store).__name__,
        count=len(paths),
        elapsed_ms=int(elapsed * 1000),
    )


@pytest.mark.benchmark
async def test_bench_query_path_info_latency(bench_store: Store) -> None:
    """Benchmark QueryPathInfo latency for a single path."""
    resp_all = await bench_store.execute(QueryAllValidPathsRequest())
    paths = list(resp_all.paths)
    path = paths[0]

    start = time.perf_counter()
    # 100 iterations to get a stable average
    for _ in range(100):
        await bench_store.execute(QueryPathInfoRequest(path=path))
    elapsed = time.perf_counter() - start

    log.info(
        "bench_query_path_info_latency",
        store=type(bench_store).__name__,
        avg_ms=round((elapsed / 100) * 1000, 3),
    )


@pytest.mark.benchmark
async def test_bench_is_valid_path_throughput(bench_store: Store) -> None:
    """Benchmark IsValidPath throughput (serial)."""
    resp_all = await bench_store.execute(QueryAllValidPathsRequest())
    paths = list(resp_all.paths)[:1000]

    start = time.perf_counter()
    for p in paths:
        await bench_store.execute(IsValidPathRequest(path=p))
    elapsed = time.perf_counter() - start

    log.info(
        "bench_is_valid_path_throughput",
        store=type(bench_store).__name__,
        count=len(paths),
        ops_per_s=int(len(paths) / elapsed),
    )


@pytest.mark.benchmark
async def test_bench_is_valid_path_parallel(bench_store: Store) -> None:
    """Benchmark IsValidPath throughput (parallel)."""
    resp_all = await bench_store.execute(QueryAllValidPathsRequest())
    paths = list(resp_all.paths)[:1000]

    start = time.perf_counter()
    await all_of(*[bench_store.execute(IsValidPathRequest(path=p)) for p in paths])
    elapsed = time.perf_counter() - start

    log.info(
        "bench_is_valid_path_parallel",
        store=type(bench_store).__name__,
        count=len(paths),
        ops_per_s=int(len(paths) / elapsed),
    )


@pytest.mark.benchmark
async def test_bench_local_socket_overhead() -> None:
    """Benchmark the overhead of LocalSocketStore compared to direct daemon access.

    Compares pynixd LocalSocketStore against 'nix store query --all' directly.
    """

    # 1. pynixd overhead
    s = LocalSocketStore(LocalSocketStoreSpec(store_id=StoreId("managed"), store_path=Path("/")))
    start = time.perf_counter()
    await s.execute(QueryAllValidPathsRequest())
    pynixd_elapsed = time.perf_counter() - start
    await s.close()

    # 2. Native overhead
    start = time.perf_counter()
    _rc, _, _, _ = await run_subproc([str(CLIENT_BIN), "path-info", "--all"])
    native_elapsed = time.perf_counter() - start

    log.info(
        "bench_local_socket_overhead",
        pynixd_ms=int(pynixd_elapsed * 1000),
        native_ms=int(native_elapsed * 1000),
        ratio=round(pynixd_elapsed / native_elapsed, 2),
    )
