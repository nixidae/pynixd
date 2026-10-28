"""Tests for NAR streaming between stores."""

from __future__ import annotations

from pathlib import Path

import structlog

from pynixd import metrics
from pynixd.serde import IsValidPathRequest
from pynixd.store import LocalSocketStore
from pynixd.store.transfer import stream_paths_store_to_store
from pynixd.store_path import StorePath
from tests._conftest.constants import HELLO
from tests.conftest import (
    CLIENT_BIN,
    STORE_PREFIX,
    make_test_spec,
    rmtree_robust,
    run_subproc,
)

log = structlog.get_logger(__name__)


async def get_hello_path() -> StorePath:
    """Build hello from <nixpkgs> and return its store path."""
    _rc, stdout, _stderr, _ = await run_subproc(
        [str(CLIENT_BIN), "build", *HELLO, "--no-link", "--print-out-paths"],
    )
    return StorePath(stdout.strip())


async def test_stream_nar() -> None:
    """
    Test streaming a NAR from the system store to a temporary store.
    This doesn't start pynixd, it just uses two LocalSocketStore instances.

    Store operations triggered:
    - NarFromPath: Gets NAR from path for streaming
    """
    # 1. Build hello in the system store
    store_path = await get_hello_path()
    log.info("streaming_path", path=store_path)

    # Source store is the system store (/)
    src_store = LocalSocketStore(
        make_test_spec(store_id="system", store_path=Path("/"), no_probe=True),
    )

    # Destination store is a temporary store
    dst_path = STORE_PREFIX / "test-stream-nar"
    rmtree_robust(dst_path)
    dst_store = LocalSocketStore(
        make_test_spec(store_id="test-stream-nar", store_path=dst_path, no_probe=True),
    )

    try:
        # 2. Stream the path from src to dst
        # Check if path exists in src
        is_valid_src = await src_store.execute(IsValidPathRequest(path=StorePath(path=str(store_path))))
        assert is_valid_src.valid, f"Path {store_path} not valid in system store"

        # Use stream_paths_store_to_store which handles the NAR piping.
        # No local store is a party here, so the labels only prove the
        # plumbing records a real transfer; the scheduler pins the
        # in/out semantics in the unit suite.
        before = metrics.store_transfers(["test-stream-nar"])["test-stream-nar"]
        _src_read_before, src_written_before = src_store.transfer_pool.traffic_totals()
        dst_read_before, _ = dst_store.transfer_pool.traffic_totals()
        await stream_paths_store_to_store(
            src_store,
            dst_store,
            [store_path],
            peer_store_id="test-stream-nar",
            direction="out",
        )
        after = metrics.store_transfers(["test-stream-nar"])["test-stream-nar"]
        assert after["bytes_out"] - before["bytes_out"] > 0
        assert after["paths_out"] - before["paths_out"] >= 1

        # The wire totals move on both ends: the source wrote the NAR,
        # the destination read it.
        _, src_written_after = src_store.transfer_pool.traffic_totals()
        dst_read_after, _ = dst_store.transfer_pool.traffic_totals()
        assert src_written_after - src_written_before > 0
        assert dst_read_after - dst_read_before > 0

        # Verify it now exists in dst
        is_valid_dst_after = await dst_store.execute(
            IsValidPathRequest(path=StorePath(path=str(store_path))),
        )
        assert is_valid_dst_after.valid
    finally:
        await src_store.close()
        await dst_store.close()
