"""Integration tests for pynixd.nar against the host Nix store.

These tests connect directly to the system's nix-daemon, query all valid
paths, and parameterize one test per path so each gets its own timeout
and appears individually in the test report.

Run explicitly by path::

    pytest tests/nar_integration/test_nar_roundtrip.py
"""

from __future__ import annotations

import io
import random
from pathlib import Path

import anyio
import pytest
import structlog

from pynixd.nar import (
    NarForwarder,
    find_nar_entry,
    forward_nar,
    parse_nar,
    write_nar,
)
from pynixd.serde import QueryAllValidPathsRequest, QueryPathInfoRequest
from pynixd.store import LocalSocketStore
from tests.conftest import make_test_spec, read_nar_from_store, serde_path

log = structlog.get_logger(__name__)


def _get_system_store_paths() -> list[str]:
    """Query the host store for all valid paths (synchronous wrapper)."""

    async def _inner() -> list[str]:
        store = LocalSocketStore(
            make_test_spec(store_id="system", store_path=Path("/"), no_probe=True),
        )
        try:
            resp = await store.execute(QueryAllValidPathsRequest())
            return sorted(str(p) for p in resp.paths)
        finally:
            await store.close()

    return anyio.run(_inner, backend="asyncio")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parameterize NAR roundtrip tests with real store paths."""
    if "store_path_str" in metafunc.fixturenames:
        paths = _get_system_store_paths()
        # Deterministic shuffle so first N aren't always the same
        rng = random.Random(42)
        rng.shuffle(paths)
        # Allow capping via CLI option (handled in conftest, but we can
        # read env here as a simple limiter)
        max_paths = int(__import__("os").environ.get("PYNIXD_MAX_NAR_PATHS", 0)) or None
        if max_paths:
            paths = paths[:max_paths]
        metafunc.parametrize("store_path_str", paths)


async def test_nar_roundtrip_via_streaming(store_path_str: str) -> None:
    """For a single store path: get NAR, stream through NarForwarder,
    parse/serialize, and assert all bytes match.
    """
    from pynixd.store_path import StorePath

    spath = StorePath(store_path_str)

    store = LocalSocketStore(
        make_test_spec(store_id="system", store_path=Path("/"), no_probe=True),
    )
    try:
        info_resp = await store.execute(QueryPathInfoRequest(path=serde_path(spath)))
        if not info_resp.valid or info_resp.info is None:
            pytest.skip(f"No path info for {spath}")

        nar_size = info_resp.info.nar_size
        original = await read_nar_from_store(store, spath, nar_size)
        if not original:
            pytest.skip(f"Empty NAR for {spath}")

        # ── 1. Stream through NarForwarder in small chunks ──
        rng = random.Random(42)
        forwarder = NarForwarder()
        forwarded_chunks: list[bytes] = []
        offset = 0
        while offset < len(original):
            chunk_size = rng.randint(1, 256)
            chunk = original[offset : offset + chunk_size]
            forwarded_chunks.extend(forwarder.feed(chunk))
            offset += len(chunk)
        forwarded_chunks.extend(forwarder.feed(b""))

        assert forwarder.complete, f"NarForwarder did not complete for {spath}"
        forwarded = b"".join(forwarded_chunks)
        assert forwarded == original, f"NarForwarder output differs for {spath}"

        # ── 2. Parse → Serialize roundtrip ──
        node = parse_nar(original)
        serialized = write_nar(node)
        assert serialized == original, f"parse/write roundtrip failed for {spath}"

        # ── 3. Sanity-check helpers ──
        entry = find_nar_entry(node, "")
        assert entry is node

    finally:
        await store.close()


async def test_nar_forwarder_convenience(store_path_str: str) -> None:
    """Verify forward_nar() works end-to-end with a single real store path."""
    from pynixd.store_path import StorePath

    spath = StorePath(store_path_str)

    store = LocalSocketStore(
        make_test_spec(store_id="system", store_path=Path("/"), no_probe=True),
    )
    try:
        info_resp = await store.execute(QueryPathInfoRequest(path=serde_path(spath)))
        if not info_resp.valid or info_resp.info is None:
            pytest.skip(f"No path info for {spath}")

        nar_size = info_resp.info.nar_size
        original = await read_nar_from_store(store, spath, nar_size)
        if not original:
            pytest.skip(f"Empty NAR for {spath}")

        src = io.BytesIO(original)
        dst = io.BytesIO()
        total = forward_nar(src, dst)
        assert total == len(original)
        assert dst.getvalue() == original

    finally:
        await store.close()
