"""Functional tests for the HTTP binary cache server."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

import aiohttp
import structlog

from pynixd.serde import QueryAllValidPathsRequest, QueryPathInfoRequest, StorePath as StorePath
from tests.conftest import (
    CLIENT_BIN,
    SESSION_HTTP_PASS,
    SESSION_HTTP_USER,
    STORE_PREFIX,
    rmtree_robust,
    run_subproc,
)

if TYPE_CHECKING:
    from pynixd import Server
    from pynixd.store import DaemonStore

log = structlog.get_logger(__name__)


async def _pick_random_path(store: DaemonStore) -> StorePath:
    """Pick an arbitrary valid path from the store."""
    resp = await store.execute(QueryAllValidPathsRequest())
    all_paths = list(resp.paths)
    assert all_paths, "Store has no paths?!"

    # Shuffle to avoid always hitting the same slow-to-find paths
    random.shuffle(all_paths)

    # Filter for something that likely has metadata
    count = 0
    for p in all_paths:
        count += 1
        if count > 100:  # Don't try too many
            break
        if p.endswith(".drv"):
            continue
        # Also ensure it has some size
        info_resp = await store.execute(QueryPathInfoRequest(path=p))
        if info_resp.valid and info_resp.info and info_resp.info.nar_size > 0:
            return p

    return all_paths[0]


async def test_narinfo(pynixd_server: Server) -> None:
    """Test fetching .narinfo from the HTTP cache."""
    local_store = pynixd_server.local_store
    base_url = f"http://127.0.0.1:{pynixd_server.http_bound_port}"

    path = await _pick_random_path(local_store)
    hash_part = path.hash_part()
    log.info("test_narinfo", path=path, hash_part=hash_part, url=base_url)

    async with aiohttp.ClientSession() as session:
        auth = aiohttp.BasicAuth(SESSION_HTTP_USER, SESSION_HTTP_PASS)
        url = f"{base_url}/{hash_part}.narinfo"
        async with session.get(url, auth=auth, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            assert resp.status == 200
            text = await resp.text()
            assert f"StorePath: {path}" in text
            assert "URL: nar/" in text


async def test_nar_streaming(pynixd_server: Server) -> None:
    """Test streaming a NAR from the HTTP cache."""
    local_store = pynixd_server.local_store
    base_url = f"http://127.0.0.1:{pynixd_server.http_bound_port}"

    path = await _pick_random_path(local_store)
    hash_part = path.hash_part()

    auth = aiohttp.BasicAuth(SESSION_HTTP_USER, SESSION_HTTP_PASS)
    async with aiohttp.ClientSession() as session:
        url = f"{base_url}/{hash_part}.narinfo"
        async with session.get(url, auth=auth, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            assert resp.status == 200
            narinfo = await resp.text()
            nar_url = ""
            for line in narinfo.splitlines():
                if line.startswith("URL: "):
                    nar_url = line.split(": ", 1)[1].strip()
                    break
            assert nar_url

        # Stream the NAR and verify it's not empty
        async with session.get(
            f"{base_url}/{nar_url}",
            auth=auth,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            assert resp.status == 200
            total_bytes = 0
            async for chunk in resp.content.iter_chunked(1024 * 1024):
                total_bytes += len(chunk)
            assert total_bytes > 0


async def test_cache_as_substituter(pynixd_server: Server) -> None:
    """Test using pynixd HTTP cache as a substituter."""
    local_store = pynixd_server.local_store

    target_path = await _pick_random_path(local_store)
    base_url = f"http://{SESSION_HTTP_USER}:{SESSION_HTTP_PASS}@127.0.0.1:{pynixd_server.http_bound_port}"

    # Use a fresh temporary store for substitution
    subst_store_path = STORE_PREFIX / "http-subst-functional"
    rmtree_robust(subst_store_path)
    subst_store_path.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(CLIENT_BIN),
        "copy",
        "--to",
        f"file://{subst_store_path}",
        "--from",
        base_url,
        str(target_path),
    ]

    rc, _stdout, stderr, _ = await run_subproc(cmd)
    assert rc == 0, f"Copy via cache failed:\n{stderr}"

    # Verify it exists in the new store
    cmd = [
        str(CLIENT_BIN),
        "store",
        "ls",
        "--store",
        f"file://{subst_store_path!s}",
        str(target_path),
    ]
    rc, _stdout, stderr, _ = await run_subproc(cmd)
    assert rc == 0, f"path check failed:\n{stderr}"


async def test_cache_not_found(pynixd_server: Server) -> None:
    """Test 404 response for non-existent paths.

    Store operations triggered:
    - None: This test only checks HTTP 404 handling without triggering Store operations
    """
    base_url = f"http://127.0.0.1:{pynixd_server.http_bound_port}"
    auth = aiohttp.BasicAuth(SESSION_HTTP_USER, SESSION_HTTP_PASS)
    async with aiohttp.ClientSession() as session:
        hash_part = "00000000000000000000000000000000"
        async with session.get(
            f"{base_url}/{hash_part}.narinfo",
            auth=auth,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            assert resp.status == 404
