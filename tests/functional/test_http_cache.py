"""Functional tests for the HTTP binary cache server."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiohttp
import structlog

from pynixd.serde import StorePath
from tests.conftest import (
    CLIENT_BIN,
    SESSION_HTTP_PASS,
    SESSION_HTTP_USER,
    STORE_PREFIX,
    rmtree_robust,
    run_subproc,
    server_uri,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd import Server

log = structlog.get_logger(__name__)


async def _add_path(server: Server, tmp_path: Path) -> StorePath:
    """A path of the test's own: with `PYNIXD_NO_DAEMON=1` the session store starts empty."""
    content = tmp_path / "served"
    content.write_text(f"served by the HTTP cache from {tmp_path.name}\n")
    _rc, out, _err, _ = await run_subproc(
        [str(CLIENT_BIN), "store", "add-file", "--store", server_uri(server), str(content)]
    )
    return StorePath(out.strip())


async def test_narinfo(pynixd_server: Server, tmp_path: Path) -> None:
    """Test fetching .narinfo from the HTTP cache."""
    base_url = f"http://127.0.0.1:{pynixd_server.http_bound_port}"

    path = await _add_path(pynixd_server, tmp_path)
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


async def test_nar_streaming(pynixd_server: Server, tmp_path: Path) -> None:
    """Test streaming a NAR from the HTTP cache."""
    base_url = f"http://127.0.0.1:{pynixd_server.http_bound_port}"

    path = await _add_path(pynixd_server, tmp_path)
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


async def test_cache_as_substituter(pynixd_server: Server, tmp_path: Path) -> None:
    """Test using pynixd HTTP cache as a substituter."""
    target_path = await _add_path(pynixd_server, tmp_path)
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
