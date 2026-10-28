"""Functional tests for HTTP binary cache upload (PUT)."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import TYPE_CHECKING

import aiohttp
import structlog

from pynixd.serde import QueryPathInfoRequest
from pynixd.store import LocalSocketStore
from pynixd.store_path import StorePath
from tests._conftest.constants import HELLO
from tests.conftest import (
    CLIENT_BIN,
    SESSION_HTTP_PASS,
    SESSION_HTTP_USER,
    make_test_spec,
    read_nar_from_store,
    run_subproc,
    serde_path,
)

if TYPE_CHECKING:
    from pynixd import Server

log = structlog.get_logger(__name__)

HTTP_AUTH_HEADER = "Basic " + base64.b64encode(f"{SESSION_HTTP_USER}:{SESSION_HTTP_PASS}".encode()).decode()


async def get_hello_path() -> StorePath:
    """Build hello from <nixpkgs> and return its store path."""
    rc, stdout, _stderr, _ = await run_subproc(
        [str(CLIENT_BIN), "path-info", *HELLO],
    )
    if rc != 0:
        await run_subproc([str(CLIENT_BIN), "build", *HELLO])
        rc, stdout, _stderr, _ = await run_subproc(
            [str(CLIENT_BIN), "path-info", *HELLO],
        )
    return StorePath(stdout.strip())


async def get_no_refs_path() -> StorePath:
    """Create a path with no references and return its store path."""
    rc, stdout, stderr, _ = await run_subproc(
        [
            str(CLIENT_BIN),
            "build",
            "--no-link",
            "--print-out-paths",
            "--impure",
            "--expr",
            'builtins.toFile "no-refs-test" "some random content"',
        ],
    )
    assert rc == 0, f"Failed to create no-refs path: {stderr}"
    return StorePath(stdout.strip())


async def test_http_upload(
    pynixd_server: Server,
) -> None:
    """Test uploading a path to the HTTP cache via PUT using aiohttp directly.

    Store operations triggered:
    - None: This test only checks HTTP upload functionality without triggering explicit Store operations
    """
    # Use no-refs path for direct PUT to avoid dependency issues
    path = await get_no_refs_path()
    hash_part = path.hash_part()

    # Get its NAR and narinfo from root store
    root_store = LocalSocketStore(
        make_test_spec(store_id="root", store_path=Path("/"), no_probe=True),
    )
    info_resp = await root_store.execute(QueryPathInfoRequest(path=serde_path(path)))
    assert info_resp.valid
    assert info_resp.info is not None
    vinfo = info_resp.info

    nar_data = await read_nar_from_store(root_store, path, vinfo.nar_size)

    # Build narinfo using NAR hash in URL (matches what handle_put_nar saves as)
    nar_hash = str(vinfo.nar_hash)
    if not nar_hash.startswith("sha256:"):
        nar_hash = f"sha256:{nar_hash}"
    nar_hash_part = nar_hash.split(":")[-1]
    narinfo_lines = [
        f"StorePath: {path}",
        f"URL: nar/{nar_hash_part}.nar",
        "Compression: none",
        f"NarHash: {nar_hash}",
        f"NarSize: {vinfo.nar_size}",
    ]
    refs = sorted(str(r) for r in vinfo.references)
    if refs:
        narinfo_lines.append(f"References: {' '.join(r.rsplit('/', 1)[-1] for r in refs)}")
    if vinfo.deriver:
        narinfo_lines.append(f"Deriver: {str(vinfo.deriver).rsplit('/', 1)[-1]}")
    narinfo = "\n".join(narinfo_lines) + "\n"

    base_url = f"http://127.0.0.1:{pynixd_server.http_bound_port}"

    async with aiohttp.ClientSession() as session:
        headers = {"Authorization": HTTP_AUTH_HEADER}
        # 3. PUT NAR
        nar_hash_part = str(vinfo.nar_hash).split(":")[-1]
        log.info("uploading_nar", hash=nar_hash_part, size=len(nar_data))
        async with session.put(
            f"{base_url}/nar/{nar_hash_part}.nar",
            data=nar_data,
            headers=headers,
        ) as resp:
            assert resp.status == 200
            assert await resp.text() == "ok\n"

        # 4. PUT .narinfo
        log.info("uploading_narinfo", hash=hash_part)
        async with session.put(
            f"{base_url}/{hash_part}.narinfo",
            data=narinfo,
            headers=headers,
        ) as resp:
            assert resp.status == 200
            assert await resp.text() == "ok\n"

    # 5. Verify it now exists in the local store (HTTP uploads go to local_store)
    info_resp = await pynixd_server.local_store.execute(QueryPathInfoRequest(path=serde_path(path)))
    assert info_resp.valid, f"Path {path} should be valid in local store after upload"


async def test_nix_copy_to_http(
    pynixd_server: Server,
) -> None:
    """Test copying a path to the HTTP cache using 'nix copy --to http://...'.

    Store operations triggered:
    - None: This test only checks HTTP upload functionality via nix copy without triggering explicit Store operations
    """
    # Use hello as requested by user - nix copy handles the closure
    path = await get_hello_path()

    # 3. Use 'nix copy' to upload
    # Nix copy expects URL with embedded credentials for basic auth
    auth_url = f"http://{SESSION_HTTP_USER}:{SESSION_HTTP_PASS}@127.0.0.1:{pynixd_server.http_bound_port}"
    cmd = [
        str(CLIENT_BIN),
        "copy",
        "--to",
        auth_url,
        str(path),
    ]
    rc, _stdout, stderr, _ = await run_subproc(cmd)
    assert rc == 0, f"nix copy failed:\n{stderr}"

    # 4. Verify it now exists in the local store (HTTP uploads go to local_store)
    info_resp = await pynixd_server.local_store.execute(QueryPathInfoRequest(path=serde_path(path)))
    assert info_resp.valid, f"Path {path} should be valid in local store after nix copy"
