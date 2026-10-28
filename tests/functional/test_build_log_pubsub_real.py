"""Integration test for build log pub/sub using real nix client.

Two separate nix build processes connect to the same pynixd server and
build the same derivation. The second is deduped and subscribes to the
same QueuedBuild. Both should receive identical build log output.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import anyio
import structlog

from tests.conftest import CLIENT_BIN, TEST_NIX, run_subproc, server_uri

if TYPE_CHECKING:
    from pathlib import Path

    import pyinstrument

    from pynixd import Server

log = structlog.get_logger(__name__)


async def _run_client_build(
    client_store_path: Path,
    builders_uri: str,
    nix_file: str,
    attr: str,
) -> tuple[int, str, str, str]:
    """Run a single nix build with a local store and remote builders."""
    cmd = [
        str(CLIENT_BIN),
        "build",
        "-v",
        "-v",
        "--store",
        str(client_store_path),
        "--builders",
        f"{builders_uri} x86_64-linux",
        "--file",
        nix_file,
        attr,
        "--no-link",
        "--print-out-paths",
        "--max-jobs",
        "0",
    ]
    return await run_subproc(
        cmd,
        env={"NIX_STATE_DIR": str(client_store_path / "var/nix")},
    )


async def _run_client2_delayed(
    client_store_path: Path,
    builders_uri: str,
    nix_file: str,
    attr: str,
) -> tuple[int, str, str, str]:
    """Wait 5s then run a nix build, so the first build is still in-flight."""
    await anyio.sleep(5)
    return await _run_client_build(client_store_path, builders_uri, nix_file, attr)


async def _fetch_nix_log(client_store_path: Path, out_path: str) -> str:
    """Fetch the build log for an output path using nix log."""
    cmd = [
        str(CLIENT_BIN),
        "log",
        "--store",
        str(client_store_path),
        out_path,
    ]
    _rc, _stdout, _stderr, combined = await run_subproc(cmd, expected_retcode=None)
    return combined


async def test_build_log_pubsub_real_nix(
    profiler: pyinstrument.Profiler,
    pynixd_server: Server,
    tmp_path: Path,
) -> None:
    """Two nix builds of the same derivation receive identical log output.

    Client 1 starts first. Client 2 starts 5s later while the build is
    still running (~10s total build time). The second build is deduped by
    the scheduler and both clients subscribe to the same QueuedBuild's log
    stream.
    """
    uri = server_uri(pynixd_server)

    # Two separate local stores so the clients don't share SQLite DBs.
    store1 = tmp_path / "client1"
    store2 = tmp_path / "client2"
    store1.mkdir()
    store2.mkdir()

    log.info("starting_both_clients")

    result1, result2 = await asyncio.gather(
        _run_client_build(store1, uri, str(TEST_NIX), "log_test"),
        _run_client2_delayed(store2, uri, str(TEST_NIX), "log_test"),
    )

    rc1, stdout1, _stderr1, combined1 = result1
    rc2, stdout2, _stderr2, combined2 = result2

    log.info("client1_done", rc=rc1, out=stdout1.strip())
    log.info("client2_done", rc=rc2, out=stdout2.strip())

    # Both should succeed.
    assert rc1 == 0, f"client1 build failed:\n{combined1}"
    assert rc2 == 0, f"client2 build failed:\n{combined2}"

    # Both should have produced the same output path.
    out1 = stdout1.strip()
    out2 = stdout2.strip()
    assert out1 == out2
    assert "/nix/store/" in out1

    # Verify both client stores have the full build log via nix log.
    log1 = await _fetch_nix_log(store1, out1)
    log2 = await _fetch_nix_log(store2, out2)

    log.info("nix_log_client1", log=log1)
    log.info("nix_log_client2", log=log2)

    # Both clients must have identical, complete logs.
    assert log1 == log2, f"client logs differ:\nclient1:\n{log1}\n---\nclient2:\n{log2}"
    for i in range(1, 11):
        assert str(i) in log1, f"client1 nix log missing line '{i}':\n{log1}"
