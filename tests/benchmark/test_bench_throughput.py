"""Dedicated build throughput benchmark with baselines."""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from typing import TYPE_CHECKING

import anyio
import pytest
import structlog

from nix_daemon_protocol.ids import StoreId
from pynixd.instance import Server
from pynixd.store import LocalSocketStore, get_current_system
from tests.conftest import (
    CLIENT_BIN,
    NIX_BIN,
    STORE_PREFIX,
    TEST_NIX,
    make_test_spec,
    rmtree_robust,
    run_subproc,
    server_uri,
)

if TYPE_CHECKING:
    import pyinstrument

log = structlog.get_logger(__name__)

# Common test configuration
NIX_FILE = TEST_NIX
TARGET = "parallel"
MAX_JOBS = 20
TEST_ENV = {
    "PYNIXD_PAR_COUNT": "100",
    "PYNIXD_PAR_SLEEP": "0",
    "PYNIXD_PAR_ID": "throughput-profile",
}


@pytest.mark.benchmark
async def test_throughput_local() -> None:
    """Baseline: Build directly with a custom local store path."""
    store_path = STORE_PREFIX / "throughput-local-baseline"
    rmtree_robust(store_path)
    store_path.mkdir(parents=True, exist_ok=True)

    log.info("starting_local_baseline")
    cmd = [
        str(CLIENT_BIN),
        "build",
        "--max-jobs",
        str(MAX_JOBS),
        "--no-link",
        "--file",
        str(NIX_FILE),
        "--store",
        str(store_path),
        TARGET,
    ]

    start = time.perf_counter()
    rc, _, _, stdboth = await run_subproc(cmd, env=TEST_ENV)
    elapsed = time.perf_counter() - start

    assert rc == 0, f"Local baseline build failed:\n{stdboth}"
    log.info("local_baseline_finished", elapsed=f"{elapsed:.2f}s")


@pytest.mark.benchmark
async def test_throughput_daemon() -> None:
    """Baseline: Build against a standard Nix daemon."""
    store_path = STORE_PREFIX / "throughput-daemon-baseline"
    rmtree_robust(store_path)
    store_path.mkdir(parents=True, exist_ok=True)

    socket_path = store_path / "nix" / "var" / "nix" / "daemon-socket" / "socket"
    socket_path.parent.mkdir(parents=True, exist_ok=True)

    daemon_env = os.environ.copy()
    daemon_env["NIX_DAEMON_SOCKET_PATH"] = str(socket_path)

    log.info("spawning_baseline_daemon", store_path=str(store_path))
    proc = await anyio.open_process(
        [str(NIX_BIN), "daemon", "--store", str(store_path), "--max-jobs", str(MAX_JOBS)],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=daemon_env,
    )

    try:
        # Wait for daemon
        for _ in range(100):
            if socket_path.exists():
                break
            await anyio.sleep(0.05)

        for _ in range(50):
            try:
                _r, w = await asyncio.open_unix_connection(str(socket_path))
                w.close()
                await w.wait_closed()
                break
            except (ConnectionRefusedError, ConnectionResetError):
                await anyio.sleep(0.1)

        remote_uri = f"unix://{socket_path}"

        log.info("starting_daemon_baseline")
        cmd = [
            str(NIX_BIN),
            "build",
            "--max-jobs",
            str(MAX_JOBS),
            "--no-link",
            "--file",
            str(NIX_FILE),
            "--store",
            "daemon",
            TARGET,
        ]

        start = time.perf_counter()
        rc, _, _, stdboth = await run_subproc(
            cmd,
            env=TEST_ENV | {"NIX_REMOTE": remote_uri},
        )
        elapsed = time.perf_counter() - start

        assert rc == 0, f"Daemon baseline build failed:\n{stdboth}"
        log.info("daemon_baseline_finished", elapsed=f"{elapsed:.2f}s")
    finally:
        proc.terminate()
        with anyio.CancelScope(shield=True):
            await proc.aclose()


@pytest.mark.benchmark
async def test_throughput_pynixd(profiler: pyinstrument.Profiler) -> None:
    """pynixd: Build through pynixd proxy."""
    # THIS TEST MUST COMPLETE WITHIN 120 SECONDS. If it takes longer, something is broken.
    # With MAX_JOBS=20 and sleep=0, builds complete in ~10-30s depending on system.
    local_path = STORE_PREFIX / "throughput-pynixd-local"
    builder_path = STORE_PREFIX / "throughput-pynixd-builder"
    client_path = STORE_PREFIX / "throughput-pynixd-client"

    rmtree_robust(local_path)
    rmtree_robust(builder_path)
    rmtree_robust(client_path)

    local_path.mkdir(parents=True, exist_ok=True)
    builder_path.mkdir(parents=True, exist_ok=True)
    client_path.mkdir(parents=True, exist_ok=True)

    local_store = LocalSocketStore(
        make_test_spec(store_id="local", store_path=local_path),
    )
    builder_store = LocalSocketStore(
        make_test_spec(store_id="builder", store_path=builder_path),
    )

    async with Server(
        stores={StoreId("local"): local_store, StoreId("builder"): builder_store},
        ssh_port=0,
    ) as server:
        log.info("server_up_resetting_profiler")
        profiler.reset()  # type: ignore[no-untyped-call]  # pyinstrument ships no type hints

        try:
            system = get_current_system()
            builder_spec = f"{server_uri(server)} {system} - {MAX_JOBS}"

            cmd = [
                str(CLIENT_BIN),
                "build",
                "--store",
                str(client_path),
                "--builders",
                builder_spec,
                "--max-jobs",
                "0",
                "--no-link",
                "--file",
                str(NIX_FILE),
                TARGET,
            ]

            start = time.perf_counter()
            rc, _, _, stdboth = await run_subproc(
                cmd,
                env={
                    "NIX_STATE_DIR": str(client_path / "var/nix"),
                    **TEST_ENV,
                },
            )
            elapsed = time.perf_counter() - start

            assert rc == 0, f"pynixd throughput build failed:\n{stdboth}"
            log.info("pynixd_throughput_finished", elapsed=f"{elapsed:.2f}s")
        finally:
            log.info("build_finished")
