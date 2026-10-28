"""DAG build tests."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import structlog

from pynixd.store import get_current_system
from tests.conftest import (
    CLIENT_BIN,
    TEST_NIX,
    run_subproc,
    server_uri,
    set_log_levels,
)

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd import Server


log = structlog.get_logger(__name__)


async def test_builders(pynixd_server: Server, tmp_path: Path) -> None:
    """Build nix/standard.dag via --builders.

    Store operations triggered:
    - AddMultipleToStore: Adds multiple paths to store
    - BuildDerivation: Builds derivation
    - NarFromPath: Gets NAR from path
    - QueryPathInfo: Queries path info
    - QueryValidPaths: Queries valid paths
    """
    test_nix = TEST_NIX

    client_store_path = tmp_path / "client-store"
    client_store_path.mkdir(parents=True, exist_ok=True)

    uri = server_uri(pynixd_server)
    system = get_current_system()
    builder_spec = f"{uri} {system} - 100"

    cmd = [
        str(CLIENT_BIN),
        "build",
        "--store",
        str(client_store_path),
        "--builders",
        builder_spec,
        "--file",
        str(test_nix),
        "dag",
        "--no-link",
        "--print-out-paths",
        "--max-jobs",
        "0",
    ]

    rc, _stdout, _stderr, stdboth = await run_subproc(cmd)
    assert rc == 0, f"build failed:\n{stdboth}"


async def test_store(pynixd_server: Server, tmp_path: Path) -> None:
    """Build nix/standard.dag via --store.

    Store operations triggered:
    - AddMultipleToStore: Adds multiple paths to store
    - BuildPaths: Builds derivation paths
    - BuildPathsWithResults: Builds derivation paths with results
    - QueryMissing: Queries missing paths
    - QueryValidPaths: Queries valid paths
    """
    test_nix = TEST_NIX

    with set_log_levels({"pynixd.op.AddToStore": logging.INFO}):
        uri = server_uri(pynixd_server)

        cmd = [
            str(CLIENT_BIN),
            "build",
            "--eval-store",
            "auto",
            "--store",
            uri,
            "--file",
            str(test_nix),
            "dag",
            "--no-link",
            "--print-out-paths",
        ]
        rc, _, _, stdboth = await run_subproc(cmd)
        assert rc == 0, f"build failed:\n{stdboth}"
