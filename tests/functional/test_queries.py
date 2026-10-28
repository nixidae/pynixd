"""Advanced store query tests."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest
import structlog

from tests.conftest import (
    CLIENT_BIN,
    TEST_NIX,
    run_subproc,
    server_uri,
)

if TYPE_CHECKING:
    from pathlib import Path

    import pyinstrument

    from pynixd import Server

log = structlog.get_logger(__name__)


@pytest.fixture
async def query_env(pynixd_server: Server) -> tuple[Server, str, str]:
    """Set up a pynixd server with some initial paths."""
    uri = server_uri(pynixd_server)

    test_nix = TEST_NIX
    cmd: list[str | Path] = [
        CLIENT_BIN,
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--impure",
        "--file",
        test_nix,
        "minimal.leaf",
        "--no-link",
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(cmd)
    assert rc == 0, f"Initial build failed:\n{stdboth}"

    cmd = [
        CLIENT_BIN.parent / "nix-instantiate",
        "--impure",
        test_nix,
        "-A",
        "minimal.leaf",
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(cmd)
    assert rc == 0
    drv_path = stdout.strip()

    cmd = [
        CLIENT_BIN.parent / "nix-store",
        "-q",
        "--outputs",
        drv_path,
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(cmd)
    assert rc == 0
    out_path = stdout.strip()
    assert out_path.startswith("/nix/store/"), f"Unexpected path: {out_path}"

    return pynixd_server, uri, out_path


@pytest.mark.legacy_nix_commands
async def test_query_referrers(profiler: pyinstrument.Profiler, query_env: tuple[Server, str, str]) -> None:
    """Verify QueryReferrers via 'nix-store -q --referrers'.

    Store operations triggered:
    - QueryReferrers: Queries referrers
    """
    _server, uri, out_path = query_env
    test_nix = TEST_NIX

    # Build another thing that depends on 'out_path'
    cmd = [
        CLIENT_BIN,
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--impure",
        "--file",
        test_nix,
        "minimal.dependent",
        "--no-link",
    ]
    rc, stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0

    # Get dep_path locally
    cmd = [
        CLIENT_BIN.parent / "nix-instantiate",
        "--impure",
        test_nix,
        "-A",
        "minimal.dependent",
    ]
    rc, stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0
    dep_drv = stdout.strip()

    cmd = [
        CLIENT_BIN.parent / "nix-store",
        "-q",
        "--outputs",
        dep_drv,
    ]
    rc, stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0
    dep_path = stdout.strip()

    # No nix3 equivalent exists for --referrers (no `nix store referrers`).
    cmd = [
        CLIENT_BIN.parent / "nix-store",
        "--store",
        uri,
        "-q",
        "--referrers",
        out_path,
    ]
    rc, stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0
    referrers = [line.strip() for line in stdout.splitlines()]
    assert dep_path in referrers


async def test_query_path_from_hash_part(
    profiler: pyinstrument.Profiler,
    query_env: tuple[Server, str, str],
) -> None:
    """Verify QueryPathFromHashPart via 'nix store path-from-hash-part'.

    Store operations triggered:
    - QueryPathFromHashPart: Queries path from hash part
    """
    _server, uri, out_path = query_env

    # out_path is like /nix/store/hash-name
    m = re.match(r"/nix/store/([a-z0-9]+)-", out_path)
    assert m
    hash_part = m.group(1)

    cmd = [
        CLIENT_BIN,
        "store",
        "path-from-hash-part",
        "--store",
        uri,
        hash_part,
    ]
    rc, stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0
    assert stdout.strip() == out_path


@pytest.mark.legacy_nix_commands
async def test_query_valid_derivers(profiler: pyinstrument.Profiler, query_env: tuple[Server, str, str]) -> None:
    """Verify QueryValidDerivers via 'nix-store -q --deriver'.

    Store operations triggered:
    - QueryValidDerivers: Queries valid derivers
    """
    _server, uri, out_path = query_env

    # Could use `nix path-info --derivation <store-path>` as a nix3 equivalent,
    # but `nix-store -q --deriver` is kept for consistency with other query tests.
    cmd = [
        CLIENT_BIN.parent / "nix-store",
        "--store",
        uri,
        "-q",
        "--deriver",
        out_path,
    ]
    rc, stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0
    deriver = stdout.strip()
    assert deriver.endswith(".drv")
    assert deriver.startswith("/nix/store/")


async def test_query_missing(profiler: pyinstrument.Profiler, query_env: tuple[Server, str, str]) -> None:
    """Verify QueryMissing via 'nix build --dry-run'.

    Store operations triggered:
    - QueryMissing: Queries missing paths
    """
    _server, uri, _out_path = query_env

    test_nix = TEST_NIX
    cmd = [
        CLIENT_BIN,
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--impure",
        "--file",
        test_nix,
        "minimal.leaf",
        "--dry-run",
    ]
    rc, _stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0


@pytest.mark.legacy_nix_commands
async def test_find_roots(profiler: pyinstrument.Profiler, query_env: tuple[Server, str, str]) -> None:
    """Verify FindRoots via 'nix-store --gc --print-roots'.

    Store operations triggered:
    - None: This test only checks roots without triggering Store operations
    """
    _server, uri, _out_path = query_env

    # No nix3 equivalent exists for --print-roots (`nix store gc` has no such f…
    cmd = [
        CLIENT_BIN.parent / "nix-store",
        "--store",
        uri,
        "--gc",
        "--print-roots",
    ]
    rc, _stdout, _stderr, stdboth = await run_subproc(cmd)
    # If it fails with host resolution error, it's a nix-store limitation with URI ports.
    if rc != 0 and "Could not resolve hostname" in stdboth:
        pytest.skip("nix-store does not support ports in URIs for this command")
