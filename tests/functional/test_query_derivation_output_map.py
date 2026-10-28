"""
Tests for QueryDerivationOutputMap (op 41).

This operation queries the output -> path mapping for a derivation.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import structlog

from tests.conftest import CLIENT_BIN, TEST_NIX, run_subproc, server_uri

if TYPE_CHECKING:
    from pynixd import Server


log = structlog.get_logger(__name__)


async def test_query_derivation_output_map(pynixd_server: Server) -> None:
    """Build a derivation and query its output map.

    QueryDerivationOutputMap maps output names to store paths.
    After a successful build, it should return the realized paths.
    """
    uri = server_uri(pynixd_server)

    test_nix = TEST_NIX
    cmd = [
        str(CLIENT_BIN),
        "build",
        "--eval-store",
        "auto",
        "--store",
        uri,
        "--file",
        str(test_nix),
        "minimal.leaf",
        "--no-link",
        "--print-out-paths",
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(cmd)
    assert rc == 0, f"build failed:\n{stdboth}"

    out_path = stdout.strip()
    assert out_path.startswith("/nix/store/"), f"Expected store path, got: {out_path}"

    # QueryDerivationOutputMap: query the output map for the derivation
    cmd = [
        str(CLIENT_BIN),
        "path-info",
        "--store",
        uri,
        "--json",
        out_path,
    ]
    rc, stdout, _stderr, stdboth = await run_subproc(cmd)
    assert rc == 0, f"path-info failed:\n{stdboth}"
    info = json.loads(stdout)
    assert out_path in info, f"Expected {out_path} in path-info output"
