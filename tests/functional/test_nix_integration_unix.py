from __future__ import annotations

import contextlib
from pathlib import Path

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.serde import IsValidPathRequest, StorePath
from pynixd.store import LocalSocketStore
from tests.conftest import CLIENT_BIN, make_test_spec, run_subproc

"""
End-to-End Nix Integration Tests via Unix Socket

These tests use the real `nix` binary to perform builds against a
running pynixd server via its Unix socket. This verifies the
daemon protocol proxying logic without SSH complexity.
"""


@pytest.fixture
async def pynixd_server(tmp_path: Path):
    """Start a pynixd server listening on a Unix socket."""
    store_path = tmp_path / "store"
    store_path.mkdir()
    socket_path = tmp_path / "pynixd.sock"

    local_store = LocalSocketStore(
        make_test_spec(store_id="local", store_path=store_path, no_probe=True),
    )

    async with Server(
        stores={StoreId("local"): local_store},
        unix_path=socket_path,
        ssh_port=None,  # Disable SSH
        http_port=None,
    ) as server:
        yield server, socket_path, store_path


async def test_nix_build_via_unix(pynixd_server):
    """Verify that 'nix build' works when using pynixd via Unix socket."""
    server, socket_path, store_path = pynixd_server

    # Construction of Unix URI: unix:///path/to/socket?root=/path/to/store
    uri = f"unix://{socket_path}?root={store_path}"

    nix_expr = """
    with import <nixpkgs> {};
    runCommand "pynixd-test" {
        ts = builtins.currentTime;
    } "echo 'hello from pynixd' > $out"
    """
    expr_path = Path("/tmp/pynixd-it-test.nix")
    expr_path.write_text(nix_expr)  # noqa: ASYNC240 — test setup

    try:
        cmd = [
            str(CLIENT_BIN),
            "build",
            "--file",
            str(expr_path),
            "--store",
            uri,
            "--no-link",
            "--print-out-paths",
            "--impure",
        ]

        rc, stdout, _stderr, _stdboth = await run_subproc(cmd)
        assert rc == 0
        assert "/nix/store/" in stdout
        out_path = stdout.strip()
        resp = await server.local_store.execute(IsValidPathRequest(path=StorePath(path=out_path)))
        assert resp.valid
    finally:
        with contextlib.suppress(OSError):
            expr_path.unlink()  # noqa: ASYNC240 — test cleanup


@pytest.mark.legacy_nix_commands
async def test_nix_copy_via_unix(pynixd_server, tmp_path: Path):
    """Verify 'nix copy' works against pynixd via Unix socket."""
    server, socket_path, store_path = pynixd_server
    uri = f"unix://{socket_path}?root={store_path}"

    dummy_file = tmp_path / "dummy"
    dummy_file.write_text("pynixd-copy-test")

    # Setup: add to system store
    rc, stdout, _stderr, _stdboth = await run_subproc(
        [str(CLIENT_BIN.parent / "nix-store"), "--add", str(dummy_file)],
    )
    assert rc == 0
    system_path = stdout.strip()

    cmd = [str(CLIENT_BIN), "copy", "--to", uri, system_path]

    rc, stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0

    # Verify it exists in pynixd's local store
    resp = await server.local_store.execute(IsValidPathRequest(path=StorePath(path=system_path)))
    assert resp.valid
