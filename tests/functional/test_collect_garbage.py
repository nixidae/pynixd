"""
Tests for the CollectGarbage daemon operation.

GC operation (op 20) requires admin privileges and is forwarded to the
remote daemon. Tests verify:
- Admin users can GC successfully
- Non-admin users get an error
- GC correctly reclaims storage
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from tests.conftest import (
    CLIENT_BIN,
    run_subproc,
    ssh_admin_uri,
    ssh_user_uri,
    unix_session_uri,
)

if TYPE_CHECKING:
    from pynixd import Server


log = structlog.get_logger(__name__)


async def test_collect_garbage_admin(pynixd_server: Server) -> None:
    """GC as admin user should succeed.

    Uses the SSH admin-user to trigger GC on the remote store.
    """
    uri = ssh_admin_uri(pynixd_server)
    cmd = [str(CLIENT_BIN), "store", "gc", "--store", uri, "--max", "0"]
    rc, stdout, stderr, stdboth = await run_subproc(cmd)
    assert rc == 0, f"GC as admin failed:\n{stdboth}"
    # GC should report freed bytes or "0 bytes"
    assert "freed" in stdout.lower() or "freed" in stderr.lower(), f"Unexpected GC output:\n{stdboth}"


async def test_collect_garbage_non_admin(pynixd_server: Server) -> None:
    """GC as an untrusted user succeeds: Nix allows it, `daemon.cc:735`. Issue #56."""
    uri = ssh_user_uri(pynixd_server)
    cmd = [str(CLIENT_BIN), "store", "gc", "--store", uri, "--max", "0"]
    rc, _stdout, _stderr, stdboth = await run_subproc(cmd)
    assert rc == 0, f"GC as an untrusted user failed:\n{stdboth}"


async def test_collect_garbage_unix_admin(pynixd_server: Server) -> None:
    """GC over Unix socket (implicit admin) should succeed."""
    uri = unix_session_uri(pynixd_server)
    cmd = [str(CLIENT_BIN), "store", "gc", "--store", uri, "--max", "0"]
    rc, _stdout, _stderr, stdboth = await run_subproc(cmd)
    assert rc == 0, f"GC over Unix socket failed:\n{stdboth}"
