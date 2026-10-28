"""Test Role-Based Access Control (RBAC)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
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


@pytest.mark.xfail(reason="nix 2.34.7 returns exit code 0 even on access-denied errors")
async def test_rbac_ssh_admin_vs_user(pynixd_server: Server) -> None:
    """Verify that SSH admin can GC, but SSH user cannot.

    Store operations triggered:
    - None: This test only checks RBAC authentication/authorization without triggering Store operations
    """
    uri_user = ssh_user_uri(pynixd_server)
    cmd_user = [str(CLIENT_BIN), "store", "gc", "--store", uri_user]
    rc_user, _stdout_user, _stderr_user, stdboth_user = await run_subproc(
        cmd_user,
        expected_retcode=None,
    )
    assert rc_user != 0
    assert "requires administrative privileges" in stdboth_user

    uri_admin = ssh_admin_uri(pynixd_server)
    cmd_admin = [str(CLIENT_BIN), "store", "gc", "--store", uri_admin]
    rc_admin, _stdout_admin, _stderr_admin, _stdboth_admin = await run_subproc(cmd_admin)
    assert rc_admin == 0


async def test_rbac_unix_implicit_admin(pynixd_server: Server) -> None:
    """Verify that Unix socket connections are implicit admins.

    Store operations triggered:
    - None: This test only checks RBAC authentication/authorization without triggering Store operations
    """
    uri = unix_session_uri(pynixd_server)
    cmd = [str(CLIENT_BIN), "store", "gc", "--store", uri]
    rc, _stdout, _stderr, _stdboth = await run_subproc(cmd)
    assert rc == 0
