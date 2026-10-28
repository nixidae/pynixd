"""Test PSIMonitorSFTPServer restricts access to PSI/cgroup monitoring files."""

from __future__ import annotations

from typing import TYPE_CHECKING

import asyncssh
from asyncssh.constants import FX_NO_SUCH_FILE, FX_PERMISSION_DENIED

from tests._conftest.ssh_keys import SESSION_SSH_KEYS

if TYPE_CHECKING:
    from pynixd import Server


class _SFTPSession:
    """Holds both SSH connection and SFTP client for proper cleanup."""

    def __init__(self, conn: asyncssh.SSHClientConnection, sftp: asyncssh.SFTPClient) -> None:
        self._conn = conn
        self._sftp = sftp

    async def __aenter__(self) -> asyncssh.SFTPClient:
        return self._sftp

    async def __aexit__(self, *args: object) -> None:
        self._sftp.exit()
        await self._sftp.wait_closed()
        self._conn.close()
        await self._conn.wait_closed()


async def _sftp_client(server: Server) -> _SFTPSession:
    conn = await asyncssh.connect(
        "127.0.0.1",
        server.port,
        known_hosts=None,
        username="test",
        client_keys=[str(SESSION_SSH_KEYS / "id_user")],
        agent_path=None,
    )
    sftp = await conn.start_sftp_client()
    return _SFTPSession(conn, sftp)


async def test_sftp_stat_allowed_path(pynixd_server: Server) -> None:
    """stat() should succeed on allowed paths like /proc/meminfo."""
    async with await _sftp_client(pynixd_server) as sftp:
        attrs = await sftp.stat("/proc/meminfo")
        assert attrs.permissions is not None


async def test_sftp_stat_denied_path(pynixd_server: Server) -> None:
    """stat() should fail on non-allowed paths."""
    async with await _sftp_client(pynixd_server) as sftp:
        try:
            await sftp.stat("/etc/passwd")
            raise AssertionError("Expected SFTPError for denied path")
        except asyncssh.SFTPError as e:
            assert e.code == FX_NO_SUCH_FILE  # noqa: PT017


async def test_sftp_stat_allowed_dir(pynixd_server: Server) -> None:
    """stat() should succeed on allowed directories like /proc."""
    async with await _sftp_client(pynixd_server) as sftp:
        attrs = await sftp.stat("/proc")
        assert attrs is not None


async def test_sftp_scandir_allowed_dir(pynixd_server: Server) -> None:
    """scandir() on /proc/pressure should only return allowed entries."""
    async with await _sftp_client(pynixd_server) as sftp:
        names = [entry.filename async for entry in sftp.scandir("/proc/pressure")]
        assert b"cpu" in names or "cpu" in names


async def test_sftp_scandir_denied_dir(pynixd_server: Server) -> None:
    """scandir() on a non-allowed directory should fail."""
    async with await _sftp_client(pynixd_server) as sftp:
        try:
            await sftp.listdir("/etc")
            raise AssertionError("Expected SFTPError for denied path")
        except asyncssh.SFTPError as e:
            assert e.code == FX_NO_SUCH_FILE  # noqa: PT017


async def test_sftp_read_allowed_file(pynixd_server: Server) -> None:
    """Reading /proc/meminfo (size=0 virtual file) requires explicit read size.

    Virtual files in /proc report size 0 via stat, so the SFTP client's
    read() with no size argument returns empty data. This matches how
    pynixd's PSI monitor reads these files: stat first, then loop-read
    if size is 0.
    """
    async with await _sftp_client(pynixd_server) as sftp:
        async with sftp.open("/proc/meminfo", encoding=None) as f:
            data: bytes = await f.read(65536)
        assert b"MemTotal" in data


async def test_sftp_read_denied_file(pynixd_server: Server) -> None:
    """Opening a non-allowed file should fail."""
    async with await _sftp_client(pynixd_server) as sftp:
        try:
            await sftp.open("/etc/passwd", encoding=None)
            raise AssertionError("Expected SFTPError for denied path")
        except asyncssh.SFTPError as e:
            assert e.code == FX_NO_SUCH_FILE  # noqa: PT017


async def test_sftp_write_denied(pynixd_server: Server) -> None:
    """Write operations should always fail with permission denied."""
    async with await _sftp_client(pynixd_server) as sftp:
        try:
            await sftp.setstat("/proc/meminfo", asyncssh.SFTPAttrs())
            raise AssertionError("Expected SFTPError for setstat")
        except asyncssh.SFTPError as e:
            assert e.code == FX_PERMISSION_DENIED  # noqa: PT017

        try:
            await sftp.mkdir("/tmp/malicious")
            raise AssertionError("Expected SFTPError for mkdir")
        except asyncssh.SFTPError as e:
            assert e.code == FX_PERMISSION_DENIED  # noqa: PT017

        try:
            await sftp.rmdir("/proc")
            raise AssertionError("Expected SFTPError for rmdir")
        except asyncssh.SFTPError as e:
            assert e.code == FX_PERMISSION_DENIED  # noqa: PT017

        try:
            await sftp.remove("/proc/meminfo")
            raise AssertionError("Expected SFTPError for remove")
        except asyncssh.SFTPError as e:
            assert e.code == FX_PERMISSION_DENIED  # noqa: PT017

        try:
            await sftp.rename("/proc/meminfo", "/proc/meminfo2")
            raise AssertionError("Expected SFTPError for rename")
        except asyncssh.SFTPError as e:
            assert e.code == FX_PERMISSION_DENIED  # noqa: PT017
