"""
asyncssh SSH server that speaks the nix-daemon protocol.

Accepts SSH connections, authenticates, and spawns a DaemonProxy
for each `nix-daemon --stdio` exec request. Also provides an SFTP
subsystem restricted to PSI/cgroup monitoring files.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio
import asyncssh
import structlog

from . import wire
from .config import ScheduleMode
from .constants import SSH_ENCRYPTION_ALGS, SSH_KEEPALIVE_COUNT_MAX, SSH_KEEPALIVE_INTERVAL
from .hangup import ssh_hangup
from .proxy import DaemonProxy
from .serde.auth import Role
from .sftp_server import PSIMonitorSFTPServer
from .wire import SSHNixReader, SSHNixWriter

if TYPE_CHECKING:
    from pathlib import Path

    from .context import PynixdContext

log = structlog.get_logger(__name__)


_ROLE_INFO = "pynixd_role"
"""The connection's extra-info key that carries the role its key earned."""


class SSHKeyring:
    """The keys the SSH listener admits, and the role each one earns.

    A key in an `admin_keys` file is `Role.ADMIN`, the way a user in Nix's
    `trusted-users` is; a key in a `user_keys` file is `Role.USER`; any
    other key is refused. The username decides nothing, because a client
    names any user it likes (#94) -- except through the deprecated
    `admin_users`, which can only raise a key that is already listed.

    The files are read at each authentication, so an edit applies to the
    next connection with no watcher and no restart. They are a few lines
    each, and a client authenticates once per connection.
    """

    def __init__(self, user_keys: list[Path], admin_keys: list[Path], admin_users: set[str] | None = None) -> None:
        self._files = [(Role.ADMIN, path) for path in admin_keys] + [(Role.USER, path) for path in user_keys]
        self._admin_users = admin_users or set()

    async def role_of(self, key: asyncssh.SSHKey, peer: str, username: str) -> Role | None:
        role = await self._listed_role(key, peer)
        if role == Role.USER and username in self._admin_users:
            log.warning("ssh_admin_users_deprecated", username=username, fix="list the key in ssh_admin_keys")
            return Role.ADMIN
        return role

    async def _listed_role(self, key: asyncssh.SSHKey, peer: str) -> Role | None:
        for role, path in self._files:
            try:
                text = await anyio.Path(path).read_text()
            except FileNotFoundError:
                continue
            if not text.strip():
                continue
            try:
                listed = asyncssh.import_authorized_keys(text)
            except ValueError:
                log.warning("ssh_authorized_keys_unusable", path=str(path), exc_info=True)
                continue
            if listed.validate(key, peer, peer) is not None:
                return role
        return None


class _NixSSHServer(asyncssh.SSHServer):
    """Admit the keys of the keyring, and nobody else.

    asyncssh's own `authorized_client_keys` stays unset: a key it lists
    gets in without asking this class, and so without a role.
    """

    def __init__(self, keyring: SSHKeyring) -> None:
        self._keyring = keyring
        self._conn: asyncssh.SSHServerConnection | None = None

    def connection_made(self, conn: asyncssh.SSHServerConnection) -> None:
        self._conn = conn

    def password_auth_supported(self) -> bool:
        return False

    def public_key_auth_supported(self) -> bool:
        return True

    async def validate_public_key(self, username: str, key: asyncssh.SSHKey) -> bool:
        conn = self._conn
        if conn is None:
            return False
        peer = str((conn.get_extra_info("peername") or ("",))[0])
        role = await self._keyring.role_of(key, peer, username)
        log.info(
            "ssh_key_auth",
            username=username,
            key_fingerprint=key.get_fingerprint(),
            role=None if role is None else role.name,
        )
        if role is None:
            return False
        conn.set_extra_info(**{_ROLE_INFO: role})
        return True


async def start_ssh_server(
    ctx: PynixdContext,
    host: str = "127.0.0.1",
    port: int = 0,
    host_key_path: Path | None = None,
    schedule_mode: ScheduleMode | None = None,
    user_keys: list[Path] | None = None,
    admin_keys: list[Path] | None = None,
    admin_users: set[str] | None = None,
) -> asyncssh.SSHAcceptor:
    """Start the SSH server.

    Args:
        ctx: Shared application context
        host: Listen address
        port: Listen port (0 for random available port)
        host_key_path: Path to SSH host key
        schedule_mode: Scheduling mode for this listener
        user_keys: `authorized_keys` files whose keys get `Role.USER`
        admin_keys: `authorized_keys` files whose keys get `Role.ADMIN`
        admin_users: Deprecated. Usernames that raise a `user_keys` key to
            `Role.ADMIN`.

    Returns:
        The asyncssh acceptor instance.
    """
    keyring = SSHKeyring(user_keys or [], admin_keys or [], admin_users)
    if admin_users:
        log.warning("ssh_admin_users_deprecated", admin_users=sorted(admin_users), fix="use ssh_admin_keys")
    if not user_keys and not admin_keys:
        log.warning("ssh_no_client_keys", reason="the SSH listener admits nobody")

    # Load or generate host key
    if host_key_path and host_key_path.exists():  # noqa: ASYNC240 — one-time startup check
        host_key: asyncssh.SSHKey = asyncssh.read_private_key(str(host_key_path))
        log.info("host_key_loaded_from_file", host_key_path=host_key_path)
    else:
        host_key = asyncssh.generate_private_key("ssh-rsa", key_size=4096)
        if host_key_path:
            host_key.write_private_key(str(host_key_path))
            log.info("host_key_generated", host_key_path=host_key_path)
        else:
            log.info("host_key_ephemeral_generated")

    async def handle_client(process: asyncssh.SSHServerProcess[bytes]) -> None:
        cmd: str | None = process.command
        log.info("client_exec", cmd=cmd)
        if not cmd or ("nix-daemon" not in cmd and "nix daemon" not in cmd):
            process.stderr.write(b"pynixd: unsupported command\n")
            process.exit(1)
            return

        process.channel.set_write_buffer_limits(
            high=wire._SSH_WINDOW_SIZE,
            low=wire._SSH_WINDOW_SIZE // 4,
        )
        exit_code = 0
        username = str(process.get_extra_info("username", "unknown"))
        role: Role = process.get_extra_info(_ROLE_INFO, Role.USER)
        try:
            proxy = DaemonProxy(
                SSHNixReader(process.stdin, identifier="client"),
                SSHNixWriter(process.stdout, identifier="client"),
                ctx=ctx,
                role=role,
                username=username,
                schedule_mode=schedule_mode or ScheduleMode.auto,
                transport="ssh",
                hangup=ssh_hangup(process),
            )
            await proxy.run()
        except Exception:
            log.exception("proxy_session_failed")
            exit_code = 1
        finally:
            process.exit(exit_code)

    # Start the SSH server
    server = await asyncssh.listen(
        host,
        port,
        server_host_keys=[host_key],
        server_factory=lambda: _NixSSHServer(keyring),
        process_factory=handle_client,
        sftp_factory=PSIMonitorSFTPServer,
        encoding=None,
        encryption_algs=SSH_ENCRYPTION_ALGS,
        keepalive_interval=SSH_KEEPALIVE_INTERVAL,
        keepalive_count_max=SSH_KEEPALIVE_COUNT_MAX,
    )
    bound_port = server.get_port()
    log.info("ssh_server_listening", host=host, port=bound_port)
    return server
