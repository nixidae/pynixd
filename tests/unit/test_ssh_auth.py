"""The SSH listener admits listed keys only, and a key carries its role. Issue #94.

`_NixSSHServer` answered `True` to every password and every key, and the
role came from the username, so a stranger who connected as an admin user
got a trusted session. asyncssh accepts a key from `authorized_client_keys`
itself and asks the server about every other key, so that answer was the
whole gate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import asyncssh
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import PynixdSettings
from pynixd.connection import Connection
from pynixd.context import PynixdContext
from pynixd.serde.auth import Role
from pynixd.ssh_server import start_ssh_server
from pynixd.wire import SSHNixReader, SSHNixWriter
from tests._conftest.ssh_keys import make_ssh_test_keys
from tests.functional.mock_store import MockStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from asyncssh import SSHAcceptor

    from tests._conftest.ssh_keys import SSHTestKeys


async def _role_of(
    server: SSHAcceptor,
    ctx: PynixdContext,
    *,
    key: asyncssh.SSHKey | str | None,
    password: str | None = None,
) -> Role:
    # `admin` names an admin, and grants nothing: the key decides.
    async with asyncssh.connect(
        "127.0.0.1",
        server.get_port(),
        username="admin",
        client_keys=[key] if key is not None else None,
        password=password,
        known_hosts=None,
        agent_path=None,
    ) as ssh:
        process = await ssh.create_process("nix-daemon --stdio", encoding=None)
        conn = Connection(
            SSHNixReader(process.stdout, identifier="t"), SSHNixWriter(process.stdin, identifier="t"), "t"
        )
        await conn.connect()
        (proxy,) = ctx.sessions.proxies()
        role = proxy.role
        process.stdin.write_eof()
        return role


@pytest.fixture
async def listener(tmp_path: Path) -> AsyncIterator[tuple[SSHAcceptor, PynixdContext, SSHTestKeys]]:
    keys = make_ssh_test_keys(tmp_path)
    store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(settings=PynixdSettings(), _stores={StoreId("local"): store})
    server = await start_ssh_server(ctx, port=0, user_keys=[keys.user_keys], admin_keys=[keys.admin_keys])
    try:
        yield server, ctx, keys
    finally:
        server.close()
        await server.wait_closed()


async def test_a_key_nobody_listed_is_refused(listener: tuple[SSHAcceptor, PynixdContext, SSHTestKeys]) -> None:
    server, ctx, _keys = listener
    with pytest.raises(asyncssh.PermissionDenied):
        await _role_of(server, ctx, key=asyncssh.generate_private_key("ssh-ed25519"))


async def test_a_password_is_refused(listener: tuple[SSHAcceptor, PynixdContext, SSHTestKeys]) -> None:
    server, ctx, _keys = listener
    with pytest.raises(asyncssh.PermissionDenied):
        await _role_of(server, ctx, key=None, password="anything")


async def test_a_listed_key_is_a_user_whatever_name_it_claims(
    listener: tuple[SSHAcceptor, PynixdContext, SSHTestKeys],
) -> None:
    server, ctx, keys = listener
    assert await _role_of(server, ctx, key=str(keys.user)) == Role.USER


async def test_an_admin_key_is_an_admin(listener: tuple[SSHAcceptor, PynixdContext, SSHTestKeys]) -> None:
    server, ctx, keys = listener
    assert await _role_of(server, ctx, key=str(keys.admin)) == Role.ADMIN


async def test_deprecated_admin_users_raises_only_a_listed_key(tmp_path: Path) -> None:
    """A config that still sets `admin_users` keeps its trusted deploys,
    and a stranger who claims the name still gets nothing."""
    keys = make_ssh_test_keys(tmp_path)
    store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(settings=PynixdSettings(), _stores={StoreId("local"): store})
    server = await start_ssh_server(ctx, port=0, user_keys=[keys.user_keys], admin_users={"admin"})
    try:
        assert await _role_of(server, ctx, key=str(keys.user)) == Role.ADMIN
        with pytest.raises(asyncssh.PermissionDenied):
            await _role_of(server, ctx, key=asyncssh.generate_private_key("ssh-ed25519"))
    finally:
        server.close()
        await server.wait_closed()


async def test_a_key_added_to_a_file_admits_the_next_connection(
    listener: tuple[SSHAcceptor, PynixdContext, SSHTestKeys],
) -> None:
    """No watcher and no restart: the files are read at each authentication."""
    server, ctx, keys = listener
    newcomer = asyncssh.generate_private_key("ssh-ed25519")
    with pytest.raises(asyncssh.PermissionDenied):
        await _role_of(server, ctx, key=newcomer)

    with keys.user_keys.open("ab") as listed:
        listed.write(newcomer.export_public_key())

    assert await _role_of(server, ctx, key=newcomer) == Role.USER
