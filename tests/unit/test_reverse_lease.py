"""The controller tells a builder it admitted by token how long it may idle: `pynixd-lease`. nixidae/nixidae#1198.

A real SSH session on loopback, the builder side as the reverse initiator serves it.
"""

from __future__ import annotations

import contextlib
import json
from typing import TYPE_CHECKING

import asyncssh
import pytest

from pynixd.config import PynixdSettings, ReverseInitiatorSettings
from pynixd.context import PynixdContext
from pynixd.provisioning.ticket import LEASE_COMMAND, Lease
from pynixd.reverse_client import ReverseInitiator, _ReverseSSHServer
from pynixd.reverse_server import send_lease

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable
    from pathlib import Path

LEASE = Lease(idle_seconds=95.0, boot_seconds=95.0)


@contextlib.asynccontextmanager
async def _builder(
    handler: Callable[[asyncssh.SSHServerProcess[bytes]], Awaitable[None]],
) -> AsyncIterator[asyncssh.SSHClientConnection]:
    """An SSH server that serves *handler*, and the controller's connection to it."""
    server = await asyncssh.listen(
        "127.0.0.1",
        0,
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        server_factory=_ReverseSSHServer,
        process_factory=handler,
        encoding=None,
    )
    try:
        port = server.sockets[0].getsockname()[1]
        async with asyncssh.connect(
            "127.0.0.1",
            port,
            username="controller",
            known_hosts=None,
            client_keys=[asyncssh.generate_private_key("ssh-ed25519")],
            encoding=None,
        ) as conn:
            yield conn
    finally:
        server.close()
        await server.wait_closed()


def _initiator(lease: Path | None) -> ReverseInitiator:
    settings = ReverseInitiatorSettings(enabled=True, systems=["x86_64-linux"], idle_lease=lease)
    # The context serves daemon sessions; a lease reaches none.
    return ReverseInitiator(PynixdContext(settings=PynixdSettings(), _stores={}), settings)


async def test_the_builder_writes_the_lease_where_its_guard_reads_it(tmp_path: Path) -> None:
    path = tmp_path / "idle" / "lease.json"
    async with _builder(_initiator(path)._handle_request) as conn:
        assert await send_lease(conn, "provisioned-p1", LEASE)

    assert Lease.model_validate_json(path.read_text()) == LEASE
    assert json.loads(path.read_text())["idle_seconds"] == 95.0, "the guard reads this key"
    assert not (path.parent / ".lease.json.partial").exists()


async def test_a_builder_with_no_lease_path_takes_the_lease_and_writes_nothing(tmp_path: Path) -> None:
    async with _builder(_initiator(None)._handle_request) as conn:
        assert await send_lease(conn, "provisioned-p1", LEASE)
    assert list(tmp_path.iterdir()) == []


async def test_a_text_that_is_no_lease_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "lease.json"
    async with _builder(_initiator(path)._handle_request) as conn:
        result = await conn.run(LEASE_COMMAND, input=b'{"idle_seconds": "soon"}')
    assert result.exit_status == 1
    assert isinstance(result.stderr, bytes)
    assert b"not a lease" in result.stderr
    assert not path.exists()


async def test_an_older_builder_refuses_the_command_and_the_controller_goes_on() -> None:
    async def older(process: asyncssh.SSHServerProcess[bytes]) -> None:
        # What a builder without `pynixd-lease` answers: `_handle_request`'s unsupported command.
        process.stderr.write(b"pynixd: unsupported command\n")
        process.exit(1)

    async with _builder(older) as conn:
        assert not await send_lease(conn, "provisioned-p1", LEASE)


async def test_a_builder_that_never_answers_costs_the_timeout_only(monkeypatch: pytest.MonkeyPatch) -> None:
    async def silent(process: asyncssh.SSHServerProcess[bytes]) -> None:
        await process.wait_closed()

    monkeypatch.setattr("pynixd.reverse_server.LEASE_TIMEOUT", 0.5)
    async with _builder(silent) as conn:
        assert not await send_lease(conn, "provisioned-p1", LEASE)
