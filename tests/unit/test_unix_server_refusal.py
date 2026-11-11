"""A peer outside `allowed-users` reads Determinate Nix's access-denied magic, then end-of-file."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio
import pytest

from nix_daemon_protocol.constants import WORKER_MAGIC_1, WORKER_MAGIC_ACCESS_DENIED
from pynixd.trust import TrustPolicy
from pynixd.unix_server import start_unix_server


async def test_a_refused_peer_reads_the_access_denied_magic(tmp_path: Path) -> None:
    """`src/nix/unix/daemon.cc:384` of Determinate v3.24.0 writes it before the close."""
    ctx: Any = SimpleNamespace(trust=TrustPolicy(trusted_users=(), allowed_users=()))
    socket_path = tmp_path / "daemon.sock"
    async with anyio.create_task_group() as tg:

        def spawn(fn: Any, *args: Any) -> None:
            tg.start_soon(fn, *args)

        server = await start_unix_server(ctx, socket_path, spawn)
        async with await anyio.connect_unix(socket_path) as client:
            await client.send(WORKER_MAGIC_1.to_bytes(8, "little"))
            assert await client.receive(8) == WORKER_MAGIC_ACCESS_DENIED.to_bytes(8, "little")
            with pytest.raises(anyio.EndOfStream):
                await client.receive()
        await server.aclose()
