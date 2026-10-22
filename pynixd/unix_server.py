"""
Unix socket server that speaks the nix-daemon protocol.

Accepts connections on a Unix domain socket and spawns a DaemonProxy
for each client. Used for testing (avoids SSH) and local daemon mode.
"""

from __future__ import annotations

import asyncio
import os
import socket
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
import structlog

from .config import ScheduleMode
from .hangup import wait_for_socket_hangup
from .proxy import DaemonProxy
from .trust import PeerRefusedError, authorise, peer_of
from .wire import UnixNixReader, UnixNixWriter

if TYPE_CHECKING:
    from pathlib import Path

    from .context import PynixdContext

log = structlog.get_logger(__name__)

SD_LISTEN_FDS_START = 3


def inherited_listener(socket_path: Path) -> socket.socket | None:
    """The socket systemd passed for `socket_path`, if it passed one.

    `sd_listen_fds(3)`: descriptors from 3 on, for this pid only. A socket
    unit binds the path before pynixd starts, so a client that connects
    early waits in the backlog. Without it, a user's Nix finds no daemon
    and fails on the store it cannot open (#59).
    """
    if os.environ.get("LISTEN_PID") != str(os.getpid()):
        return None
    count = int(os.environ.get("LISTEN_FDS", "0"))
    for fd in range(SD_LISTEN_FDS_START, SD_LISTEN_FDS_START + count):
        sock = socket.socket(fileno=fd)
        if sock.family == socket.AF_UNIX and sock.getsockname() == str(socket_path):
            return sock
        sock.detach()
    return None


async def start_unix_server(
    ctx: PynixdContext,
    socket_path: Path,
    schedule_mode: ScheduleMode | None = None,
) -> asyncio.Server:
    """Start a Unix socket server.

    Args:
        ctx: Shared application context
        socket_path: Path for the Unix domain socket
        schedule_mode: Scheduling mode for this listener

    Returns:
        The asyncio.Server instance.
    """

    async def handle_client(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = peer_of(writer.get_extra_info("socket"))
        try:
            # A thread, because NSS may answer from the network.
            role, user = await anyio.to_thread.run_sync(authorise, peer, ctx.trust)
        except PeerRefusedError as ex:
            # nix-daemon closes the socket before the handshake, and the
            # client reads end-of-file.
            log.warning("unix_client_refused", pid=peer.pid, uid=peer.uid, reason=str(ex))
            writer.close()
            return
        log.info("unix_client_connected", pid=peer.pid, user=user, role=role.name)
        fd: int = writer.get_extra_info("socket").fileno()
        try:
            proxy = DaemonProxy(
                UnixNixReader(reader, identifier="client"),
                UnixNixWriter(writer, identifier="client"),
                ctx=ctx,
                role=role,
                username=user or "<unknown>",
                schedule_mode=schedule_mode or ScheduleMode.auto,
                transport="unix",
                hangup=lambda: wait_for_socket_hangup(fd),
                pid=peer.pid,
            )
            await proxy.run()
        except Exception:
            log.exception("unix_proxy_session_failed")
        finally:
            writer.close()

    inherited = inherited_listener(socket_path)
    if inherited is not None:
        # The path is systemd's: removing it on close would break the
        # socket unit for the next start.
        server = await asyncio.start_unix_server(handle_client, sock=inherited, limit=2**18, cleanup_socket=False)
        log.info("unix_server_listening", socket_path=socket_path, inherited=True)
        return server

    # Clean up stale socket
    sock = anyio.Path(socket_path)
    if await sock.exists():
        await sock.unlink()

    server = await asyncio.start_unix_server(handle_client, path=str(socket_path), limit=2**18)
    await sock.chmod(0o666)
    log.info("unix_server_listening", socket_path=socket_path)
    return server
