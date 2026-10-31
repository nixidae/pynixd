"""
Unix socket server that speaks the nix-daemon protocol.

Accepts connections on a Unix domain socket and spawns a DaemonProxy
for each client. Used for testing (avoids SSH) and local daemon mode.
"""

from __future__ import annotations

import contextlib
import os
import socket
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
import structlog
from anyio.abc import SocketListener

from .config import ScheduleMode
from .hangup import wait_for_socket_hangup
from .proxy import DaemonProxy
from .trust import PeerRefusedError, authorise, peer_of
from .unix_socket import as_link
from .wire import UnixNixReader, UnixNixWriter

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from pathlib import Path

    from anyio.abc import SocketStream

    from .context import PynixdContext

    type Spawn = Callable[..., None]

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


class UnixServer:
    """The listener, and the loop that hands each client to the owner's task group.

    Not `listener.serve`: its own task group cancels every client when the
    listener closes, and shutdown closes the listener first and answers the
    clients after (`Instance.close`, issue #64).
    """

    def __init__(self, listener: SocketListener, socket_path: Path, *, owns_path: bool) -> None:
        self.listener = listener
        self.socket_path = socket_path
        self.owns_path = owns_path
        self._accepting = anyio.CancelScope()
        self._accept_ended = anyio.Event()

    async def accept_loop(self, spawn: Spawn, handle: Callable[[SocketStream], Awaitable[None]]) -> None:
        try:
            with self._accepting:
                while True:
                    try:
                        stream = await self.listener.accept()
                    except anyio.BrokenResourceError as ex:
                        # EMFILE and the like: asyncio's server waits a second
                        # and accepts again, and so does this one.
                        log.warning("unix_accept_failed", error=str(ex.__cause__ or ex))
                        await anyio.sleep(1)
                        continue
                    spawn(handle, stream)
        finally:
            self._accept_ended.set()

    async def aclose(self) -> None:
        # The accept ends before the socket closes: anyio's listener leaves a
        # waiting accept registered on the closed descriptor otherwise.
        self._accepting.cancel()
        await self._accept_ended.wait()
        await self.listener.aclose()
        # The path is systemd's when it passed the socket: removing it would
        # break the socket unit for the next start.
        if self.owns_path:
            with contextlib.suppress(FileNotFoundError):
                await anyio.Path(self.socket_path).unlink()


async def start_unix_server(
    ctx: PynixdContext,
    socket_path: Path,
    spawn: Spawn,
    schedule_mode: ScheduleMode | None = None,
) -> UnixServer:
    """Listen on `socket_path`, and run each client session through *spawn*."""

    async def handle_client(accepted: SocketStream) -> None:
        # An error here must not reach the owner's task group: it would end
        # every other session with it.
        try:
            link = await as_link(accepted)
        except Exception:
            log.exception("unix_client_accept_failed")
            await accepted.aclose()
            return
        stream, sock = link.stream, link.sock
        try:
            peer = peer_of(sock)
            # A thread, because NSS may answer from the network.
            role, user = await anyio.to_thread.run_sync(authorise, peer, ctx.trust)
        except PeerRefusedError as ex:
            # nix-daemon closes the socket before the handshake, and the
            # client reads end-of-file.
            log.warning("unix_client_refused", pid=peer.pid, uid=peer.uid, reason=str(ex))
            await stream.aclose()
            return
        except Exception:
            log.exception("unix_client_authorise_failed")
            await stream.aclose()
            return
        log.info("unix_client_connected", pid=peer.pid, user=user, role=role.name)
        fd = sock.fileno()
        try:
            proxy = DaemonProxy(
                UnixNixReader(link, identifier="client"),
                UnixNixWriter(link, identifier="client"),
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
            anyio.notify_closing(sock)
            await stream.aclose()

    inherited = inherited_listener(socket_path)
    if inherited is not None:
        server = UnixServer(await SocketListener.from_socket(inherited), socket_path, owns_path=False)
        log.info("unix_server_listening", socket_path=socket_path, inherited=True)
    else:
        # `create_unix_listener` removes a stale socket at the path first.
        listener = await anyio.create_unix_listener(socket_path, mode=0o666)
        server = UnixServer(listener, socket_path, owns_path=True)
        log.info("unix_server_listening", socket_path=socket_path)
    spawn(server.accept_loop, spawn, handle_client)
    return server
