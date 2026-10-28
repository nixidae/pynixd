"""Notice that a client has gone, on every transport a session runs over.

`daemon.cc:1016` of Nix runs `MonitorFdHup` on the client's descriptor for
the whole session and interrupts the running operation when the client
goes. `DaemonProxy` cancels the session on the same signal, so a request
gives its build references back. Issue #91.

Each transport gets the strongest signal it carries:

- Unix socket, Linux: `POLLHUP`, which needs both directions shut, so a
  half-close is not a hangup. The question Nix asks at `monitor-fd.hh:105`.
- Unix socket, darwin: kqueue `EV_EOF` on the read filter, as Nix does at
  `monitor-fd.hh:64`. It also fires on a half-close: kqueue has no
  `POLLHUP` that tells the two apart, and Nix accepts the same.
- SSH channel: end of file or channel close. Nix serves SSH as
  `nix-daemon --stdio` behind sshd, where stdin is a pipe, and a pipe
  raises `POLLHUP` when its writer closes. The client's ssh closes the
  channel's input when the client dies, so EOF is the hangup there.
"""

from __future__ import annotations

import select
import sys
from typing import TYPE_CHECKING

import anyio
import structlog

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import asyncssh

log = structlog.get_logger(__name__)


async def wait_for_socket_hangup(fd: int) -> None:
    """Return when the peer of the socket *fd* has gone.

    A second poll set asks beside the event loop's own registration of the
    descriptor, and the set's own descriptor turns readable at the hangup.
    No byte of the stream is read, so the session's reader loses nothing.
    """
    if sys.platform == "linux":
        await _epoll_hangup(fd)
    elif sys.platform == "darwin" or sys.platform.startswith("freebsd"):
        await _kqueue_hangup(fd)
    else:
        log.warning("client_hangup_unwatched", platform=sys.platform)
        await anyio.sleep_forever()


async def _epoll_hangup(fd: int) -> None:
    ep = select.epoll()
    try:
        ep.register(fd, 0)
        while True:
            await anyio.wait_readable(ep.fileno())
            if any(ev & (select.EPOLLHUP | select.EPOLLERR) for _fd, ev in ep.poll(0)):
                return
    finally:
        ep.close()


async def _kqueue_hangup(fd: int) -> None:
    if sys.platform == "linux" or sys.platform == "win32":
        # Also what lets the type checker, configured for Linux, skip the body.
        raise RuntimeError(f"no kqueue on {sys.platform}")
    kq = select.kqueue()
    try:
        # EV_CLEAR: data arriving wakes this once, and the transport reads it.
        kq.control([select.kevent(fd, select.KQ_FILTER_READ, select.KQ_EV_ADD | select.KQ_EV_CLEAR)], 0, 0)
        while True:
            await anyio.wait_readable(kq.fileno())
            if any(ev.flags & select.KQ_EV_EOF for ev in kq.control(None, 8, 0)):
                return
    finally:
        kq.close()


def ssh_hangup(process: asyncssh.SSHServerProcess[bytes]) -> Callable[[], Awaitable[None]]:
    """A waiter that returns when the SSH client ends the channel's input.

    asyncssh calls `eof_received` on the session for a channel EOF, and
    `connection_lost` calls it too when the channel closes first, so one
    hook sees both. Install it before the session reads anything.
    """
    gone = anyio.Event()
    original = process.eof_received

    def eof_received() -> bool:
        gone.set()
        return original()

    process.eof_received = eof_received  # type: ignore[method-assign]
    if process.is_closing():
        gone.set()
    return gone.wait
