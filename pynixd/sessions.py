"""
Established client sessions, and closing them on shutdown.

`Server.close` closes the listeners, which stops new clients, but the
handlers of established clients are tasks nobody holds: an in-flight
operation survives the shutdown sequence and its client waits on a dead
socket until systemd SIGKILLs the process. Issue #64.

`DaemonProxy.run` registers each session here on entry and unregisters
on exit, so this always names the sessions that exist right now.
`Server.close` calls `shutdown` after closing the listeners: every
in-flight operation is answered with an error first, so its client fails
fast and retries against the new daemon, and only then are the handlers
cancelled and waited on, with a deadline.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import anyio
import structlog

if TYPE_CHECKING:
    from .proxy import DaemonProxy

log = structlog.get_logger(__name__)

SHUTDOWN_ERROR = "pynixd: server is shutting down"
"""The text an in-flight operation carries when shutdown answers it."""

ERROR_TIMEOUT = 2.0
"""Give one error write this long. The client is blocked reading, so a slow
drain must not hold the shutdown sequence."""

SHUTDOWN_DEADLINE = 5.0
"""Wait this long for cancelled handlers to finish, then return anyway. A
handler that ignores cancellation is abandoned; the process exits next."""


@dataclass(eq=False)
class Session:
    """One registered session: the scope its operations run in, and its end."""

    proxy: DaemonProxy
    scope: anyio.CancelScope = field(default_factory=anyio.CancelScope)
    """Shutdown cancels it. The session's cleanup runs outside it."""
    ended: anyio.Event = field(default_factory=anyio.Event)


class ClientSessions:
    """Every established client session, for the shutdown path."""

    def __init__(self) -> None:
        self._sessions: set[Session] = set()

    def __len__(self) -> int:
        return len(self._sessions)

    def proxies(self) -> list[DaemonProxy]:
        """Every live session. The state collector reads this."""
        return [session.proxy for session in list(self._sessions)]

    def by_transport(self) -> dict[str, int]:
        """Live sessions by transport. The state collector reads this."""
        counts: dict[str, int] = {}
        for session in list(self._sessions):
            transport = session.proxy.transport
            counts[transport] = counts.get(transport, 0) + 1
        return counts

    def track(self, proxy: DaemonProxy) -> Session:
        """Register *proxy*. It runs its operations in the answered scope."""
        session = Session(proxy)
        self._sessions.add(session)
        return session

    def untrack(self, session: Session) -> None:
        """Forget *session*, and tell `shutdown` that it has ended."""
        self._sessions.discard(session)
        session.ended.set()

    async def shutdown(self, reason: str = SHUTDOWN_ERROR) -> None:
        """Answer every in-flight operation with *reason*, then close them.

        The error goes out before the cancel: the client is blocked reading
        its response, and a `STDERR_ERROR` fails it fast. Cancelling first
        would run the handler's `finally`, which closes the transport before
        the error goes out. The write races a response the handler may be
        encoding at this same moment; both outcomes fail the client fast,
        which is what matters -- no outcome leaves it waiting on a dead
        socket.
        """
        sessions = list(self._sessions)
        if not sessions:
            return
        log.info("server_closing_client_sessions", count=len(sessions))
        for session in sessions:
            with contextlib.suppress(Exception), anyio.move_on_after(ERROR_TIMEOUT):
                await session.proxy.send_error(reason)
            session.scope.cancel()
        with anyio.move_on_after(SHUTDOWN_DEADLINE):
            for session in sessions:
                await session.ended.wait()
