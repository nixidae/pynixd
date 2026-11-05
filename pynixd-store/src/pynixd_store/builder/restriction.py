"""`RestrictionContext` (`restricted-store.hh`): what a recursive-nix builder may reach, and how a path joins it."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Protocol

import anyio
from anyio.streams.buffered import BufferedByteReceiveStream

if TYPE_CHECKING:
    import socket
    from contextlib import AbstractAsyncContextManager
    from pathlib import Path

    from anyio.abc import SocketStream

SOCKET_NAME = ".nix-socket"
"""`startDaemon` (`derivation-builder.cc:1170`) puts the socket in the build directory under this name."""


class AddDependencyError(Exception):
    """The helper could not bind a path into the sandbox; the text is Nix's."""


class RestrictionContext:
    """The input closure, and the paths and outputs the builder added or built since (`isAllowed`, `:239`)."""

    def __init__(self, inputs: set[str], control: SocketStream, real_store_dir: Path) -> None:
        self.inputs = frozenset(inputs)
        self.added: set[str] = set()
        self.added_drv_outputs: set[str] = set()
        """Each `DrvOutput` a build of the builder realised, as its wire text (`addedDrvOutputs`)."""
        self._control = control
        self._replies = BufferedByteReceiveStream(control)
        self._real_store_dir = real_store_dir
        self._lock = anyio.Lock()

    def is_allowed(self, path: str) -> bool:
        return path in self.inputs or path in self.added

    @property
    def allowed(self) -> set[str]:
        return set(self.inputs | self.added)

    async def add_dependency(self, path: str) -> None:
        """`addDependency`: a path not allowed yet becomes allowed, and the helper binds it into the sandbox.

        The path joins first, as `addDependencyPrep` inserts it before the bind (`chroot-derivation-builder.cc:180`).
        """
        async with self._lock:
            if self.is_allowed(path):
                return
            self.added.add(path)
            source = str(self._real_store_dir / path.rsplit("/", 1)[-1])
            await self._control.send(json.dumps({"source": source, "target": path}).encode() + b"\n")
            reply = json.loads(await self._replies.receive_until(b"\n", 1 << 20))
        if reply["error"] is not None:
            raise AddDependencyError(reply["error"])


class RecursiveServer(Protocol):
    """Serves the daemon protocol on *listener* to the builder, restricted to *restriction*, until the exit.

    `daemon::processConnection` with `NotTrusted` and `Recursive` (`derivation-builder.cc:1200`). On exit every
    session ends: the builder has exited, so each of its clients has gone too.
    """

    def __call__(
        self, listener: socket.socket, restriction: RestrictionContext
    ) -> AbstractAsyncContextManager[None]: ...
