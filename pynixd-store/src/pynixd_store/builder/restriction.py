"""`RestrictionContext` (`restricted-store.hh`): what a recursive-nix builder may reach, and how a path joins it."""

from __future__ import annotations

import json
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Protocol

import anyio
import anyio.to_thread
from anyio.streams.buffered import BufferedByteReceiveStream

from ..hintfmt import magenta

if TYPE_CHECKING:
    import socket
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager
    from pathlib import Path

    from anyio.abc import SocketStream

SOCKET_NAME = ".nix-socket"
"""`startDaemon` (`derivation-builder.cc:1170`) puts the socket in the build directory under this name."""


class AddDependencyError(Exception):
    """The helper could not bind a path into the sandbox; the text is Nix's."""


class SubmitError(Exception):
    """`submitOutput` refused the output; the text is Nix's."""


class RestrictionContext:
    """The input closure, and the paths and outputs the builder added or built since (`isAllowed`, `:239`).

    *submitting* is a `builder-rpc-v0` build: the builder submits its outputs (`RecursiveSubmitted`). *control*
    is the helper's control socket; without one, `attach` gives it once the helper connects. *reveal* runs for
    each path the builder adds, before the helper binds it. Without *binds* (no sandbox) a path only joins.
    With *repairing*, the build repairs its outputs, and each path the builder adds replaces what the store holds.
    """

    def __init__(
        self,
        inputs: set[str],
        control: SocketStream | None,
        real_store_dir: Path,
        *,
        submitting: bool,
        repairing: bool = False,
        reveal: Callable[[str], None] | None = None,
        binds: bool = True,
    ) -> None:
        self.inputs = frozenset(inputs)
        self.submitting = submitting
        self.repairing = repairing
        self.added: set[str] = set()
        self.added_drv_outputs: set[str] = set()
        """Each `DrvOutput` a build of the builder realised, as its wire text (`addedDrvOutputs`)."""
        self.submitted: dict[str, str] = {}
        """The store path the builder submitted for each output (`submittedOutputs`)."""
        self._control: SocketStream | None = None
        self._replies: BufferedByteReceiveStream | None = None
        self._attached = anyio.Event()
        self._real_store_dir = real_store_dir
        self._reveal = reveal
        self._binds = binds
        self._lock = anyio.Lock()
        if control is not None:
            self.attach(control)

    def attach(self, control: SocketStream) -> None:
        self._control = control
        self._replies = BufferedByteReceiveStream(control)
        self._attached.set()

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
            if not self._binds:
                return
            source = str(self._real_store_dir / PurePosixPath(path).name)
            if self._reveal is not None:
                await anyio.to_thread.run_sync(self._reveal, path)
            await self._attached.wait()
            if self._control is None or self._replies is None:
                raise RuntimeError("the control socket is attached and missing")
            await self._control.send(json.dumps({"source": source, "target": path}).encode() + b"\n")
            reply = json.loads(await self._replies.receive_until(b"\n", 1 << 20))
        if reply["error"] is not None:
            raise AddDependencyError(reply["error"])

    def submit_output(self, path: str, output: str, *, opaque: bool) -> None:
        """`submitOutput` (`unix-derivation-builder.cc:791`). *path* is `SingleDerivedPath::to_string`.

        It checks neither that the path is valid nor that the build may see it: `checkSubmittedOutputs` reads
        it after the builder exits.
        """
        if not opaque:
            raise SubmitError(
                f"Attempted to submit Built path '{magenta(path)}' for output '{magenta(output)}'.\n"
                " Only Opaque paths are supported, see https://github.com/NixOS/nix/issues/12727"
            )
        if output in self.submitted:
            raise SubmitError(
                f"Attempted to submit duplicate output '{magenta(output)}' "
                f"(old '{magenta(self.submitted[output])}', new '{magenta(path)}')"
            )
        self.submitted[output] = path


class RecursiveServer(Protocol):
    """Serves the daemon protocol on *listener* to the builder, restricted to *restriction*, until the exit.

    `daemon::processConnection` with `NotTrusted` and `Recursive` (`derivation-builder.cc:1200`). On exit every
    session ends: the builder has exited, so each of its clients has gone too.
    """

    def __call__(
        self, listener: socket.socket, restriction: RestrictionContext
    ) -> AbstractAsyncContextManager[None]: ...
