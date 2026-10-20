"""Shared exceptions for pynixd."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.exceptions import DaemonProtocolError

if TYPE_CHECKING:
    from collections.abc import Iterator


def leaves(ex: BaseException) -> Iterator[BaseException]:
    """*ex* itself, or every exception a group of groups holds.

    A task group raises a group around the failure of its task, so a handler
    that matches a class sees the group and not the class. Issue #889.
    """
    if isinstance(ex, BaseExceptionGroup):
        for inner in ex.exceptions:
            yield from leaves(inner)
    else:
        yield ex


def error_text(ex: BaseException) -> str:
    """The text that `STDERR_ERROR` carries for *ex*.

    **A client prints this text after the word "error:", so it must read as
    one.** This was `repr(ex)`, which gave the client
    `error: BackendError("Cannot build '\\x1b[35;1m/nix/store/...")` -- the
    name of a Python class, a quoted string, and every escape of the message
    doubled. Nix writes the message alone.

    A `PynixdError` carries a message that pynixd wrote for a reader, and a
    `DaemonProtocolError` carries the message of a daemon behind pynixd. Both
    are the whole text. Any other exception is a fault of pynixd, and the name
    of the class is the part that says so.

    **A task group carries the failure of its task, and the group itself says
    nothing.** The client read `error: ExceptionGroup: unhandled errors in a
    TaskGroup (1 sub-exception)` and never learned the reason. `ca:signatures`
    reads the reason, which is "cannot add path '...' because it lacks a
    signature by a trusted key".
    """
    return "\n".join(
        str(leaf) if isinstance(leaf, PynixdError | DaemonProtocolError) else f"{type(leaf).__name__}: {leaf}"
        for leaf in leaves(ex)
    )


class PynixdError(Exception):
    """Base for all pynixd errors."""


class InfrastructureError(PynixdError):
    """Transport/connection failure (SSH down, EOF, timeout)."""


class BackendError(PynixdError):
    """Raised when the backend sends STDERR_ERROR.

    The error has already been forwarded to the client.
    """


class ClosingError(PynixdError):
    """An error that ends the session after the client reads it.

    `processConnection` in `src/libstore/daemon.cc` closes the connection
    when an operation throws before `startWork`, since the arguments may be
    unread. An operation that refuses a client there raises this.
    """


class OpNotImplementedError(PynixdError):
    """Raised when an operation is not implemented for a specific executor (e.g. DB)."""


class GCNotPermittedError(PynixdError):
    """Raised when a delete is asked before the liveness mirror is proven.

    The collector plans freely -- a dry-run deletes nothing -- but EXECUTE
    stays refused until the operator sets `gc_allow_execute`, which is the
    signature after sustained zero-divergence between the mirror and what
    Nix reports alive. Fail closed: an unproven mirror must not name
    deletions, and a refused delete must not read as success.
    """
