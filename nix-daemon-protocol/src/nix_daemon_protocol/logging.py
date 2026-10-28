"""Optional structured logging for daemon protocol consumers.

The protocol package never configures logging. It uses structlog when the host
has it installed and otherwise falls back to the standard-library logger.
"""

from __future__ import annotations

import logging as stdlib_logging
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Any, Literal, Protocol

_STRUCTLOG_GET_LOGGER: Callable[[str], ProtocolLogger] | None
try:
    from structlog import get_logger as _structlog_get_logger
except ImportError:  # structlog is intentionally an optional dependency.
    _STRUCTLOG_GET_LOGGER = None
else:
    _STRUCTLOG_GET_LOGGER = _structlog_get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from .context import ReadContext


class ProtocolLogger(Protocol):
    """Minimal structured logging surface used by protocol decoding."""

    def exception(self, event: str, /, **fields: object) -> None: ...


class _StdlibLogger:
    """Adapt standard logging to the protocol's structured event shape."""

    def __init__(self, name: str) -> None:
        self._logger = stdlib_logging.getLogger(name)

    def exception(self, event: str, /, **fields: object) -> None:

        # forwards to `logging.Logger.exception`, and each of its own callers
        # is inside an `except` block. A method cannot be inside one.
        self._logger.exception(event, extra={"daemon_protocol": fields})  # noqa: LOG004


def get_logger(name: str) -> ProtocolLogger:
    """Return the host's structured logger without configuring it."""
    if _STRUCTLOG_GET_LOGGER is not None:
        return _STRUCTLOG_GET_LOGGER(name)
    return _StdlibLogger(name)


_DEFAULT_LOGGER = get_logger("nix_daemon_protocol")
_DECODE_DEPTH: ContextVar[int] = ContextVar("nix_daemon_protocol_decode_depth", default=0)


def log_deserialization_failure(ctx: ReadContext, model_type: type, exc: BaseException) -> None:
    """Report a failed outermost decode without including wire payload data."""
    reader = ctx.reader
    fields: dict[str, Any] = {
        "message_type": model_type.__name__,
        "protocol_version": ctx.version,
        "exception_type": type(exc).__name__,
    }
    operation = getattr(model_type, "op", None)
    if operation is not None:
        fields["operation"] = operation
    reader_id = getattr(reader, "identifier", None)
    if reader_id is not None:
        fields["reader_id"] = reader_id
    offset = getattr(reader, "tell", None)
    if callable(offset):
        fields["offset"] = offset()
    (ctx.logger or _DEFAULT_LOGGER).exception("daemon_deserialization_failed", **fields)


class _NoopScope:
    """What a nested decode enters. It does nothing.

    Only the outermost decode logs, so a nested one needs no depth and no
    `try`. One shared instance, because a fresh object per nested decode is
    the cost this exists to remove.
    """

    __slots__ = ()

    def __enter__(self) -> _NoopScope:
        return self

    def __exit__(self, *_exc: object) -> Literal[False]:
        return False


_NOOP_SCOPE = _NoopScope()


class _DeserializationScope:
    """What the outermost decode enters: mark the depth, and log one failure."""

    __slots__ = ("_ctx", "_model_type", "_token")

    def __init__(self, ctx: ReadContext, model_type: type) -> None:
        self._ctx = ctx
        self._model_type = model_type
        self._token: Token[int] | None = None

    def __enter__(self) -> _DeserializationScope:
        self._token = _DECODE_DEPTH.set(1)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        _tb: object,
    ) -> Literal[False]:
        if self._token is not None:
            _DECODE_DEPTH.reset(self._token)
        # A cancel or an interrupt stops a read that may be fine, so only an
        # `Exception` is a failure of the bytes.
        if isinstance(exc, Exception):
            log_deserialization_failure(self._ctx, self._model_type, exc)
        return False


def deserialization_scope(ctx: ReadContext, model_type: type) -> _NoopScope | _DeserializationScope:
    """Log only the outermost failure in a recursive decode operation.

    **A pair of scopes, not a `@contextmanager`.** Every model and every
    string type enters this, so one system build entered it 5.3 M times and
    contextlib's generator scaffolding was 9.4 s of the build's 137 s. A
    nested decode needs no token and no `try`, so it gets one shared no-op
    and the outermost gets the one scope that sets the depth. The outermost
    scope logs, which is the whole of the behaviour.
    """
    if _DECODE_DEPTH.get():
        return _NOOP_SCOPE
    return _DeserializationScope(ctx, model_type)
