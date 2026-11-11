"""pynixd-specific construction helpers for protocol codec contexts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from nix_daemon_protocol.context import ReadContext as ProtocolReadContext, WriteContext as ProtocolWriteContext

from ..exceptions import BackendError
from .auth import Role

if TYPE_CHECKING:
    from ..connection import ClientConn, Connection
    from ..proxy import DaemonProxy


@dataclass(frozen=True)
class RequestContext:
    """Daemon execution state passed to pynixd operation handlers."""

    proxy: DaemonProxy
    role: Role
    version: int
    username: str


class ReadContext(ProtocolReadContext):
    """Protocol read context with pynixd connection convenience constructors.

    **Each constructor takes the negotiated feature set of its own peer.**
    A proxy has two of them, and they are not the same set: the client
    handshake gives `proxy.standard_features` and each backend handshake
    gives `conn.standard_features`. A field with `needs_features` reads the
    set of the side it is going to or coming from, so a context built from
    the wrong one puts the shape of one peer on the wire of the other.
    Issue #14.
    """

    @classmethod
    def from_request(cls, ctx: RequestContext) -> ReadContext:
        return cls(
            reader=ctx.proxy.r,
            version=ctx.version,
            error_factory=BackendError,
            features=ctx.proxy.standard_features,
        )

    @classmethod
    def from_conn(
        cls,
        conn: Connection,
        client: ClientConn | None = None,
        buffer_logs: bool | None = None,
        raise_on_error: bool = True,
    ) -> ReadContext:
        if buffer_logs is None:
            buffer_logs = client is None
        return cls(
            reader=conn.r,
            version=conn.version,
            log_sink=client,
            buffer_logs=buffer_logs,
            raise_on_error=raise_on_error,
            error_factory=BackendError,
            features=conn.standard_features,
        )


class WriteContext(ProtocolWriteContext):
    """Protocol write context with pynixd connection convenience constructors.

    See `ReadContext` for why each constructor takes its own peer's set.
    """

    @classmethod
    def from_request(cls, ctx: RequestContext) -> WriteContext:
        return cls(writer=ctx.proxy.w, version=ctx.version, features=ctx.proxy.standard_features)

    @classmethod
    def from_conn(cls, conn: Connection, traceparent: str = "") -> WriteContext:
        return cls(writer=conn.w, version=conn.version, features=conn.standard_features, traceparent=traceparent)

    @classmethod
    def from_proxy(cls, proxy: DaemonProxy) -> WriteContext:
        return cls(writer=proxy.w, version=proxy.version, features=proxy.standard_features)
