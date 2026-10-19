"""
Nix daemon protocol client.

Connection is a concrete class that takes a pre-established reader/writer pair
and speaks the daemon protocol over it. Transport setup (subprocess, SSH,
Unix socket) is handled by Store types.

Lifecycle:
    r, w = <transport-specific setup>
    conn = Connection(r, w, "my-store")
    await conn.connect()          # handshake
    result = await conn.call(...)  # or typed methods
    await conn.close()
"""

from __future__ import annotations

import contextlib
import time
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from . import wire
from .exceptions import InfrastructureError
from .protocol import get_extension_features
from .serde.context import ReadContext, WriteContext

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path
    from types import TracebackType

    from nix_daemon_protocol.wire_message import WireModel
    from nix_daemon_protocol.wire_ops import WireRequest

    from .local_store_db import SyncReader
    from .serde import SetOptionsRequest
    from .wire import (
        NixReader,
        NixWriter,
    )

log = structlog.get_logger(__name__)


# ── Shared types ────────────────────────────────────────────────────


class ClientConn:
    """Client connection with serialized stderr writing.

    Multiple tasks can call send() concurrently; the internal lock
    ensures only one write+drain cycle runs at a time.
    No background drain task needed.

    Call flush() before writing the response to ensure all stderr is sent.
    """

    def __init__(self, w: NixWriter) -> None:
        """Wrap a writer for thread-safe stderr output to a client."""
        self.w = w
        self._write_lock = anyio.Lock()
        self._dirty = False
        """Whether a stderr message or raw bytes went out since the last flush."""
        self.standard_features: frozenset[str] = frozenset()
        """The features that this client and pynixd negotiated.

        `DaemonProxy.handshake` fills it in, and it is empty until then. A
        message that goes out before the handshake ends therefore takes the
        shape that every version reads, which is the right answer: the two
        sides have agreed on nothing yet.

        **This is the set of the client, and not of a backend.** The two
        differ, and a message written with the wrong one carries a shape the
        reader does not expect. Issue #14.
        """
        self.options: SetOptionsRequest | None = None
        """The options that this client set for its session.

        `nix-daemon` holds one connection for one client, so an option of that
        client reaches every operation of that client. pynixd holds a pool,
        and it gave the option to whichever connection was free. The client
        then set `--post-build-hook` and the hook ran for three of the five
        derivations that the request built. Issue nixidae/nixidae#231.

        `Connection.call` reads this and applies the set to the connection it
        holds, when that connection carries another set.
        """
        self.sync_reader: SyncReader | None = None
        """The read-only connection this session reads the store DB over.

        `DaemonProxy` fills it in from `LocalStoreDB.sync_reader`, and closes
        it when the session ends. It is `None` when the store has no readable
        database, and every read then uses the pooled `aiosqlite` connection.
        It lives here because it belongs to one client, and the operations
        that read the store DB already receive this object as `client`.
        """

    async def send(self, msg: WireModel) -> None:
        """Send a stderr message to the client. Safe to call from multiple tasks."""
        buf = wire.BytesWriter("client")
        await msg.to_writer(WriteContext(writer=buf, version=wire.PROTOCOL_VERSION, features=self.standard_features))
        data = buf.get_bytes()
        if data:
            self._dirty = True
            async with self._write_lock:
                self.w.write(data)
                await self.w.drain()

    async def send_raw(self, data: bytes) -> None:
        """Send raw bytes to the client. Safe to call from multiple tasks."""
        if data:
            self._dirty = True
            async with self._write_lock:
                self.w.write(data)
                await self.w.drain()

    async def flush(self) -> None:
        """Wait until all pending writes are complete and OS buffer is drained.

        **A `flush` with nothing to flush skips the lock.** `DaemonProxy.op_loop`
        calls this before every response, and `send` and `send_raw` already
        drained under the lock by the time they return, so for the common
        operation -- one that writes no stderr at all -- this took an `anyio`
        lock and drained for nothing. Measured with pyinstrument on one raw
        pump session, `flush` was 0.65 s of 2.9 s of `dispatch`. A message that
        did go out sets `_dirty`, and the next flush then takes the lock as
        before; a send that races this check drains itself, because `send`
        holds the lock for its own drain.
        """
        if not self._dirty:
            return
        async with self._write_lock:
            self._dirty = False
            await self.w.drain()


# ── Connection ──────────────────────────────────────────────────────


class Connection:
    """Nix daemon protocol client over a pre-established transport.

    Takes a reader/writer pair and handles handshake, operation
    dispatch, stderr forwarding, and response decoding.
    """

    def __init__(
        self,
        r: NixReader,
        w: NixWriter,
        conn_id: str,
        store_path: Path | None = None,
    ) -> None:
        """Wrap a reader/writer pair as a daemon protocol connection.

        Args:
            r: Reader for incoming daemon traffic.
            w: Writer for outgoing daemon traffic.
            conn_id: Unique identifier for this connection (e.g. store name).
            store_path: Optional store root path for local Nix stores.
        """
        self.id: str = conn_id
        self.store_path: Path | None = store_path
        self.version: int = wire.PROTOCOL_VERSION
        self.nix_version: str = ""
        self.features: set[str] = set()
        self.standard_features: frozenset[str] = frozenset()
        self.r = r
        self.w = w
        self.connected: bool = False
        self.dirty: bool = False
        self.op_log: list[str] = []
        self.opened_at: float = time.monotonic()
        """When this connection was made, for the lifetime rule of the pool.

        A worker of the daemon holds a temporary root for each path that it
        builds or substitutes, and it releases those roots when it exits. A
        pooled connection keeps the worker alive, so the roots of one client
        can reach the next one. `ConnectionPool.max_lifetime` retires a
        connection that has lived long enough, which retires those roots with
        it. Issue #20.
        """
        self.applied_options: SetOptionsRequest | None = None
        """The option set that this connection carries now.

        A daemon keeps the options of a `SetOptions` request until the next
        one, so a pooled connection carries the options of whichever client
        used it last. `apply_options` reads this field, and it sends nothing
        when the set does not change. Issue nixidae/nixidae#231.
        """
        self.current_op: tuple[str, float] | None = None
        """The operation this connection waits on, and when it was sent.
        `ConnectionPool.in_flight` and `pynixd state` read it. Issue #93."""
        self.on_close: Callable[[Connection], None] | None = None
        """Report-once hook the pool sets to collect this connection's totals.

        Fires after the transport closes, so the final flush is counted.
        `None` outside a pool.
        """

    async def __aenter__(self) -> Connection:
        """Enter async context; no setup required."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        """Exit connection context. Marks as dirty if an exception occurred
        or if buffers are not empty.
        """
        if exc_type is not None:
            self.dirty = True

        if not self.dirty and (await self.r.is_dirty() or await self.w.is_dirty()):
            self.dirty = True

    async def connect(self) -> None:
        """Perform daemon protocol handshake."""
        await self.handshake(self.r, self.w)
        self.connected = True

    async def close(self) -> None:
        """Close the connection and release the underlying transport."""
        self.connected = False
        try:
            with contextlib.suppress(Exception):
                await self.w.close()
        finally:
            # The hook disarms itself: removal paths overlap (a retired
            # connection can meet the sweep too), and each must report once.
            # A cancelled close still reports.
            if self.on_close is not None:
                report, self.on_close = self.on_close, None
                report(self)

    async def apply_options(self, options: SetOptionsRequest | None) -> None:
        """Give this connection the option set of a client, when it needs it.

        A client of `nix-daemon` sends `SetOptions` once, and the daemon keeps
        the set for that connection. pynixd holds a pool, so the connection
        that carried the request of a client is not the connection that runs
        the next operation of that client. This sends the set again, on the
        connection that is about to do the work.

        It sends nothing when the connection carries the set already, so the
        common case costs one comparison. Issue nixidae/nixidae#231.
        """
        if options is None or options == self.applied_options:
            return
        self.op_log.append(type(options).__name__)
        await options.to_writer(WriteContext.from_conn(self))
        await self.w.drain()
        # The daemon answers `SetOptions` with the log stream alone, so this
        # reads that stream and discards it. A failure here is a failure of
        # the connection, and the caller sees it on its own request.
        await type(options).response_type.from_reader(ReadContext.from_conn(self, buffer_logs=False))
        self.applied_options = options

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        options: SetOptionsRequest | None = None,
    ) -> Any:
        """Send an operation on the established connection.

        Args:
            request: Request object with ClassVars for op and response_type
            client: If provided, stream stderr to this client via send()
            suppress_last: If True, consume STDERR_LAST
                but don't write it to client
            raise_on_error: If True, raise BackendError on stderr errors
            options: The option set to apply first. Defaults to the set of
                *client*. A build reads it from the queue instead, because the
                build runs after the request of the client returned.
        """
        if not self.connected:
            raise RuntimeError(f"Connection {self.id!r} not connected")

        await self.apply_options(options if options is not None else (client.options if client else None))

        self.op_log.append(type(request).__name__)
        self.current_op = (type(request).__name__, time.monotonic())
        try:
            await request.to_writer(WriteContext.from_conn(self))
            await self.w.drain()
            resp_cls = type(request).response_type
            return await resp_cls.from_reader(
                ReadContext.from_conn(
                    self,
                    client=client,
                    buffer_logs=not suppress_last,
                    raise_on_error=raise_on_error,
                ),
            )
        finally:
            self.current_op = None

    async def call_with_payload(
        self,
        request: WireRequest,
        payload: bytes,
        client: ClientConn | None = None,
        suppress_last: bool = False,
    ) -> Any:
        """Send an operation whose header is followed by framed bytes.

        `AddToStore` is the operation that needs this: it writes the header,
        and then the file itself in the framed form that `FramedSource` of Nix
        reads. `AddToStoreHandler` forwards those frames from a client, and
        this sends frames that pynixd made.
        """
        if not self.connected:
            raise RuntimeError(f"Connection {self.id!r} not connected")

        await self.apply_options(client.options if client else None)

        self.op_log.append(type(request).__name__)
        self.current_op = (type(request).__name__, time.monotonic())
        try:
            await request.to_writer(WriteContext.from_conn(self))
            framed = self.w.framed()
            framed.write(payload)
            await framed.finalize()
            resp_cls = type(request).response_type
            return await resp_cls.from_reader(
                ReadContext.from_conn(self, client=client, buffer_logs=not suppress_last),
            )
        finally:
            self.current_op = None

    # ── Handshake ───────────────────────────────────────────────────

    async def handshake(
        self,
        r: NixReader,
        w: NixWriter,
    ) -> None:
        """Perform daemon protocol handshake (client side)."""
        self.w.write_uint64(wire.WORKER_MAGIC_1)
        await w.drain()

        magic = await self.r.read_uint64()
        if magic != wire.WORKER_MAGIC_2:
            raise ValueError(f"Bad magic: {magic:#x}")

        server_version = await self.r.read_uint64()
        self.version = min(wire.PROTOCOL_VERSION, server_version)
        if self.version < wire.MINIMUM_REMOTE_PROTOCOL:
            msg = (
                f"Store {self.id} negotiated protocol {wire.proto_str(self.version)}, "
                f"but we require >= {wire.proto_str(wire.MINIMUM_REMOTE_PROTOCOL)}"
            )
            log.error("protocol_version_too_low", store_id=self.id, error=msg)
            raise InfrastructureError(msg)
        log.debug(
            "daemon_protocol_negotiated",
            server_version=wire.proto_str(server_version),
            negotiated=wire.proto_str(self.version),
        )

        self.w.write_uint64(wire.PROTOCOL_VERSION)

        # Feature negotiation (1.38+) — before CPU/reserveSpace
        if self.version >= wire.FEATURE_EXCHANGE_PROTOCOL:
            self.w.write_string_set(get_extension_features() | set(wire.SUPPORTED_STANDARD_FEATURES))
            await w.drain()
            self.features = await self.r.read_string_set()
            # `self.features` holds what the daemon named, because the
            # extension names are a capability list that `store/daemon.py`
            # reads, and the `feature_matrix:` entries under it are data.
            # `standard_features` is the intersection that Nix computes, and
            # only that answers what a codec may send.
            self.standard_features = wire.negotiate_features(self.features, wire.SUPPORTED_STANDARD_FEATURES)
            log.debug(
                "daemon_features",
                server_features=self.features,
                standard_features=sorted(self.standard_features),
            )
        self.w.write_uint64(0)  # sendCpu
        self.w.write_uint64(0)  # reserveSpace
        await w.drain()

        # Server conditions these on clientVersion, not negotiated version
        if server_version >= wire.proto(1, 33):
            self.nix_version = await self.r.read_string()
            log.debug("daemon_nix_version", nix_version=self.nix_version)
            if server_version >= wire.proto(1, 35):
                await self.r.read_uint64()

        # Drain initial STDERR_LAST
        await self.r.drain_stderr()

    def __repr__(self) -> str:
        """String representation showing the connection identifier."""
        return f"Connection(id={self.id!r})"
