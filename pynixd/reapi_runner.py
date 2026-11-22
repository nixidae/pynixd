"""A Buildbarn runner that makes a Nix closure valid before an action runs.

bb_worker calls a runner over gRPC (`buildbarn.runner.Runner`, bb-remote-execution
`pkg/proto/runner/runner.proto`) and bb_runner offers no hook before a command.
This service sits between the two. When an action's program (argv[0]) is in
the store, it makes that store path's closure valid through the local daemon,
roots it, and forwards the request to bb_runner unchanged.

argv is part of the REAPI `Command`, so the toolchain is part of the action
digest: a new toolchain misses the action cache. A client whose compiler path
is relative names a store path in front of it: Siso's `remote_wrapper`.
An environment variable is no channel: Siso sends its `env` config with local
steps only (`build/run_cmd.go`, `runLocal`).

The proxy forwards bytes. It decodes only the arguments of a `RunRequest`, so
it needs no generated protobuf code.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

import anyio
import structlog
from grpclib.client import Channel, UnaryUnaryMethod
from grpclib.const import Cardinality, Handler, Status
from grpclib.encoding.base import CodecBase
from grpclib.exceptions import GRPCError
from grpclib.server import Server

from nix_daemon_protocol.add_temp_root import AddTempRootRequest
from nix_daemon_protocol.ensure_path import EnsurePathRequest
from nix_daemon_protocol.store_dir import store_prefix
from nix_daemon_protocol.store_path import StorePath

from .connection import Connection
from .exceptions import BackendError
from .unix_socket import open_unix_connection
from .wire import UnixNixReader, UnixNixWriter

if TYPE_CHECKING:
    from pathlib import Path

    from grpclib.server import Stream

log = structlog.get_logger(__name__)

SERVICE: Final = "/buildbarn.runner.Runner"

_ARGUMENTS_FIELD: Final = 1
_WIRE_FIXED64: Final = 1
_WIRE_LENGTH: Final = 2


class RawCodec(CodecBase):
    """Messages stay bytes in both directions: the proxy re-encodes nothing."""

    __content_subtype__ = "proto"

    def encode(self, message: Any, message_type: Any) -> bytes:  # noqa: ANN401 -- grpclib's CodecBase signature
        if not isinstance(message, bytes):
            raise TypeError(f"RawCodec encodes bytes, not {type(message).__name__}")
        return message

    def decode(self, data: bytes, message_type: Any) -> bytes:  # noqa: ANN401 -- grpclib's CodecBase signature
        return data


def _varint(data: bytes, pos: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ValueError("truncated varint")
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint longer than 64 bits")


def _fields(data: bytes) -> Iterator[tuple[int, int, int | bytes]]:
    """Each field of one protobuf message: number, wire type, value."""
    pos = 0
    while pos < len(data):
        key, pos = _varint(data, pos)
        number, wire = key >> 3, key & 7
        match wire:
            case 0:
                value, pos = _varint(data, pos)
                yield number, wire, value
            case 1 | 5:
                width = 8 if wire == _WIRE_FIXED64 else 4
                if pos + width > len(data):
                    raise ValueError("truncated fixed field")
                yield number, wire, data[pos : pos + width]
                pos += width
            case 2:
                length, pos = _varint(data, pos)
                if pos + length > len(data):
                    raise ValueError("truncated length-delimited field")
                yield number, wire, data[pos : pos + length]
                pos += length
            case _:
                raise ValueError(f"unsupported wire type {wire}")


def program(request: bytes) -> str | None:
    """argv[0] of a serialized `RunRequest` (`arguments`, field 1), or None for no arguments."""
    for number, wire, value in _fields(request):
        if number == _ARGUMENTS_FIELD and wire == _WIRE_LENGTH and isinstance(value, bytes):
            return value.decode()
    return None


def closure_root(argv0: str | None) -> StorePath | None:
    """The store path that holds `argv0`, or None when it is not in the store."""
    prefix = store_prefix()
    if argv0 is None or not argv0.startswith(prefix):
        return None
    base = argv0.removeprefix(prefix).split("/", 1)[0]
    return StorePath(base) if base else None


@dataclass
class Closures:
    """Store paths made valid and rooted on one daemon connection, for this process's lifetime.

    The temporary roots belong to the connection: the daemon drops them when
    it closes, so the connection stays open while the runner serves.
    """

    socket: Path
    _conn: Connection | None = None
    _valid: set[StorePath] = field(default_factory=set)
    _lock: anyio.Lock = field(default_factory=anyio.Lock)

    async def ensure(self, path: StorePath) -> None:
        if path in self._valid:
            return
        # One realisation at a time: eight actions that start together wait on
        # the first download instead of asking the daemon eight times.
        async with self._lock:
            if path in self._valid:
                return
            started = anyio.current_time()
            log.info("closure_ensuring", path=str(path))
            try:
                conn = await self._connection()
                await conn.call(EnsurePathRequest(path=path), raise_on_error=True)
                await conn.call(AddTempRootRequest(path=path), raise_on_error=True)
            except (BackendError, OSError, EOFError, anyio.BrokenResourceError, anyio.EndOfStream):
                await self.close()
                raise
            self._valid.add(path)
            log.info("closure_ensured", path=str(path), seconds=round(anyio.current_time() - started, 3))

    async def _connection(self) -> Connection:
        if self._conn is None or not self._conn.connected:
            link = await open_unix_connection(self.socket)
            conn = Connection(
                UnixNixReader(link, identifier="reapi-runner"),
                UnixNixWriter(link, identifier="reapi-runner"),
                "reapi-runner",
            )
            await conn.connect()
            self._conn = conn
        return self._conn

    async def close(self) -> None:
        # A new connection holds no roots: every path is realised and rooted again.
        self._valid.clear()
        if self._conn is not None:
            conn, self._conn = self._conn, None
            await conn.close()


@dataclass
class RunnerProxy:
    """`buildbarn.runner.Runner`, served to bb_worker and forwarded to bb_runner."""

    closures: Closures
    upstream: Channel

    def __mapping__(self) -> dict[str, Handler]:
        return {
            f"{SERVICE}/CheckReadiness": Handler(self._check_readiness, Cardinality.UNARY_UNARY, bytes, bytes),
            f"{SERVICE}/Run": Handler(self._run, Cardinality.UNARY_UNARY, bytes, bytes),
        }

    async def _forward(self, method: str, request: bytes) -> bytes:
        call = UnaryUnaryMethod[bytes, bytes](self.upstream, f"{SERVICE}/{method}", bytes, bytes)
        return await call(request)

    async def _check_readiness(self, stream: Stream[bytes, bytes]) -> None:
        request = await stream.recv_message()
        if request is None:
            raise GRPCError(Status.INVALID_ARGUMENT, "no request")
        await stream.send_message(await self._forward("CheckReadiness", request))

    async def _run(self, stream: Stream[bytes, bytes]) -> None:
        request = await stream.recv_message()
        if request is None:
            raise GRPCError(Status.INVALID_ARGUMENT, "no request")
        try:
            root = closure_root(program(request))
        except ValueError as error:
            raise GRPCError(Status.INVALID_ARGUMENT, f"RunRequest: {error}") from error
        # An error status, not a non-zero exit code: a client reads an exit
        # code as the command's own failure. REAPI's FAILED_PRECONDITION is a
        # missing input, and Siso fails the step on it; Siso retries
        # UNAVAILABLE, so that is for a daemon that may come back.
        if root is not None:
            try:
                await self.closures.ensure(root)
            except BackendError as error:
                log.warning("closure_refused", path=str(root), error=str(error))
                raise GRPCError(Status.FAILED_PRECONDITION, f"pynixd could not make {root} valid: {error}") from error
            except (OSError, EOFError, anyio.BrokenResourceError, anyio.EndOfStream) as error:
                log.warning("closure_daemon_unreachable", path=str(root), exc_info=True)
                raise GRPCError(
                    Status.UNAVAILABLE, f"pynixd could not reach its Nix daemon for {root}: {error}"
                ) from error
        await stream.send_message(await self._forward("Run", request))


async def serve(listen: Path, upstream: Path, daemon: Path) -> None:
    """Serve on `listen` until cancelled."""
    closures = Closures(daemon)
    channel = Channel(path=str(upstream), codec=RawCodec())
    server = Server([RunnerProxy(closures, channel)], codec=RawCodec())
    try:
        await anyio.Path(listen).unlink(missing_ok=True)
        await server.start(path=str(listen))
        log.info("reapi_runner_listening", listen=str(listen), upstream=str(upstream), daemon=str(daemon))
        await server.wait_closed()
    finally:
        with anyio.CancelScope(shield=True), anyio.move_on_after(10):
            server.close()
            await server.wait_closed()
            channel.close()
            await closures.close()
