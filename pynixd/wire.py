"""
Nix daemon wire protocol primitives (async).

All integers are little-endian uint64. Strings are length-prefixed with
zero-padding to 8-byte alignment.

Read functions are async (for asyncio.StreamReader / asyncssh.SSHReader).
Write functions are sync (writer.write() buffers; callers await drain()).
"""

from __future__ import annotations

import asyncio
import os
import struct
import time
from typing import TYPE_CHECKING, Protocol

from anyio.lowlevel import checkpoint

from nix_daemon_protocol.logs import LogMessage, drain as drain_log_stream, read_stream as read_log_stream

from ._lazy import ssh_connection_lost
from .constants import (
    FEATURE_ADD_TEMP_ROOTS as FEATURE_ADD_TEMP_ROOTS,
    FEATURE_EXCHANGE_PROTOCOL as FEATURE_EXCHANGE_PROTOCOL,
    MINIMUM_CLIENT_PROTOCOL as MINIMUM_CLIENT_PROTOCOL,
    MINIMUM_REMOTE_PROTOCOL as MINIMUM_REMOTE_PROTOCOL,
    PROTOCOL_VERSION as PROTOCOL_VERSION,
    STANDARD_FEATURES as STANDARD_FEATURES,
    STDERR_LAST as STDERR_LAST,
    SUPPORTED_STANDARD_FEATURES as SUPPORTED_STANDARD_FEATURES,
    WORKER_MAGIC_1 as WORKER_MAGIC_1,
    WORKER_MAGIC_2 as WORKER_MAGIC_2,
    negotiate_features as negotiate_features,
    proto as proto,
    proto_str as proto_str,
)
from .serde.context import ReadContext

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable

    import asyncssh


def _env_int(name: str, default: int) -> int:
    """The integer that *name* holds, or *default* when it holds nothing.

    `environs` did this, and it cost 64 ms of every daemon start for four
    calls across three modules: it pulls `marshmallow` and `python-dotenv`,
    and nothing here ever called `env.read_env()`. Issue #30.
    """
    raw = os.environ.get(name, "")
    return int(raw) if raw else default


_CHUNK_SIZE = _env_int("PYNIXD_CHUNK_SIZE", 1024 * 1024)

_SSH_WINDOW_SIZE = _env_int("PYNIXD_SSH_WINDOW", 16 * 1024 * 1024)

_SSH_READ_AHEAD = 16 * 1024  # read-ahead size to amortize asyncssh lock overhead

_UNIX_READ_AHEAD = 16 * 1024  # read-ahead size to amortize syscall overhead

_UINT64_STRUCT = struct.Struct("<Q")


def _nar_pad(n: int) -> int:
    """Return number of padding bytes needed for 8-byte alignment."""
    return (8 - n % 8) % 8


# ── NixReader / NixWriter ──────────────────────────────────────


class NixReader:
    """Wraps AsyncReader with wire protocol methods and dirty checking."""

    def __init__(self, identifier: str = "unknown") -> None:
        self.identifier = identifier
        self.bytes_read = 0
        """Transport bytes taken off the wire through this reader.

        Counted where the transport is read, so readahead counts once
        at fill and never again when served from the buffer. One
        int-add per transport read: thousands of times rarer than the
        protocol reads it serves, and off every per-chunk path.
        """
        self.last_read_at = time.monotonic()
        """When the transport last delivered bytes. `pynixd state` reads it
        to say how long a daemon has been silent. Issue #93."""

    async def readexactly(self, n: int) -> bytes:
        raise NotImplementedError

    async def read_uint64(self) -> int:
        return _UINT64_STRUCT.unpack(await self.readexactly(8))[0]

    async def read_bool(self) -> bool:
        return bool(await self.read_uint64())

    async def read_optional_uint64(self) -> int | None:
        tag = await self.read_uint64()
        if tag == 0:
            return None
        return await self.read_uint64()

    async def read_bytes(self) -> bytes:
        length = await self.read_uint64()
        data = await self.readexactly(length)
        pad = _nar_pad(length)
        if pad:
            await self.readexactly(pad)
        return data

    async def read_string[T = str](self, tp: Callable[[str], T] = str) -> T:
        return tp((await self.read_bytes()).decode("utf-8"))

    async def read_string_list[T = str](self, tp: Callable[[str], T] = str) -> list[T]:
        count: int = await self.read_uint64()
        return [await self.read_string(tp) for _ in range(count)]

    async def read_string_set[T = str](self, tp: Callable[[str], T] = str) -> set[T]:
        count: int = await self.read_uint64()
        return {await self.read_string(tp) for _ in range(count)}

    async def drain_stderr(self, raise_on_error: bool = True) -> None:
        """Read and discard all stderr messages until STDERR_LAST."""
        await drain_log_stream(ReadContext(reader=self, version=PROTOCOL_VERSION), raise_on_error=raise_on_error)

    def read_stderr(self) -> AsyncIterator[LogMessage]:
        """Return an AsyncIterator that yields stderr messages until STDERR_LAST."""
        return read_log_stream(ReadContext(reader=self, version=PROTOCOL_VERSION))

    async def is_dirty(self) -> bool:
        return self._transport_is_dirty()

    def _transport_is_dirty(self) -> bool:
        return False

    def peer_closed(self) -> bool:
        """Has the other side closed the stream, with nothing left to read?

        The transport reads while a connection sits idle, so a close lands
        here before anyone asks for the next byte.
        """
        return False


class SSHNixReader(NixReader):
    # Read ahead, because a call costs the same whatever it asks for. The NAR
    # forward spends 2.47s of its 2.99s inside asyncssh, two calls per frame:
    # an 8-byte length prefix costs the same round trip as the 64 KiB payload
    # behind it. Serving the small reads out of one larger read removes the
    # prefix calls entirely.
    #
    # This is safe only because nothing reads `self.reader` except this class,
    # so a byte held here is never invisible to a later operation on the same
    # connection. `_transport_is_dirty` counts this buffer for the same reason.
    # Check both before giving anything else access to the stream.
    _READAHEAD = 256 * 1024

    def __init__(self, reader: asyncssh.SSHReader, identifier: str = "unknown") -> None:
        super().__init__(identifier=identifier)
        self.reader = reader
        self._buf: bytes = b""
        self._pos = 0

    async def readexactly(self, n: int) -> bytes:
        if (len(self._buf) - self._pos) >= n:
            out = self._buf[self._pos : self._pos + n]
            self._pos += n
            return out

        head = self._buf[self._pos :]
        self._buf = b""
        self._pos = 0
        need = n - len(head)

        try:
            # A read at least as large as the read-ahead goes straight through.
            # Buffering it would copy the payload to no purpose.
            if need >= self._READAHEAD:
                data = await self.reader.readexactly(need)
                # Only `need` is new: `head` came out of the buffer, where it
                # was counted when the transport filled it.
                self.bytes_read += need
                self.last_read_at = time.monotonic()
                return head + data

            parts = [head]
            while need > 0:
                chunk = await self.reader.read(self._READAHEAD)
                if not chunk:
                    raise EOFError("SSH connection closed")
                self.bytes_read += len(chunk)
                self.last_read_at = time.monotonic()
                if len(chunk) >= need:
                    parts.append(chunk[:need])
                    self._buf = chunk
                    self._pos = need
                    need = 0
                else:
                    parts.append(chunk)
                    need -= len(chunk)
            return b"".join(parts)
        except ssh_connection_lost():
            raise EOFError("SSH connection lost") from None

    def peer_closed(self) -> bool:
        return self._pos >= len(self._buf) and self.reader.at_eof()

    def _transport_is_dirty(self) -> bool:
        if (len(self._buf) - self._pos) > 0:
            return True
        if hasattr(self.reader, "get_read_buffer_size"):
            return self.reader.get_read_buffer_size() > 0  # type: ignore[reportAttributeAccessIssue]
        # Fallback to private access if API changed or is missing
        # _recv_buf is a dict of list of bytes
        buf = self.reader._session._recv_buf.get(
            self.reader._datatype,
            [],
        )
        return any(isinstance(chunk, (bytes, bytearray)) and chunk for chunk in buf)


class UnixNixReader(NixReader):
    # Read ahead, because `StreamReader.readexactly` is one await per call and
    # the protocol makes three of them for every string: the 8-byte length, the
    # payload, and its padding. A build sends thousands of store paths, so the
    # reader spends its time suspended between fields rather than parsing them.
    # One larger read serves many small ones from memory.
    #
    # **Safe only because nothing else reads `self.reader`.** A byte held here
    # is invisible to anything that reads the stream directly, so a second
    # reader would lose it. `_transport_is_dirty` counts this buffer for the
    # same reason. Check both before giving another caller the stream.
    _READAHEAD = _UNIX_READ_AHEAD

    def __init__(
        self,
        reader: asyncio.StreamReader,
        identifier: str = "unknown",
    ) -> None:
        super().__init__(identifier=identifier)
        self.reader = reader
        self._buf: bytes = b""
        self._pos = 0

    async def readexactly(self, n: int) -> bytes:
        if (len(self._buf) - self._pos) >= n:
            out = self._buf[self._pos : self._pos + n]
            self._pos += n
            return out

        head = self._buf[self._pos :]
        self._buf = b""
        self._pos = 0
        need = n - len(head)

        # A read at least as large as the read-ahead goes straight through.
        # Buffering it would copy the payload to no purpose.
        if need >= self._READAHEAD:
            data = await self.reader.readexactly(need)
            # Only `need` is new: `head` came out of the buffer, where it
            # was counted when the transport filled it.
            self.bytes_read += need
            self.last_read_at = time.monotonic()
            return head + data

        parts = [head]
        while need > 0:
            chunk = await self.reader.read(self._READAHEAD)
            if not chunk:
                raise asyncio.IncompleteReadError(b"".join(parts), n)
            self.bytes_read += len(chunk)
            self.last_read_at = time.monotonic()
            if len(chunk) >= need:
                parts.append(chunk[:need])
                self._buf = chunk
                self._pos = need
                need = 0
            else:
                parts.append(chunk)
                need -= len(chunk)
        return b"".join(parts)

    def _transport_is_dirty(self) -> bool:
        if (len(self._buf) - self._pos) > 0:
            return True
        return bool(self.reader._buffer)  # type: ignore[attr-defined]

    def peer_closed(self) -> bool:
        return self._pos >= len(self._buf) and self.reader.at_eof()


class StrCoercible(Protocol):
    """Anything that can be converted to ``str`` via ``str()``."""

    def __str__(self) -> str: ...


class NixWriter:
    """Wraps AsyncWriter with wire protocol methods."""

    # Coalesce before the transport sees anything.
    #
    # **asyncio charges per write, not per byte, and the charge is the whole
    # buffer.** `transport.write` calls `_maybe_pause_protocol`, which calls
    # `get_write_buffer_size`, which is `sum(map(len, self._buffer))` over
    # everything not yet on the socket. `write_bytes` is two or three writes
    # per value -- the length, the payload, the padding -- so an op carrying
    # thousands of store paths paid that sum tens of thousands of times over
    # a buffer that only grew. The cost is quadratic in the number of values,
    # and none of it suspends, so the event loop runs nothing throughout.
    #
    # One large write per flush makes the buffer a handful of chunks whatever
    # the op carries, so the sum is over a handful of lengths.
    #
    # **Safe because every complete message is followed by `drain`.** The op
    # loop of `DaemonProxy` drains after each response and `Connection.call`
    # drains after each request, so nothing is left here unsent. A subclass
    # that overrides `write` -- `BytesWriter`, `FramedWriter` -- never reaches
    # this buffer and needs nothing.
    _FLUSH_BYTES = 64 * 1024

    def __init__(self, identifier: str = "unknown") -> None:
        self.identifier = identifier
        self._pending = bytearray()
        self.bytes_written = 0
        """Transport bytes handed to the wire through this writer.

        Counted in `_write_to_transport`, so one coalesced flush costs
        one int-add, and framed bytes count too: a `FramedWriter` writes
        into its underlying writer's buffer.
        """

    def write(self, data: bytes) -> None:
        self._pending += data
        if len(self._pending) >= self._FLUSH_BYTES:
            self.flush()

    def flush(self) -> None:
        """Hand everything buffered to the transport, as one write."""
        if self._pending:
            self._write_to_transport(bytes(self._pending))
            self._pending.clear()

    async def drain(self) -> None:
        self.flush()
        await self._drain_transport()

    async def is_dirty(self) -> bool:
        # The pending buffer counts. A byte held here is invisible to the
        # transport, and `is_dirty` exists to answer whether anything of this
        # connection is still in flight before something else is given the
        # stream. Same argument as `SSHNixReader._transport_is_dirty`.
        return bool(self._pending) or self._transport_is_dirty()

    def _write_to_transport(self, data: bytes) -> None:
        raise NotImplementedError

    async def _drain_transport(self) -> None:
        raise NotImplementedError

    def _transport_is_dirty(self) -> bool:
        return False

    async def close(self) -> None:
        """Close the underlying transport. Override in subclasses."""

    def write_uint64(self, val: int) -> None:
        self.write(_UINT64_STRUCT.pack(val))

    def write_bool(self, v: bool) -> None:
        self.write_uint64(1 if v else 0)

    def write_uint64s(self, vals: list[int]) -> None:
        if not vals:
            return
        self.write(struct.pack(f"<{len(vals)}Q", *vals))

    def write_optional_uint64(self, val: int | None) -> None:
        if val is None:
            self.write_uint64(0)
        else:
            self.write_uint64(1)
            self.write_uint64(val)

    def write_bytes(self, data: bytes) -> None:
        self.write_uint64(len(data))
        self.write(data)
        pad: int = _nar_pad(len(data))
        if pad:
            self.write(b"\0" * pad)

    def write_string(self, s: StrCoercible) -> None:
        self.write_bytes(str(s).encode("utf-8"))

    def write_string_list(self, items: Iterable[StrCoercible]) -> None:
        items_list = list(items)
        self.write_uint64(len(items_list))
        for item in items_list:
            self.write_string(item)

    def write_string_set(self, items: Iterable[StrCoercible]) -> None:
        self.write_string_list(items)

    def framed(self) -> FramedWriter:
        """Create a FramedWriter that writes framed data to this writer."""
        return FramedWriter(self)


class BytesWriter(NixWriter):
    """A NixWriter that writes to a bytearray."""

    def __init__(self, identifier: str = "memory") -> None:
        super().__init__(identifier=identifier)
        self._buf = bytearray()

    def write(self, data: bytes) -> None:
        self._buf.extend(data)

    async def drain(self) -> None:
        pass

    def get_bytes(self) -> bytes:
        return bytes(self._buf)

    def get_bytes_since(self, offset: int) -> bytes:
        """The bytes written since *offset*, copying only those.

        `get_bytes()[offset:]` copies the whole buffer on every call,
        which wedged the event loop for 31 s on a gigabyte build log:
        one synchronous copy per log message, never yielding. This
        copies the delta alone.
        """
        return bytes(self._buf[offset:])

    def tell(self) -> int:
        return len(self._buf)


class BytesReader(NixReader):
    """A NixReader that reads from an in-memory bytes buffer.

    Mirrors BytesWriter for roundtrip testing: write with BytesWriter,
    then read back with BytesReader.
    """

    def __init__(self, data: bytes, identifier: str = "memory") -> None:
        super().__init__(identifier=identifier)
        self._data = data
        self._pos = 0

    async def readexactly(self, n: int) -> bytes:
        if self._pos + n > len(self._data):
            have = len(self._data) - self._pos
            raise EOFError(
                f"BytesReader: need {n} bytes, have {have} remaining (total {len(self._data)}, at offset {self._pos})",
            )
        result = self._data[self._pos : self._pos + n]
        self._pos += n
        return result


class SSHNixWriter(NixWriter):
    def __init__(self, writer: asyncssh.SSHWriter, identifier: str = "unknown") -> None:
        super().__init__(identifier=identifier)
        self.writer = writer

    def _write_to_transport(self, data: bytes) -> None:
        self.bytes_written += len(data)
        self.writer.write(data)

    async def _drain_transport(self) -> None:
        await self.writer.drain()

    def _transport_is_dirty(self) -> bool:
        return self.writer.channel.get_write_buffer_size() > 0

    async def close(self) -> None:
        # The close is synchronous and runs even when the drain fails or is
        # cancelled. A transport left open keeps the daemon's work running.
        try:
            await self.drain()
        finally:
            self.writer.close()


class UnixNixWriter(NixWriter):
    def __init__(
        self,
        writer: asyncio.StreamWriter,
        identifier: str = "unknown",
    ) -> None:
        super().__init__(identifier=identifier)
        self.writer = writer

    def _write_to_transport(self, data: bytes) -> None:
        self.bytes_written += len(data)
        self.writer.write(data)

    async def _drain_transport(self) -> None:
        await self.writer.drain()

    def _transport_is_dirty(self) -> bool:
        return self.writer.transport.get_write_buffer_size() > 0

    async def close(self) -> None:
        # As `SSHNixWriter.close`: the transport closes whatever the drain does.
        try:
            await self.drain()
        finally:
            self.writer.close()
        await self.writer.wait_closed()


async def stream_parse_nar(
    src: NixReader,
    dst: NixWriter,
    capture: bool = False,
) -> bytes | None:
    """Copy a NAR archive from src to dst by parsing the NAR structure.

    The NAR format is a sequence of length-prefixed, 8-byte-padded tokens.
    We track "(" / ")" depth to know when the NAR ends.
    The token after "contents" is binary file data and must not be interpreted.

    If capture=True, returns the raw NAR bytes.
    """
    chunks: list[bytes] | None = [] if capture else None
    since_yield = 0

    async def _fwd_token() -> bytes:
        nonlocal since_yield
        """Read and forward one token, returning its raw data."""
        length: int = await src.read_uint64()
        dst.write_uint64(length)
        if chunks is not None:
            chunks.append(struct.pack("<Q", length))

        data: bytes = b""
        if length > 0:
            data = await src.readexactly(length)
            dst.write(data)
            if chunks is not None:
                chunks.append(data)

        pad: int = _nar_pad(length)
        if pad:
            pad_data: bytes = await src.readexactly(pad)
            dst.write(pad_data)
            if chunks is not None:
                chunks.append(pad_data)

        # Backpressure and a guaranteed suspension, per chunk of bytes rather
        # than per token. Neither the read nor `write` reaches the event loop
        # on its own, so a NAR otherwise runs to its end with nothing else
        # scheduled. Per token would be far more often than that needs: a NAR
        # holding 8871 files carries tens of thousands of tokens of a few
        # bytes each, and a loop turn per 32 KiB frame rather than per 1 MiB
        # chunk already measured 2.5x the CPU in `forward_framed`.
        since_yield += 8 + length + pad
        if since_yield >= _CHUNK_SIZE:
            since_yield = 0
            await dst.drain()
            await checkpoint()

        return data

    depth: int = 0
    after_contents: bool = False

    while True:
        raw: bytes = await _fwd_token()

        # Token after "contents" is binary file data — skip interpretation
        if after_contents:
            after_contents = False
            continue

        try:
            tok: str = raw.decode("ascii")
        except (UnicodeDecodeError, ValueError):
            continue

        if tok == "(":
            depth += 1
        elif tok == ")":
            depth -= 1
            if depth == 0:
                break
        elif tok == "contents":
            after_contents = True

    await dst.drain()

    if chunks is not None:
        return b"".join(chunks)
    return None


async def forward_framed(
    src: NixReader,
    dst: NixWriter,
    chunk_size: int = _CHUNK_SIZE,
    on_bytes: Callable[[int], None] | None = None,
) -> None:
    """Forward framed data (chunks terminated by size=0) from src to dst.

    Streams the data without buffering the entire payload in memory.
    Each chunk is: uint64 length + raw data, terminated by length=0.

    AddToStore (op 7) and AddToStoreNar (op 39) reach this. `nix copy` does
    not: that is AddMultipleToStore at protocol 1.32 and above,
    `remote-store.cc:508`.

    **The frames that go out are not the frames that came in.** Nix frames
    with `FramedSink`, a `BufferedSink` of 32 KiB (`serialise.hh:71,724`),
    and a frame boundary carries no meaning -- `FramedSource` reassembles the
    frames into one byte stream and never sees them (`serialise.hh:694`). So
    this gathers up to `chunk_size` of client frames into one outgoing frame.
    Measured on one 256 MiB path, this machine, uvloop: 8.6 ms/MiB frame for
    frame against 7.8 ms/MiB coalesced, and 206.9 MiB resident against 18.9.

    `on_bytes` takes each outgoing chunk's length. The caller passes its own
    counter rather than this naming one, because the op a transfer belongs to
    is known at the handler and not here.
    """
    pending: list[bytes] = []
    pending_size = 0

    async def _flush() -> None:
        nonlocal pending_size
        if not pending_size:
            return
        sent = pending_size
        dst.write_uint64(pending_size)
        dst.write(pending[0] if len(pending) == 1 else b"".join(pending))
        pending.clear()
        pending_size = 0
        # Backpressure. `write` hands the chunk to the transport and returns,
        # so without this the transport holds every byte the daemon has not
        # taken yet: measured peak/sent of 1.000 over 16 MiB in
        # tests/unit/test_forward_framed_stream.py.
        await dst.drain()
        # A guaranteed suspension, which `drain` is not: it returns without
        # reaching the loop below the high-water mark, and a read served from
        # a buffer never reaches it at all. The same test measured 0 turns of
        # the loop for a whole payload.
        await checkpoint()
        if on_bytes is not None:
            on_bytes(sent)

    while True:
        size = await src.read_uint64()
        if size == 0:
            break
        pending.append(await src.readexactly(size))
        pending_size += size
        if pending_size >= chunk_size:
            await _flush()

    await _flush()
    dst.write_uint64(0)
    await dst.drain()


async def forward_raw(
    src: NixReader,
    dst: NixWriter,
    size: int,
    chunk_size: int = _CHUNK_SIZE,
    on_bytes: Callable[[int], None] | None = None,
) -> None:
    """Forward exactly *size* unframed bytes from src to dst.

    The serving direction of a NAR: pynixd reads from the local daemon, which
    is a Unix socket that is nearly always ready, and writes to the client.
    Neither end reaches the event loop on its own, so the drain and the
    checkpoint per chunk are what keep the process answering while a closure
    moves. See `forward_framed`.

    `on_bytes` takes each chunk's length. The caller passes its own counter
    rather than this naming one, because the op a transfer belongs to is
    known at the handler and not here.
    """
    remaining = size
    while remaining > 0:
        to_read = min(remaining, chunk_size)
        chunk = await src.readexactly(to_read)
        dst.write(chunk)
        await dst.drain()
        await checkpoint()
        remaining -= to_read
        if on_bytes is not None:
            on_bytes(len(chunk))


class FramedReader(NixReader):
    """Reassembles framed chunks into a logical byte stream.

    Reads [uint64 size][data]... [uint64 0] from a DaemonReader and
    presents a readexactly() interface. Inherits from NixReader so it
    can be passed directly to code expecting a NixReader.

    This allows parsing structured data (PathInfo, NAR tokens, etc.)
    from inside framed payloads without buffering the entire payload.
    """

    def __init__(self, src: NixReader) -> None:
        super().__init__()
        self._src = src
        self._buf = bytearray()
        self._pos = 0
        self._eof = False

    def _compact(self) -> None:
        if self._pos > 0:
            del self._buf[: self._pos]
            self._pos = 0

    async def readexactly(self, n: int) -> bytes:
        while (len(self._buf) - self._pos) < n:
            if self._eof:
                have = len(self._buf) - self._pos
                raise EOFError(f"Framed stream ended, need {n} bytes, have {have}")
            size = await self._src.read_uint64()
            if size == 0:
                self._eof = True
                have = len(self._buf) - self._pos
                if have < n:
                    raise EOFError(f"Framed stream ended, need {n} bytes, have {have}")
                break
            data = await self._src.readexactly(size)
            self._buf.extend(data)
        result = bytes(self._buf[self._pos : self._pos + n])
        self._pos += n
        if self._pos > 64 * 1024:
            self._compact()
        return result

    async def ensure_eof(self) -> None:
        """Read until the end of the framed stream (size=0).

        Discards any remaining data in the current framed stream.
        This is important to ensure the underlying reader is positioned
        correctly for the next operation.
        """
        if self._eof:
            # We already saw the 0 frame, but there might be unread data in _buf.
            # However, for protocol sync, seeing the 0 frame from _src is what matters.
            return

        while True:
            size = await self._src.read_uint64()
            if size == 0:
                self._eof = True
                break
            await self._src.readexactly(size)

    async def is_dirty(self) -> bool:
        if self._pos < len(self._buf):
            return True
        return await self._src.is_dirty()

    @property
    def at_eof(self) -> bool:
        return self._eof and self._pos == len(self._buf)


class FramedWriter(NixWriter):
    """Buffers writes and emits framed chunks to an underlying writer.

    Framed format: [uint64 size][data]... [uint64 0] (terminator).
    Call finalize() when done to flush remaining data and write terminator.
    """

    def __init__(
        self,
        dst: NixWriter,
    ) -> None:
        super().__init__(identifier=dst.identifier)
        self._dst = dst

    def write_bytes(self, data: bytes) -> None:
        self._dst.write(data)

    def write(self, data: bytes) -> None:
        self._dst.write_uint64(len(data))
        self._dst.write(data)

    async def drain(self) -> None:
        """Drain the writer underneath, which is the one holding the bytes.

        `NixWriter.drain` would call this object's own `_drain_transport`, and
        a FramedWriter has no transport -- it writes through `_dst`.
        """
        await self._dst.drain()

    async def finalize(self) -> None:
        self._dst.write_uint64(0)  # terminator
        await self._dst.drain()
