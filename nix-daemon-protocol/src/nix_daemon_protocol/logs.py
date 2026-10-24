"""WireModel types for the Nix daemon stderr/log stream.

The stderr stream uses a tagged-union wire format::

    [uint64 code][message body][uint64 code][message body]...[uint64 STDERR_LAST]

Each message type has a ``code`` field that is written on the wire
(``serialize=True``) but skipped during body reading (``deserialize=False``)
because ``WireLogs.from_reader`` dispatches on code before delegating to the
individual message's ``from_reader``.

**Every `str` field here is opaque bytes, and never validated text.** The
wire `string` is a length-prefixed byte run (`readString` moves bytes, and
`worker-protocol-connection.cc:53` hands them to the sink unread), the
builder's output is split on `\\n` alone and emitted as a raw `std::string`
(`build-log.cc:39`), and the client prints what it got (`terminal.cc:39`
walks invalid UTF-8 one byte at a time). A builder prints whatever it
prints -- a gzip stream in the middle of `nixpkgs` fixup output -- and the
daemon forwards it without looking. Strict UTF-8 decoding fails the whole
build on those bytes, so each field below takes `text_errors` and
round-trips them. Issue nixidae/nixidae#662.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, cast

from .constants import (
    STDERR_ERROR,
    STDERR_LAST,
    STDERR_NEXT,
    STDERR_RESULT,
    STDERR_START_ACTIVITY,
    STDERR_STOP_ACTIVITY,
)
from .logging import deserialization_scope
from .protocol import FieldType
from .wire_message import WireField, WireModel

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from .context import ReadContext, WriteContext


# ── Helper: nested trace element ────────────────────────────────────


class TraceLine(WireModel):
    """A single trace entry inside ``LogError``."""

    pos: int = 0
    hint: str = WireField(default="", text_errors="surrogateescape")


# ── Helper: tagged-union field inside activities ─────────────────────


class ActivityField(WireModel):
    """A single typed field in ``LogStartActivity`` or ``LogResult``.

    Wire format: ``uint64 type`` then either ``uint64 valint`` (if INT)
    or ``string valstr`` (if STRING).
    """

    type: FieldType = FieldType.INT
    valint: int | None = WireField(default=None, wire_depends_on=lambda self: self.type == FieldType.INT)
    valstr: str | None = WireField(
        default=None,
        wire_depends_on=lambda self: self.type == FieldType.STRING,
        text_errors="surrogateescape",
    )


# ── Log message types ────────────────────────────────────────────────


class LogNext(WireModel):
    """STDERR_NEXT — a log line from the daemon."""

    code: int = WireField(default=STDERR_NEXT, serialize=True, deserialize=False)
    text: str = WireField(default="", text_errors="surrogateescape")


class LogStartActivity(WireModel):
    """STDERR_START_ACTIVITY — begin a tracked activity."""

    code: int = WireField(default=STDERR_START_ACTIVITY, serialize=True, deserialize=False)
    act_id: int = 0
    level: int = 0
    type: int = 0
    text: str = WireField(default="", text_errors="surrogateescape")
    fields: list[ActivityField] = WireField(default_factory=list)
    parent: int = 0


class LogStopActivity(WireModel):
    """STDERR_STOP_ACTIVITY — end a tracked activity."""

    code: int = WireField(default=STDERR_STOP_ACTIVITY, serialize=True, deserialize=False)
    act_id: int = 0


class LogResult(WireModel):
    """STDERR_RESULT — result data for an activity."""

    code: int = WireField(default=STDERR_RESULT, serialize=True, deserialize=False)
    act_id: int = 0
    result_type: int = 0
    fields: list[ActivityField] = WireField(default_factory=list)


class LogError(WireModel):
    """STDERR_ERROR — the daemon is reporting an error."""

    code: int = WireField(default=STDERR_ERROR, serialize=True, deserialize=False)
    type: str = WireField(default="", text_errors="surrogateescape")
    level: int = 0
    name: str = WireField(default="", text_errors="surrogateescape")
    msg: str = WireField(default="", text_errors="surrogateescape")
    have_pos: int = 0
    traces: list[TraceLine] = WireField(default_factory=list)


# ── Tagged-union container ───────────────────────────────────────────

# Union of all log message types — used for type annotations
LogMessage = LogNext | LogStartActivity | LogStopActivity | LogResult | LogError

# Dispatch table: uint64 code → message class
_LOG_PARSERS: dict[int, type[WireModel]] = {
    STDERR_NEXT: LogNext,
    STDERR_START_ACTIVITY: LogStartActivity,
    STDERR_STOP_ACTIVITY: LogStopActivity,
    STDERR_RESULT: LogResult,
    STDERR_ERROR: LogError,
}


_YIELD_EVERY = 256
"""Messages read between two turns of the event loop.

A read served from buffered bytes never suspends, and a daemon that
substitutes a closure keeps the buffer full: one `resProgress` per chunk it
copies. Without the turn the loop stalls for the whole stream. Issue #507."""


async def read_stream(ctx: ReadContext) -> AsyncIterator[LogMessage]:
    """Yield stderr/log messages until STDERR_LAST."""
    unknown_streak = 0
    count = 0
    while True:
        msg_type = await ctx.reader.read_uint64()

        if msg_type == STDERR_LAST:
            return

        parser = _LOG_PARSERS.get(msg_type)
        if parser is None:
            unknown_streak += 1
            if unknown_streak >= 3:
                raise ConnectionError(
                    f"Protocol desync: {unknown_streak} consecutive unknown stderr msg_types (last: 0x{msg_type:x})",
                )
            continue

        unknown_streak = 0
        msg = cast("LogMessage", await parser.from_reader(ctx))
        yield msg
        count += 1
        if count % _YIELD_EVERY == 0:
            await asyncio.sleep(0)
        if isinstance(msg, LogError):
            return


async def drain(ctx: ReadContext, raise_on_error: bool = True) -> LogError | None:
    """Read and discard all stderr/log messages until STDERR_LAST.

    **The exception carries the message of the daemon, and nothing in front of
    it.** A proxy sends that text on to its own client, which prints it after
    the word "error:". This put "Backend error: " in front, so a client of
    pynixd read `error: Backend error: Cannot delete path '...'`, and a client
    of `nix-daemon` read `error: Cannot delete path '...'`. The class of the
    exception already says where the message came from.
    """
    last_error: LogError | None = None
    async for msg in read_stream(ctx):
        if isinstance(msg, LogError):
            last_error = msg
            if raise_on_error:
                raise ctx.error_factory(msg.msg)
    return last_error


class WireLogs(WireModel):
    """Container for a complete stderr stream.

    ``from_reader`` reads a tagged-union stream (dispatching on ``code``),
    stopping at ``STDERR_LAST`` or after ``LogError``.

    ``to_writer`` writes each message (which includes its ``code``) followed
    by ``STDERR_LAST``.
    """

    messages: list[LogMessage] = WireField(default_factory=list)  # type: ignore[valid-type]

    def add(self, msg: LogMessage) -> None:  # type: ignore[valid-type]
        """Append a log message to the stream."""
        self.messages.append(msg)

    @classmethod
    async def from_reader(cls, ctx: ReadContext) -> WireLogs:
        """Read tagged-union stderr stream."""
        backend_error: Exception | None = None
        with deserialization_scope(ctx, cls):
            obj = cls.__new__(cls)
            object.__setattr__(obj, "__pydantic_fields_set__", {"messages"})
            object.__setattr__(obj, "__pydantic_extra__", None)
            object.__setattr__(obj, "__pydantic_private__", None)

            msgs: list[LogMessage] = []
            async for msg in read_stream(ctx):
                # STDERR_ERROR is a valid daemon response, not a decode failure.
                # It is kept and never streamed: the caller reports it, as the
                # raised error or inside the response, so a streamed copy
                # reaches the client twice.
                if isinstance(msg, LogError):
                    msgs.append(msg)
                    if ctx.raise_on_error:
                        backend_error = ctx.error_factory(msg.msg)
                    break
                if ctx.log_sink:
                    await ctx.log_sink.send(msg)
                if ctx.buffer_logs:
                    msgs.append(msg)

            object.__setattr__(obj, "messages", msgs)

        if backend_error is not None:
            raise backend_error
        return obj

    async def to_writer(self, ctx: WriteContext) -> None:
        """Write each message body (including its code) then STDERR_LAST."""
        for msg in self.messages:
            await msg.to_writer(ctx)
        ctx.writer.write_uint64(STDERR_LAST)
