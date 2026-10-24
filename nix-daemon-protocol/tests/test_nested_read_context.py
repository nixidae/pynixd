"""A nested wire field reads under the settings of the read that contains it.

`WireResponse.logs` is a `WireLogs`, so every response of the protocol reads
its stderr stream as a nested field. `WireLogs.from_reader` is the one reader
that acts on `error_factory` and on `raise_on_error`, and a nested read built
a context that carried neither.

The cost was measured in CI: pynixd's capability probe catches `BackendError`,
the daemon answered `you are not privileged to build input-addressed
derivations`, the nested read raised `DaemonProtocolError` instead, and 149
functional tests failed at their fixture because the store could not start.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol import PROTOCOL_VERSION
from nix_daemon_protocol.constants import STDERR_ERROR, STDERR_LAST, STDERR_NEXT
from nix_daemon_protocol.context import ReadContext
from nix_daemon_protocol.exceptions import DaemonProtocolError
from nix_daemon_protocol.io import BytesReader, BytesWriter
from nix_daemon_protocol.wire_ops import WireResponse


class _OwnError(Exception):
    """What a caller asks for in place of `DaemonProtocolError`."""


class _Response(WireResponse):
    """A response whose whole body is the inherited `logs` field."""


def _stderr_error(message: str) -> bytes:
    """One STDERR_ERROR record, as the daemon writes it."""
    writer = BytesWriter()
    writer.write_uint64(STDERR_ERROR)
    writer.write_string("Error")  # type
    writer.write_uint64(0)  # level
    writer.write_string("Error")  # name
    writer.write_string(message)
    writer.write_uint64(0)  # have_pos
    writer.write_uint64(0)  # traces
    writer.write_uint64(STDERR_LAST)
    return writer.get_bytes()


def _context(**kwargs: object) -> ReadContext:
    return ReadContext(
        reader=BytesReader(_stderr_error("you are not privileged"), identifier="nested"),
        version=PROTOCOL_VERSION,
        **kwargs,  # type: ignore[arg-type]
    )


async def test_error_factory_reaches_the_nested_log_stream() -> None:
    with pytest.raises(_OwnError, match="you are not privileged"):
        await _Response.from_reader(_context(error_factory=_OwnError))


async def test_the_default_factory_still_answers() -> None:
    with pytest.raises(DaemonProtocolError, match="you are not privileged"):
        await _Response.from_reader(_context())


async def test_raise_on_error_does_not_reach_the_nested_log_stream() -> None:
    """A nested stream raises whatever the caller asked not to raise.

    `DaemonStore.call` declares `raise_on_error=False` as its default, so
    honouring the flag here makes every daemon error a silent success.
    Measured: `_try_substitute_upstream` read a failed `EnsurePath` as a hit,
    and `nix build` waited 112 s for a substitution that never came. Issue
    #46 holds the question of what the default should be.
    """
    with pytest.raises(_OwnError):
        await _Response.from_reader(_context(error_factory=_OwnError, raise_on_error=False))


class _Sink:
    def __init__(self) -> None:
        self.sent: list[object] = []

    async def send(self, message: object, /) -> None:
        self.sent.append(message)


def _next_then_last() -> bytes:
    writer = BytesWriter()
    writer.write_uint64(STDERR_NEXT)
    writer.write_string("copying path")
    writer.write_uint64(STDERR_LAST)
    return writer.get_bytes()


async def test_a_nested_read_streams_to_the_log_sink_and_keeps_nothing() -> None:
    """The client reads the stream live, and `proxy.py` writes the response back without it.

    A response kept every message until the answer, so a substituting
    `QueryValidPaths` held one per chunk of the closure. Issue #507.
    """
    sink = _Sink()
    ctx = ReadContext(
        reader=BytesReader(_next_then_last(), identifier="nested"),
        version=PROTOCOL_VERSION,
        log_sink=sink,
        buffer_logs=False,
    )
    response = await _Response.from_reader(ctx)
    assert [getattr(m, "text", None) for m in sink.sent] == ["copying path"]
    assert response.logs.messages == []


async def test_a_nested_read_does_not_stream_the_error() -> None:
    """The caller reports the error, so a streamed copy would reach the client twice."""
    sink = _Sink()
    with pytest.raises(DaemonProtocolError):
        await _Response.from_reader(_context(log_sink=sink))
    assert sink.sent == []
