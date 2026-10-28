"""A build log carries bytes, and the log stream carries them unchanged.

A builder prints whatever it prints: `containerd-2.3.5` failed 4/4 with
`pynixd: internal scheduler error` because its log holds a raw gzip stream
(`1f 8b 08 ...`) from the `gzipping man pages` fixup phase, and
`read_string` decoded the log stream as strict UTF-8. The C++ daemon moves
length-prefixed bytes it never validates -- `readString` hands them to the
sink unread (`worker-protocol-connection.cc:53`), the builder's output is
split on `\\n` alone (`build-log.cc:39`), and the client prints what it got
(`terminal.cc:39` walks invalid UTF-8 one byte at a time). So the log models
decode losslessly and encode back to the same bytes.

The Nix behaviour above is the mechanism, not a functional-suite line:
there is no Nix test that feeds binary through the log stream. The
docstring names the files instead.
"""

from __future__ import annotations

import json

import pytest

from nix_daemon_protocol.constants import (
    PROTOCOL_VERSION,
    STDERR_ERROR,
    STDERR_LAST,
    STDERR_NEXT,
    STDERR_RESULT,
)
from nix_daemon_protocol.experimental_compiled import compile_codec
from nix_daemon_protocol.logs import (
    LogError,
    LogNext,
    LogResult,
    WireLogs,
)
from nix_daemon_protocol.protocol import FieldType, ResultType
from nix_daemon_protocol.wire_message import WireField, WireModel
from pynixd.cli.base import _sanitize_for_journal
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.wire import BytesReader, BytesWriter

POISON = b"\x1f\x8b\x08\x00not-utf8\n"
"""The gzip magic plus text, as the `containerd` log holds it."""


def _ctx(data: bytes) -> ReadContext:
    return ReadContext(reader=BytesReader(data, "test"), version=PROTOCOL_VERSION, raise_on_error=False)


async def _round_trip(data: bytes) -> WireLogs:
    """Parse a raw stderr stream and serialize it back, byte for byte."""
    logs = await WireLogs.from_reader(_ctx(data))
    writer = BytesWriter("test")
    await logs.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    assert writer.get_bytes() == data
    return logs


async def _framed(code: int, text: bytes) -> bytes:
    writer = BytesWriter("test")
    writer.write_uint64(code)
    writer.write_bytes(text)
    writer.write_uint64(STDERR_LAST)
    return writer.get_bytes()


@pytest.mark.anyio
async def test_log_next_carries_raw_bytes() -> None:
    logs = await _round_trip(await _framed(STDERR_NEXT, POISON))

    assert len(logs.messages) == 1
    assert isinstance(logs.messages[0], LogNext)
    assert logs.messages[0].text.encode("utf-8", errors="surrogateescape") == POISON


@pytest.mark.anyio
async def test_build_log_line_result_carries_raw_bytes() -> None:
    """The builder's output travels as `STDERR_RESULT`, not as a log line."""
    msg = BytesWriter("test")
    msg.write_uint64(STDERR_RESULT)
    msg.write_uint64(7)  # act_id
    msg.write_uint64(ResultType.BUILD_LOG_LINE)
    msg.write_uint64(1)  # one field
    msg.write_uint64(FieldType.STRING)
    msg.write_bytes(POISON)
    msg.write_uint64(STDERR_LAST)
    logs = await _round_trip(msg.get_bytes())

    assert len(logs.messages) == 1
    assert isinstance(logs.messages[0], LogResult)
    assert logs.messages[0].fields[0].valstr is not None
    assert logs.messages[0].fields[0].valstr.encode("utf-8", errors="surrogateescape") == POISON


@pytest.mark.anyio
async def test_log_error_msg_carries_raw_bytes() -> None:
    """An error that quotes binary still parses; the error path must not crash."""
    msg = BytesWriter("test")
    msg.write_uint64(STDERR_ERROR)
    msg.write_bytes(b"Error")  # type
    msg.write_uint64(1)  # level
    msg.write_bytes(b"builder")  # name
    msg.write_bytes(POISON)  # msg
    msg.write_uint64(0)  # have_pos
    msg.write_uint64(0)  # no traces
    stream = msg.get_bytes()  # no STDERR_LAST: LogError ends the stream
    logs = await WireLogs.from_reader(_ctx(stream))

    assert len(logs.messages) == 1
    assert isinstance(logs.messages[0], LogError)
    assert logs.messages[0].msg.encode("utf-8", errors="surrogateescape") == POISON

    writer = BytesWriter("test")
    await logs.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    tail = BytesWriter("test")
    tail.write_uint64(STDERR_LAST)
    assert writer.get_bytes() == stream + tail.get_bytes()


@pytest.mark.anyio
async def test_compiled_codec_answers_lossless_fields() -> None:
    """The experiment compiles the flag too, byte-identical to the reference."""
    codec = compile_codec(LogNext, PROTOCOL_VERSION)  # type: ignore[arg-type]  # zuban-bug: a class is hashable, but zuban reads the pydantic model's __hash__ as None

    body = BytesWriter("test")
    body.write_bytes(POISON)
    raw_body = body.get_bytes()
    via_compiled = await codec.read(_ctx(raw_body))
    writer = BytesWriter("test")
    await codec.write(via_compiled, WriteContext(writer=writer, version=PROTOCOL_VERSION))

    # `to_writer` writes the `code` first: the dispatcher reads it, the body
    # codec answers what follows. The interpreted round trips above cover the
    # whole stream; this one pins the compiled body codec to the same bytes.
    framed = BytesWriter("test")
    framed.write_uint64(STDERR_NEXT)
    framed.write_bytes(POISON)
    assert writer.get_bytes() == framed.get_bytes()
    assert isinstance(via_compiled, LogNext)
    assert via_compiled.text.encode("utf-8", errors="surrogateescape") == POISON


def test_wire_field_refuses_lossy_handlers() -> None:
    with pytest.raises(ValueError, match="text_errors"):

        class _Bad(WireModel):
            text: str = WireField(default="", text_errors="replace")


def test_journal_sanitizer_replaces_surrogates() -> None:
    event = {"msg": "a\udc8bf", "nested": {"lines": ["ok", "b\udc8b"]}, "n": 3}

    cleaned = _sanitize_for_journal(event)

    assert cleaned == {"msg": "a\ufffdf", "nested": {"lines": ["ok", "b\ufffd"]}, "n": 3}
    json.dumps(cleaned).encode("utf-8")
