"""Determinate Nix's `open-telemetry`: a `traceparent` string after each op code (nixidae/nixidae#1250).

`startOp` (`worker-protocol-connection.cc:261` of Determinate v3.24.0) writes it, and `processConnection` reads
it before it knows the op (`daemon.cc:1225`). A codec that misses it reads the string as the request's first field.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from nix_daemon_protocol.constants import FEATURE_OPEN_TELEMETRY, PROTOCOL_VERSION, STDERR_LAST
from nix_daemon_protocol.experimental_compiled import compile_codec
from nix_daemon_protocol.is_valid_path import IsValidPathRequest
from nix_daemon_protocol.store_path import StorePath
from pynixd.connection import Connection
from pynixd.serde.context import WriteContext
from pynixd.wire import BytesReader, BytesWriter

TRACED = frozenset({FEATURE_OPEN_TELEMETRY})
TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
PATH = StorePath("/nix/store/00000000000000000000000000000000-traced")


def _expected(traceparent: str | None) -> bytes:
    w = BytesWriter("test")
    w.write_uint64(IsValidPathRequest.op)
    if traceparent is not None:
        w.write_string(traceparent)
    w.write_string(str(PATH))
    return w.get_bytes()


async def test_both_codecs_write_the_trace_context_after_the_op_code() -> None:
    for features, traceparent in ((TRACED, TRACEPARENT), (frozenset(), None)):
        ctx_args: dict[str, Any] = {"version": PROTOCOL_VERSION, "features": features, "traceparent": TRACEPARENT}
        plain = BytesWriter("test")
        await IsValidPathRequest(path=PATH).to_writer(WriteContext(writer=plain, **ctx_args))
        compiled = BytesWriter("test")
        codec = compile_codec(IsValidPathRequest, PROTOCOL_VERSION, features)  # type: ignore[arg-type]  # zuban-bug: a class is hashable, but zuban reads the pydantic model's __hash__ as None
        await codec.write(IsValidPathRequest(path=PATH), WriteContext(writer=compiled, **ctx_args))

        assert plain.get_bytes() == compiled.get_bytes() == _expected(traceparent)


async def test_a_backend_call_carries_the_trace_context_of_its_client() -> None:
    answer = BytesWriter("daemon")
    answer.write_uint64(STDERR_LAST)
    answer.write_uint64(1)
    sent = BytesWriter("backend")
    conn = Connection(BytesReader(answer.get_bytes(), "daemon"), sent, "backend")
    conn.connected = True
    conn.version = PROTOCOL_VERSION
    conn.standard_features = TRACED
    client = SimpleNamespace(traceparent=TRACEPARENT, options=None)

    response = await conn.call(IsValidPathRequest(path=PATH), client=cast("Any", client))

    assert response.valid
    assert sent.get_bytes() == _expected(TRACEPARENT)
