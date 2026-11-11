"""Determinate Nix's named activity crosses pynixd in the shape each peer reads (nixidae/nixidae#1250).

A daemon of Determinate v3.24.0 writes `STDERR_START_ACTIVITY_STRINGLY` to a peer that negotiated
`named-activities` (`daemon.cc:190`). pynixd names the feature to its backends, so the message reaches it, and
a client without the feature must get what that daemon would send it: `actStringly` with the text and no
fields (`logging.hh:225`). Anything else desyncs the client's stream.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.constants import (
    FEATURE_NAMED_ACTIVITIES,
    PROTOCOL_VERSION,
    STDERR_LAST,
    STDERR_START_ACTIVITY,
    STDERR_START_ACTIVITY_STRINGLY,
)
from nix_daemon_protocol.logs import (
    ActivityField,
    ActivityMetadatum,
    LogStartActivity,
    LogStartActivityStringly,
    WireLogs,
)
from nix_daemon_protocol.protocol import ActivityType, FieldType
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.wire import BytesReader, BytesWriter

NAMED = frozenset({FEATURE_NAMED_ACTIVITIES})


def _determinate() -> bytes:
    """`daemon.cc:190`: act, lvl, name, s, the metadata count, each key and field, parent; then `STDERR_LAST`."""
    w = BytesWriter("test")
    w.write_uint64(STDERR_START_ACTIVITY_STRINGLY)
    w.write_uint64(9)
    w.write_uint64(3)
    w.write_string("nix.eval")
    w.write_string("evaluating 'hello'")
    w.write_uint64(2)
    w.write_string("http.request.method")
    w.write_uint64(FieldType.STRING)
    w.write_string("GET")
    w.write_string("nix.count")
    w.write_uint64(FieldType.INT)
    w.write_uint64(42)
    w.write_uint64(4)
    w.write_uint64(STDERR_LAST)
    return w.get_bytes()


async def _read(data: bytes, features: frozenset[str]) -> WireLogs:
    ctx = ReadContext(reader=BytesReader(data, "test"), version=PROTOCOL_VERSION, features=features)
    return await WireLogs.from_reader(ctx)


async def _write(logs: WireLogs, features: frozenset[str]) -> bytes:
    writer = BytesWriter("test")
    await logs.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION, features=features))
    return writer.get_bytes()


@pytest.mark.anyio
async def test_a_peer_with_the_feature_gets_the_bytes_determinate_sent() -> None:
    logs = await _read(_determinate(), NAMED)

    assert logs.messages == [
        LogStartActivityStringly(
            act_id=9,
            level=3,
            name="nix.eval",
            text="evaluating 'hello'",
            metadata=[
                ActivityMetadatum(key="http.request.method", field=ActivityField(type=FieldType.STRING, valstr="GET")),
                ActivityMetadatum(key="nix.count", field=ActivityField(type=FieldType.INT, valint=42)),
            ],
            parent=4,
        )
    ]
    assert await _write(logs, NAMED) == _determinate()


@pytest.mark.anyio
async def test_a_peer_without_the_feature_gets_act_stringly_and_the_text() -> None:
    logs = await _read(_determinate(), NAMED)

    plain = await _write(logs, frozenset())

    w = BytesWriter("test")
    w.write_uint64(STDERR_START_ACTIVITY)
    w.write_uint64(9)
    w.write_uint64(3)
    w.write_uint64(ActivityType.STRINGLY)
    w.write_string("evaluating 'hello'")
    w.write_uint64(0)
    w.write_uint64(4)
    w.write_uint64(STDERR_LAST)
    assert plain == w.get_bytes()
    assert (await _read(plain, frozenset())).messages == [
        LogStartActivity(act_id=9, level=3, type=10113, text="evaluating 'hello'", parent=4)
    ]
