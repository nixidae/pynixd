"""Op 49 holds a set of temporary roots with one round trip. Issue #66."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from nix_daemon_protocol.add_temp_roots import AddTempRootsRequest, AddTempRootsResponse
from nix_daemon_protocol.constants import PROTOCOL_VERSION
from nix_daemon_protocol.store_path import StorePath
from pynixd.handlers.add_temp_roots import AddTempRootsHandler
from pynixd.serde.context import ReadContext, RequestContext, WriteContext
from pynixd.wire import BytesReader, BytesWriter

FIRST = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-first"
SECOND = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-second"


def _paths() -> set[StorePath]:
    return {StorePath(path=FIRST), StorePath(path=SECOND)}


async def _encode_request() -> bytes:
    writer = BytesWriter("test")
    await AddTempRootsRequest(paths=_paths()).to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    return writer.get_bytes()


@pytest.mark.anyio
async def test_request_writes_op_49_then_the_path_set() -> None:
    """The op code leads, and the body decodes back to the same set."""
    data = await _encode_request()

    assert int.from_bytes(data[:8], "little") == 49
    req = await AddTempRootsRequest.from_reader(
        ReadContext(reader=BytesReader(data[8:], "test"), version=PROTOCOL_VERSION)
    )
    assert {str(path) for path in req.paths} == {FIRST, SECOND}


@pytest.mark.anyio
async def test_response_carries_success() -> None:
    """The answer is the uint64 1 that `gc.cc` writes."""
    writer = BytesWriter("test")
    await AddTempRootsResponse(value=1).to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    resp = await AddTempRootsResponse.from_reader(
        ReadContext(reader=BytesReader(writer.get_bytes(), "test"), version=PROTOCOL_VERSION)
    )
    assert resp.value == 1


@pytest.mark.anyio
async def test_handler_holds_every_path_of_the_set() -> None:
    """Each path reaches `add_temp_root`: the roots belong to the session."""
    data = await _encode_request()
    held: list[str] = []

    async def record(path: StorePath | str) -> None:
        held.append(str(path))

    proxy = SimpleNamespace(
        r=BytesReader(data[8:], "test"),
        version=PROTOCOL_VERSION,
        standard_features=frozenset(),
        add_temp_root=record,
    )
    proxy.read_context = ReadContext(reader=proxy.r, version=proxy.version, features=proxy.standard_features)
    resp = cast(
        "AddTempRootsResponse",
        await AddTempRootsHandler().handle(cast("RequestContext", SimpleNamespace(proxy=proxy))),
    )

    assert set(held) == {FIRST, SECOND}
    assert resp.value == 1
