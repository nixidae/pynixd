"""An untrusted client may not repair, and a NAR of an invalid path ends the session.

`daemon.cc:414`, `:500` and `:936` refuse `repair` from an untrusted client
before `startWork`: repairing is not atomic. pynixd's upstream connection is
root's, so the daemon allowed what Nix refuses. `NarFromPath` of an invalid
path answered nothing, so the client waited forever; Nix ends the session
(`daemon.cc:918`). nixidae/nixidae#1003.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.add_multiple_to_store import AddMultipleToStoreRequest
from nix_daemon_protocol.add_to_store_nar import AddToStoreNarRequest
from nix_daemon_protocol.constants import PROTOCOL_VERSION
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd.exceptions import ClosingError
from pynixd.handlers.add_multiple_to_store import AddMultipleToStoreHandler
from pynixd.handlers.add_to_store import AddToStoreHandler
from pynixd.handlers.add_to_store_nar import AddToStoreNarHandler
from pynixd.serde import AddToStoreRequest
from pynixd.serde.auth import Role
from pynixd.serde.context import WriteContext
from pynixd.wire import BytesReader, BytesWriter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from nix_daemon_protocol.wire_ops import WireRequest

PATH = StorePath("/nix/store/00000000000000000000000000000001-probe")


class Upstream:
    """Records whether anything reached the daemon behind pynixd."""

    def __init__(self) -> None:
        self.opened = False
        self.w = SimpleNamespace(write=self._write, drain=self._drain)
        self.written = b""

    def _write(self, data: bytes) -> None:
        self.written += data

    async def _drain(self) -> None:
        return

    async def apply_options(self, _options: Any) -> None:
        return

    @asynccontextmanager
    async def transfer_conn(self, _options: Any = None) -> AsyncIterator[Upstream]:
        self.opened = True
        yield self

    async def execute(self, _request: Any) -> Any:
        return SimpleNamespace(valid=False, info=None)


async def _body(request: WireRequest) -> bytes:
    writer = BytesWriter("test")
    await request.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    return writer.get_bytes()[8:]


async def _handle(handler: Any, request: WireRequest, role: Role, upstream: Upstream) -> None:
    proxy = SimpleNamespace(
        r=BytesReader(await _body(request), "test"),
        version=PROTOCOL_VERSION,
        standard_features=frozenset(),
        client=None,
        local_store=upstream,
        store_for_output_path=lambda _path: None,
        metrics_enabled=False,
    )
    await handler.handle(cast("Any", SimpleNamespace(proxy=proxy, role=role)))


def _info() -> ValidPathInfo:
    return ValidPathInfo(
        path=PATH,
        info=UnkeyedValidPathInfo(
            nar_hash=NARHash(hash="0" * 64),
            references=set(),
            registration_time=Time(ts=0),
            nar_size=8,
            ultimate=False,
            sigs=set(),
            ca=ContentAddress(""),
        ),
    )


REPAIRS = [
    (
        AddToStoreHandler(),
        AddToStoreRequest(path_name="probe", cam=ContentAddress("text:sha256"), references=set(), repair=1),
        "repair paths",
    ),
    (AddMultipleToStoreHandler(), AddMultipleToStoreRequest(repair=1, dont_check_sigs=0), "repair store paths"),
    (AddToStoreNarHandler(), AddToStoreNarRequest(info=_info(), repair=1, dont_check_sigs=0), "repair store paths"),
]


@pytest.mark.anyio
@pytest.mark.parametrize(("handler", "request_", "words"), REPAIRS, ids=["op7", "op44", "op39"])
async def test_an_untrusted_repair_is_refused_before_the_daemon_sees_it(
    handler: Any, request_: WireRequest, words: str
) -> None:
    upstream = Upstream()

    with pytest.raises(ClosingError, match=f"you are not privileged to {words}"):
        await _handle(handler, request_, Role.USER, upstream)

    assert upstream.written == b""

