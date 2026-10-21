"""Codec fixes from the cross-reference against Nix 2.35.3. nixidae/nixidae#1006."""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.add_build_log import AddBuildLogRequest, AddBuildLogResponse
from nix_daemon_protocol.constants import PROTOCOL_VERSION, STDERR_LAST
from nix_daemon_protocol.drv_output import DrvOutput
from nix_daemon_protocol.keyed_drv_output import KeyedDrvOutput
from nix_daemon_protocol.query_realisation import QueryRealisationRequest, QueryRealisationResponse
from nix_daemon_protocol.realisation import Realisation
from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation
from pynixd.goals.ensure import _only_wanted_outputs  # noqa: PLC2701 -- the filter is the unit
from pynixd.handlers.add_build_log import AddBuildLogHandler
from pynixd.serde import BuildResult, BuildResultStatus, StorePath
from pynixd.serde.auth import Role
from pynixd.serde.context import WriteContext
from pynixd.store.daemon import answer_in_both_shapes
from pynixd.wire import BytesReader, BytesWriter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

DRV = "00000000000000000000000000000001-x.drv"
OUT = "/nix/store/00000000000000000000000000000002-x"


class Upstream:
    def __init__(self) -> None:
        answer = BytesWriter("upstream")
        answer.write_uint64(STDERR_LAST)
        answer.write_uint64(1)
        self.r = BytesReader(answer.get_bytes(), "upstream")
        self.w = BytesWriter("to-upstream")
        self.version = PROTOCOL_VERSION
        self.standard_features: frozenset[str] = frozenset()

    async def apply_options(self, _options: Any) -> None:
        return

    @asynccontextmanager
    async def transfer_conn(self, _options: Any = None) -> AsyncIterator[Upstream]:
        yield self


@pytest.mark.anyio
async def test_add_build_log_forwards_the_framed_log_and_a_base_name():
    """`daemon.cc:1013-1022` reads a base name, then a framed log."""
    client = BytesWriter("client")
    await AddBuildLogRequest(path=DRV).to_writer(WriteContext(writer=client, version=PROTOCOL_VERSION))
    client.write_uint64(5)
    client.write(b"hello")
    client.write_uint64(0)
    reader = BytesReader(client.get_bytes()[8:], "client")
    upstream = Upstream()
    proxy = SimpleNamespace(
        r=reader, version=PROTOCOL_VERSION, standard_features=frozenset(), client=None, local_store=upstream
    )

    answer = await AddBuildLogHandler().handle(cast("Any", SimpleNamespace(proxy=proxy, role=Role.ADMIN)))

    assert isinstance(answer, AddBuildLogResponse)
    sent = upstream.w.get_bytes()
    assert DRV.encode() in sent
    assert b"/nix/store/" not in sent
    assert sent.endswith(b"hello" + (0).to_bytes(8, "little"))
    # Every byte of the client's frames was read, so the next operation starts clean.
    assert reader._pos == len(reader._data)  # noqa: SLF001 -- the reader's position is the point


def test_unwanted_outputs_leave_the_feature_shape_too():
    """`derivation-goal.cc:255` keeps the wanted output alone, in either shape."""
    result = BuildResult(
        status=int(BuildResultStatus.BUILT),
        built_outputs_by_name={
            "out": UnkeyedRealisation(out_path=StorePath(OUT), signatures=set()),
            "dev": UnkeyedRealisation(out_path=StorePath(OUT + "-dev"), signatures=set()),
        },
    )

    kept = _only_wanted_outputs(result, {"out"})

    assert kept.built_outputs_by_name is not None
    assert set(kept.built_outputs_by_name) == {"out"}


def test_an_old_store_answer_reaches_a_new_client():
    """A 2.34 store answers a list; a client with the feature reads `present`."""
    request = QueryRealisationRequest(
        drv_output=DrvOutput("sha256:" + "1" * 64 + "!out"),
        keyed_drv_output=KeyedDrvOutput(drv_path=StorePath("/nix/store/" + DRV), output_name="out"),
    )
    old = QueryRealisationResponse(
        realisations=[
            Realisation(
                id=DrvOutput("sha256:" + "1" * 64 + "!out"),
                out_path=StorePath(OUT.removeprefix("/nix/store/")),
                signatures=[],
            )
        ]
    )

    both = answer_in_both_shapes(old, request)

    assert both.present == 1
    assert both.realisation is not None
    assert str(both.realisation.out_path) == OUT


def test_a_new_store_answer_reaches_an_old_client():
    request = QueryRealisationRequest(
        drv_output=DrvOutput("sha256:" + "1" * 64 + "!out"),
        keyed_drv_output=KeyedDrvOutput(drv_path=StorePath("/nix/store/" + DRV), output_name="out"),
    )
    new = QueryRealisationResponse(present=1, realisation=UnkeyedRealisation(out_path=StorePath(OUT), signatures=set()))

    both = answer_in_both_shapes(new, request)

    assert len(both.realisations) == 1
    assert str(both.realisations[0].id) == "sha256:" + "1" * 64 + "!out"
