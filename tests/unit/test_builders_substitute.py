"""A builder substitutes what its substituters hold, and pynixd pushes the rest.

nixidae/nixidae#1074.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue
from pynixd.daemon_extensions.query_closure_with_info import QueryClosureWithInfoResponse
from pynixd.exceptions import BackendError
from pynixd.serde import QueryValidPathsRequest, QueryValidPathsResponse, StorePath
from pynixd.store import transfer
from tests._conftest.execution import FakeConn, Streamed, fake_executor, queued
from tests._conftest.placement import FakeStore
from tests.unit.test_builder_holds_inputs import BOEHM, INPUT, _info

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireRequest

SG2 = StoreId("sg1-n2")


async def _prepare(store: FakeStore) -> Streamed:
    """Prepare a build with one input on *store*, and answer what pynixd pushed."""
    queue = BuildQueue()
    streamed = Streamed()
    build = await queued(queue, input_srcs=[INPUT])
    await fake_executor(queue, {SG2: store}, stream=streamed).prepare_build(build, store, FakeConn())
    return streamed


@pytest.mark.anyio
async def test_the_builder_substitutes_the_inputs_before_pynixd_sends_them() -> None:
    asked: list[int | None] = []

    async def check(request: WireRequest) -> QueryValidPathsResponse:
        assert isinstance(request, QueryValidPathsRequest)
        asked.append(request.substitute)
        return QueryValidPathsResponse(paths=set())

    store = FakeStore(SG2)
    store.answer = check
    streamed = await _prepare(store)

    assert asked == [1]
    assert [substitute for _, _, substitute in streamed.calls] == [True]


class _Source:
    store_id = "local"

    async def execute(self, _request: object, client: object = None) -> QueryClosureWithInfoResponse:  # noqa: ARG002
        return QueryClosureWithInfoResponse(infos=[_info(INPUT), _info(BOEHM)])


class _Builder:
    """Substitutes BOEHM when asked to."""

    store_id = "sg1-n2"

    def __init__(self) -> None:
        self.flags: list[int | None] = []

    async def execute(self, request: QueryValidPathsRequest) -> QueryValidPathsResponse:
        self.flags.append(request.substitute)
        return QueryValidPathsResponse(paths={StorePath(path=BOEHM)} if request.substitute else set())


@pytest.mark.parametrize(("substitute", "pushed"), [(True, {INPUT}), (False, {INPUT, BOEHM})])
@pytest.mark.anyio
async def test_only_what_the_builder_cannot_substitute_is_pushed(
    monkeypatch: pytest.MonkeyPatch, substitute: bool, pushed: set[str]
) -> None:
    sent: list[set[str]] = []

    async def stream(_src: Any, _dst: Any, to_transfer: list[Any], *_rest: Any) -> int:
        sent.append({str(info.path) for info in to_transfer})
        return 0

    monkeypatch.setattr(transfer, "_stream_checked", stream)
    builder = _Builder()
    await transfer.stream_paths_store_to_store(
        cast("Any", _Source()), cast("Any", builder), {StorePath(path=INPUT)}, substitute=substitute
    )

    assert builder.flags == [int(substitute)]
    assert sent == [pushed]


@pytest.mark.anyio
async def test_a_builder_that_failed_to_substitute_gets_a_plain_push() -> None:
    async def check(_request: WireRequest) -> QueryValidPathsResponse:
        raise BackendError("pynixd: store sg1-n2 did not answer QueryValidPathsRequest within 300 s of silence")

    store = FakeStore(SG2)
    store.answer = check
    streamed = await _prepare(store)

    assert [substitute for _, _, substitute in streamed.calls] == [False]
