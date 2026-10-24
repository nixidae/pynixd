"""A builder substitutes what its substituters hold, and pynixd pushes the rest.

nixidae/nixidae#1074.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from pynixd import scheduler as scheduler_module
from pynixd.daemon_extensions.query_closure_with_info import QueryClosureWithInfoResponse
from pynixd.exceptions import BackendError
from pynixd.scheduler import Scheduler
from pynixd.serde import QueryValidPathsRequest, QueryValidPathsResponse, StorePath
from pynixd.store import transfer
from tests.unit.test_builder_holds_inputs import BOEHM, INPUT, _info


@pytest.mark.anyio
async def test_the_builder_substitutes_the_inputs_before_pynixd_sends_them(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[QueryValidPathsRequest] = []
    streamed: list[dict[str, Any]] = []

    async def hold(_conn: object, _inputs: object) -> None:
        pass

    async def check(request: QueryValidPathsRequest) -> QueryValidPathsResponse:
        asked.append(request)
        return QueryValidPathsResponse(paths=set())

    async def stream(*_args: Any, **kwargs: Any) -> int:
        streamed.append(kwargs)
        return 0

    monkeypatch.setattr(scheduler_module, "stream_paths_store_to_store", stream)
    fake = SimpleNamespace(
        allocator=SimpleNamespace(strip_handled_features=lambda _build: None),
        local_store=None,
        _hold_inputs_on=hold,
    )
    build = SimpleNamespace(
        build_id="b1",
        request=SimpleNamespace(derivation=SimpleNamespace(input_srcs={StorePath(path=INPUT)})),
    )
    store = SimpleNamespace(store_id="sg1-n2", execute=check)

    await Scheduler._prepare_build(cast("Any", fake), cast("Any", build), cast("Any", store), cast("Any", object()))

    assert [r.substitute for r in asked] == [1]
    assert [kwargs["substitute"] for kwargs in streamed] == [True]


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
async def test_a_builder_that_failed_to_substitute_gets_a_plain_push(monkeypatch: pytest.MonkeyPatch) -> None:
    streamed: list[dict[str, Any]] = []

    async def hold(_conn: object, _inputs: object) -> None:
        pass

    async def check(_request: QueryValidPathsRequest) -> QueryValidPathsResponse:
        raise BackendError("pynixd: store sg1-n2 did not answer QueryValidPathsRequest within 300 s of silence")

    async def stream(*_args: Any, **kwargs: Any) -> int:
        streamed.append(kwargs)
        return 0

    monkeypatch.setattr(scheduler_module, "stream_paths_store_to_store", stream)
    fake = SimpleNamespace(
        allocator=SimpleNamespace(strip_handled_features=lambda _build: None),
        local_store=None,
        _hold_inputs_on=hold,
    )
    build = SimpleNamespace(
        build_id="b1",
        request=SimpleNamespace(derivation=SimpleNamespace(input_srcs={StorePath(path=INPUT)})),
    )
    store = SimpleNamespace(store_id="sg1-n2", execute=check)

    await Scheduler._prepare_build(cast("Any", fake), cast("Any", build), cast("Any", store), cast("Any", object()))

    assert [kwargs["substitute"] for kwargs in streamed] == [False]
