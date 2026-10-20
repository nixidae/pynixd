"""A builder keeps what a build needs, and a wrong "valid" costs a resend.

Seen on dynhetz, 2026-10-20: SG1 builders answered `QueryValidPaths` with
`boehm-gc-8.2.12` valid, then refused the copy with "path '...' is not
valid", five times. Nothing held the path on the builder between the check
and the copy, and the copy trusted the check. Issue #887.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd import scheduler as scheduler_module
from pynixd.daemon_extensions.query_closure_with_info import QueryClosureWithInfoResponse
from pynixd.exceptions import BackendError
from pynixd.scheduler import Scheduler
from pynixd.serde import (
    AddTempRootRequest,
    AddTempRootsRequest,
    ContentAddress,
    NARHash,
    QueryValidPathsRequest,
    QueryValidPathsResponse,
    StorePath,
    Time,
    UnkeyedValidPathInfo,
)
from pynixd.store import transfer
from pynixd.wire import FEATURE_ADD_TEMP_ROOTS

BOEHM = "/nix/store/x5lf684477ybm9xb0i70p0d89dp5w23b-boehm-gc-8.2.12"
NIX = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-nix-2.35.3"
INPUT = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-source"


def _info(path: str) -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path=path),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash="0" * 64),
            references=set(),
            registration_time=Time(ts=1),
            nar_size=96,
            ultimate=True,
            sigs=set(),
            ca=ContentAddress(value=""),
        ),
    )


class _Source:
    store_id = "local"

    def __init__(self, paths: list[str]) -> None:
        self.infos = [_info(p) for p in paths]

    async def execute(self, _request: object, client: object = None) -> QueryClosureWithInfoResponse:  # noqa: ARG002
        return QueryClosureWithInfoResponse(infos=self.infos)


class _Builder:
    """Says it holds `valid`, whatever its daemon then does."""

    store_id = "sg1-n2"

    def __init__(self, valid: set[str]) -> None:
        self.valid = valid

    async def execute(self, _request: object) -> QueryValidPathsResponse:
        return QueryValidPathsResponse(paths={StorePath(path=p) for p in self.valid})


def _refusal(path: str) -> BaseException:
    message = f"path '\x1b[35;1m{path}\x1b[0m' is not valid"
    return ExceptionGroup("unhandled errors in a TaskGroup", [BackendError(message)])


async def _copy(monkeypatch: pytest.MonkeyPatch, failures: list[BaseException]) -> list[set[str]]:
    sent: list[set[str]] = []

    async def stream(_src: Any, _dst: Any, to_transfer: list[ValidPathInfo], *_rest: Any) -> None:
        sent.append({str(info.path) for info in to_transfer})
        if failures:
            raise failures.pop(0)

    monkeypatch.setattr(transfer, "_stream_checked", stream)
    await transfer.stream_paths_store_to_store(
        cast("Any", _Source([BOEHM, NIX])),
        cast("Any", _Builder({BOEHM})),
        {StorePath(path=NIX)},
    )
    return sent


@pytest.mark.anyio
async def test_a_path_refused_after_valid_is_sent_on_the_next_round(monkeypatch: pytest.MonkeyPatch) -> None:
    sent = await _copy(monkeypatch, [_refusal(BOEHM)])

    assert sent == [{NIX}, {NIX, BOEHM}]


@pytest.mark.anyio
async def test_another_failure_is_not_healed(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(BaseExceptionGroup):
        await _copy(monkeypatch, [ExceptionGroup("g", [BackendError("disk full")])])


@pytest.mark.anyio
async def test_a_refused_path_the_check_did_not_call_valid_is_not_healed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sent already, and still refused: a resend would fail the same way."""
    with pytest.raises(BaseExceptionGroup):
        await _copy(monkeypatch, [_refusal(NIX)])


class _Conn:
    def __init__(self, features: frozenset[str]) -> None:
        self.standard_features = features
        self.calls: list[object] = []

    async def call(self, request: object, raise_on_error: bool = False) -> None:
        assert raise_on_error, "a root that did not hold must fail the build"
        self.calls.append(request)


def _holder() -> Any:
    return SimpleNamespace(local_store=_Source([INPUT, BOEHM]))


@pytest.mark.anyio
async def test_the_input_closure_is_rooted_in_one_call_when_the_builder_negotiated_it() -> None:
    conn = _Conn(frozenset({FEATURE_ADD_TEMP_ROOTS}))
    await Scheduler._hold_inputs_on(_holder(), cast("Any", conn), {StorePath(path=INPUT)})

    assert len(conn.calls) == 1
    request = conn.calls[0]
    assert isinstance(request, AddTempRootsRequest)
    assert {str(p) for p in request.paths} == {INPUT, BOEHM}


@pytest.mark.anyio
async def test_an_older_builder_gets_one_add_temp_root_per_path() -> None:
    conn = _Conn(frozenset())
    await Scheduler._hold_inputs_on(_holder(), cast("Any", conn), {StorePath(path=INPUT)})

    assert all(isinstance(r, AddTempRootRequest) for r in conn.calls)
    assert {str(cast("AddTempRootRequest", r).path) for r in conn.calls} == {INPUT, BOEHM}


@pytest.mark.anyio
async def test_the_roots_are_held_before_the_builder_is_asked_what_it_has(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    async def hold(_conn: object, _inputs: object) -> None:
        events.append("hold")

    async def check(request: object) -> QueryValidPathsResponse:
        if isinstance(request, QueryValidPathsRequest):
            events.append("check")
        return QueryValidPathsResponse(paths=set())

    async def stream(*_args: Any, **_kwargs: Any) -> None:
        events.append("copy")

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

    assert events == ["hold", "check", "copy"]
