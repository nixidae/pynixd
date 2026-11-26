"""Two copies to one store send a shared path once.

Builds that share a missing input each ran `QueryValidPaths`, each found the
input missing, and each sent it. sg1's central took 38 imports of one path at
once and ran out of worker threads. Issue nixidae/nixidae#1643.
"""

from __future__ import annotations

from typing import Any, cast

import anyio
import pytest

from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd.daemon_extensions.query_closure_with_info import QueryClosureWithInfoResponse
from pynixd.exceptions import BackendError
from pynixd.serde import (
    ContentAddress,
    NARHash,
    QueryValidPathsResponse,
    StorePath,
    Time,
    UnkeyedValidPathInfo,
)
from pynixd.store import transfer


def _path(name: str) -> StorePath:
    return StorePath(path=f"/nix/store/{'0' * 32}-{name}")


def _info(name: str, *references: str) -> ValidPathInfo:
    return ValidPathInfo(
        path=_path(name),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash="0" * 64),
            references={_path(name), *map(_path, references)},
            registration_time=Time(ts=1),
            nar_size=96,
            ultimate=True,
            sigs=set(),
            ca=ContentAddress(value=""),
        ),
    )


_CLOSURE = {"shared": _info("shared"), "a": _info("a", "shared"), "b": _info("b", "shared")}


class _Source:
    store_id = "source"

    async def execute(self, request: Any, client: object = None) -> QueryClosureWithInfoResponse:  # noqa: ANN401, ARG002 -- the request types of two ops
        names = {str(path).rsplit("-", 1)[-1] for path in request.paths}
        return QueryClosureWithInfoResponse(infos=[_CLOSURE[name] for name in sorted(names | {"shared"})])


class _Destination:
    store_id = "destination"

    async def execute(self, _request: object) -> QueryValidPathsResponse:
        return QueryValidPathsResponse(paths=set())


class _Sends:
    """A `_stream_checked` that records each send and holds it until released."""

    def __init__(self, *, fail_first: bool = False) -> None:
        self.sent: list[list[str]] = []
        self.release = anyio.Event()
        self.fail_first = fail_first

    async def __call__(self, _src: object, _dst: object, to_transfer: list[ValidPathInfo], *_: object) -> int:
        self.sent.append([str(info.path).rsplit("-", 1)[-1] for info in to_transfer])
        first = len(self.sent) == 1
        if first:
            await self.release.wait()
        if first and self.fail_first:
            raise BackendError("connection lost")
        return len(to_transfer)


async def _copy(root: str, flights: transfer.Flights | None, errors: list[BaseException]) -> None:
    try:
        await transfer.stream_paths_store_to_store(
            cast("Any", _Source()), cast("Any", _Destination()), [_path(root)], flights=flights
        )
    except BackendError as e:
        errors.append(e)


@pytest.mark.parametrize("flights", [True, False], ids=["flights", "negative-control"])
async def test_a_path_in_flight_is_sent_once(monkeypatch: pytest.MonkeyPatch, *, flights: bool) -> None:
    """The second copy waits for the shared path and sends only its own path, after the shared one lands."""
    sends = _Sends()
    monkeypatch.setattr(transfer, "_stream_checked", sends)
    shared = transfer.Flights() if flights else None
    errors: list[BaseException] = []
    async with anyio.create_task_group() as tg:
        tg.start_soon(_copy, "a", shared, errors)
        await anyio.wait_all_tasks_blocked()
        tg.start_soon(_copy, "b", shared, errors)
        await anyio.wait_all_tasks_blocked()
        if flights:
            assert sends.sent == [["shared", "a"]]
        else:
            assert sends.sent == [["shared", "a"], ["shared", "b"]]
        sends.release.set()
    assert not errors
    if flights:
        assert sends.sent == [["shared", "a"], ["b"]]


async def test_a_failed_flight_leaves_the_path_to_the_waiter(monkeypatch: pytest.MonkeyPatch) -> None:
    """The first copy fails, so the copy that waited for it sends the shared path itself."""
    sends = _Sends(fail_first=True)
    monkeypatch.setattr(transfer, "_stream_checked", sends)
    flights = transfer.Flights()
    errors: list[BaseException] = []
    async with anyio.create_task_group() as tg:
        tg.start_soon(_copy, "a", flights, errors)
        await anyio.wait_all_tasks_blocked()
        tg.start_soon(_copy, "b", flights, errors)
        await anyio.wait_all_tasks_blocked()
        sends.release.set()
    assert len(errors) == 1
    assert sends.sent == [["shared", "a"], ["shared", "b"]]


async def test_a_cancelled_flight_leaves_the_path_to_the_waiter(monkeypatch: pytest.MonkeyPatch) -> None:
    """A build cancelled mid-copy must not leave a waiter blocked or the path marked as landed."""
    sends = _Sends()
    monkeypatch.setattr(transfer, "_stream_checked", sends)
    flights = transfer.Flights()
    errors: list[BaseException] = []
    async with anyio.create_task_group() as tg:
        first = anyio.CancelScope()
        tg.start_soon(_copy_in, first, "a", flights, errors)
        await anyio.wait_all_tasks_blocked()
        tg.start_soon(_copy, "b", flights, errors)
        await anyio.wait_all_tasks_blocked()
        first.cancel()
    assert not errors
    assert sends.sent == [["shared", "a"], ["shared", "b"]]


async def _copy_in(scope: anyio.CancelScope, root: str, flights: transfer.Flights, errors: list[BaseException]) -> None:
    with scope:
        await _copy(root, flights, errors)
