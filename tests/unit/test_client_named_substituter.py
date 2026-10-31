"""The substituter that a client names reaches the plan and the work.

`--option substituters file:///...` names a cache that pynixd has no backend
for. pynixd asks the daemon behind it, which speaks to every kind of
substituter. Issue nixidae/nixidae#226.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.exceptions import DaemonProtocolError
from pynixd.goals.engine import GoalEngine
from pynixd.goals.query_missing import client_names_a_substituter
from pynixd.serde import (
    ContentAddress,
    DerivedPath as SerdeDerivedPath,
    EnsurePathRequest,
    EnsurePathResponse,
    IsValidPathRequest,
    IsValidPathResponse,
    QueryMissingRequest,
    QueryMissingResponse,
    QuerySubstitutablePathInfosRequest,
    QuerySubstitutablePathInfosResponse,
    SetOptionsRequest,
    StorePath,
    SubstitutablePathInfo,
    TimeSpan,
    Verbosity,
)
from pynixd.substitution_queue import SubstitutionAvailability

if TYPE_CHECKING:
    from pynixd.connection import ClientConn
    from pynixd.context import PynixdContext

PATH = "/nix/store/00000000000000000000000000000000-example"


def _options(**overrides: str) -> SetOptionsRequest:
    """A `SetOptions` request with every field, because no field has a default."""
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=False,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=1,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=1,
        use_substitutes=True,
        overrides=overrides,
    )


def _client(options: SetOptionsRequest | None) -> ClientConn:
    return cast("ClientConn", SimpleNamespace(options=options))


class FakeUpstream:
    """A local store that answers the two operations of this road."""

    def __init__(self, *, infos: list[SubstitutablePathInfo], ensure_error: str | None = None) -> None:
        self.infos = infos
        self.ensure_error = ensure_error
        self.requests: list[Any] = []
        self.valid: set[str] = set()

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        del kwargs
        self.requests.append(request)
        if isinstance(request, QuerySubstitutablePathInfosRequest):
            return QuerySubstitutablePathInfosResponse(infos=self.infos)
        if isinstance(request, EnsurePathRequest):
            if self.ensure_error is not None:
                raise DaemonProtocolError(self.ensure_error)
            self.valid.add(str(request.path))
            return EnsurePathResponse(value=1)
        if isinstance(request, IsValidPathRequest):
            return IsValidPathResponse(valid=str(request.path) in self.valid)
        raise AssertionError(f"unexpected request {type(request).__name__}")

    async def read_derivation(self, drv_store_path: Any) -> None:
        del drv_store_path
        return None


async def _plan(store: FakeUpstream, client: ClientConn | None) -> QueryMissingResponse:
    ctx = cast(
        "PynixdContext",
        SimpleNamespace(local_store=store, scheduler=None),
    )
    derived_paths: set[Any] = {SerdeDerivedPath(value=PATH)}
    request = QueryMissingRequest(derived_paths=cast("set[SerdeDerivedPath]", derived_paths))
    return await GoalEngine(ctx).query_missing(request, client)


def test_a_client_that_names_no_substituter_is_recognised() -> None:
    assert not client_names_a_substituter(None)
    assert not client_names_a_substituter(_client(None))
    assert not client_names_a_substituter(_client(_options()))


@pytest.mark.parametrize("name", ["substituters", "extra-substituters", "trusted-substituters"])
def test_each_substituter_option_counts(name: str) -> None:
    assert client_names_a_substituter(_client(_options(**{name: "file:///cache"})))


@pytest.mark.anyio
async def test_the_plan_reads_the_substituter_of_the_client() -> None:
    """The plan puts the path in `willSubstitute`, with the two sizes."""
    store = FakeUpstream(
        infos=[SubstitutablePathInfo(path=StorePath(path=PATH), download_size=11, nar_size=22)],
    )

    response = await _plan(store, _client(_options(substituters="file:///cache")))

    assert {str(path) for path in response.will_substitute} == {PATH}
    assert response.download_size == 11
    assert response.nar_size == 22


@pytest.mark.anyio
async def test_the_plan_asks_the_infos_operation_and_not_the_set_operation() -> None:
    """`QuerySubstitutablePaths` skips a cache whose `want-mass-query` is off."""
    store = FakeUpstream(
        infos=[SubstitutablePathInfo(path=StorePath(path=PATH), download_size=1, nar_size=2)],
    )

    await _plan(store, _client(_options(substituters="file:///cache")))

    asked = [req for req in store.requests if isinstance(req, QuerySubstitutablePathInfosRequest)]
    assert len(asked) == 1
    assert asked[0].paths == {StorePath(path=PATH): ContentAddress("")}


@pytest.mark.anyio
async def test_the_plan_asks_nothing_upstream_for_a_client_that_named_none() -> None:
    """A client that asked for no substituter pays no round trip."""
    store = FakeUpstream(infos=[])

    response = await _plan(store, _client(_options()))

    assert {str(path) for path in response.unknown} == {PATH}
    assert not [req for req in store.requests if isinstance(req, QuerySubstitutablePathInfosRequest)]


@pytest.mark.anyio
async def test_an_empty_answer_leaves_the_path_unknown() -> None:
    store = FakeUpstream(infos=[])

    response = await _plan(store, _client(_options(substituters="file:///cache")))

    assert {str(path) for path in response.unknown} == {PATH}
    assert not response.will_substitute


@pytest.mark.anyio
async def test_an_answer_about_another_path_does_not_count() -> None:
    other = "/nix/store/11111111111111111111111111111111-other"
    store = FakeUpstream(infos=[SubstitutablePathInfo(path=StorePath(path=other))])

    response = await _plan(store, _client(_options(substituters="file:///cache")))

    assert {str(path) for path in response.unknown} == {PATH}


def test_the_availability_of_a_missing_path_is_unavailable() -> None:
    """The helper that the plan falls back to states the negative answer."""
    assert not SubstitutionAvailability.unavailable().available
