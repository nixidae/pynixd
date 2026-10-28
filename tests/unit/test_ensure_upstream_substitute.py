"""The work follows the plan when a client names a substituter.

`QueryMissingPlanGoal` answers `willSubstitute` for a path that the cache of
the client holds, and `EnsureDerivedPathGoal` must then fetch that path the
same way. `tests/unit/test_client_named_substituter.py` holds the plan half.
Issue nixidae/nixidae#226.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import anyio
import pytest

from nix_daemon_protocol.exceptions import DaemonProtocolError
from pynixd.config import PynixdSettings
from pynixd.derived_path import DerivedPath
from pynixd.goals.ensure import EnsureDerivedPathGoal
from pynixd.goals.results import result_succeeded
from pynixd.serde import (
    BuildMode,
    EnsurePathRequest,
    EnsurePathResponse,
    IsValidPathRequest,
    IsValidPathResponse,
    SetOptionsRequest,
    TimeSpan,
    Verbosity,
)
from pynixd.store_path import StorePath

if TYPE_CHECKING:
    from pynixd.connection import ClientConn
    from pynixd.goals.engine import GoalEngine

_DRV = "/nix/store/00000000000000000000000000000000-cached.drv"
_PATH = StorePath("/nix/store/11111111111111111111111111111111-cached")


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


class FakeLocalStore:
    """A local store that either holds the path upstream, or does not."""

    def __init__(self, *, ensure_error: str | None = None) -> None:
        self.ensure_error = ensure_error
        self.requests: list[Any] = []
        self.valid: set[str] = set()

    async def execute(self, request: Any, **kwargs: Any) -> Any:
        self.requests.append((type(request).__name__, kwargs.get("client") is not None))
        if isinstance(request, EnsurePathRequest):
            if self.ensure_error is not None:
                raise DaemonProtocolError(self.ensure_error)
            self.valid.add(str(request.path))
            return EnsurePathResponse(value=1)
        if isinstance(request, IsValidPathRequest):
            return IsValidPathResponse(valid=str(request.path) in self.valid)
        raise AssertionError(f"unexpected request {type(request).__name__}")


def _goal(store: FakeLocalStore, client: ClientConn | None) -> EnsureDerivedPathGoal:
    engine = SimpleNamespace(ctx=SimpleNamespace(local_store=store, settings=PynixdSettings()))
    goal = EnsureDerivedPathGoal(
        engine=cast("GoalEngine", engine),
        derived_path=DerivedPath(f"{_DRV}!out"),
        build_mode=BuildMode.NORMAL,
        substituter_ids=(),
    )
    if client is not None:
        goal._watchers.append(client)
    return goal


def _client(options: SetOptionsRequest | None) -> ClientConn:
    return cast("ClientConn", SimpleNamespace(options=options))


@pytest.mark.anyio
async def test_the_upstream_daemon_fetches_the_path() -> None:
    """`EnsurePath` upstream, then `IsValidPath` to confirm the fetch."""
    store = FakeLocalStore()

    result = await _goal(store, _client(_options(substituters="file:///cache")))._try_substitute_upstream(_PATH)

    assert result is not None
    assert result_succeeded(result.result)
    assert result.produced_paths == {_PATH}
    assert [name for name, _ in store.requests] == ["EnsurePathRequest", "IsValidPathRequest"]


@pytest.mark.anyio
async def test_a_missing_path_is_not_a_failure_of_the_goal() -> None:
    """`EnsurePath` answers an error for a path no substituter holds.

    The goal must take the build road after that answer. It raised the error
    instead, so `nix copy` through pynixd aborted rather than build.
    """
    store = FakeLocalStore(ensure_error="path '...' is required, but there is no substituter that can build it")

    result = await _goal(store, _client(_options(substituters="file:///cache")))._try_substitute_upstream(_PATH)

    assert result is None


@pytest.mark.anyio
async def test_a_client_that_named_no_substituter_asks_nothing() -> None:
    store = FakeLocalStore()

    result = await _goal(store, _client(_options()))._try_substitute_upstream(_PATH)

    assert result is None
    assert store.requests == []


@pytest.mark.anyio
async def test_an_invalid_path_after_the_fetch_is_not_a_success() -> None:
    """A daemon that answers `EnsurePath` without the path leaves it missing."""
    store = FakeLocalStore()
    store.valid = set()

    goal = _goal(store, _client(_options(substituters="file:///cache")))

    async def no_write(request: Any, **kwargs: Any) -> Any:
        store.requests.append((type(request).__name__, kwargs.get("client") is not None))
        if isinstance(request, EnsurePathRequest):
            return EnsurePathResponse(value=1)
        return IsValidPathResponse(valid=False)

    store.execute = no_write  # type: ignore[method-assign]  # the fake states one behaviour for one test

    assert await goal._try_substitute_upstream(_PATH) is None


@pytest.mark.anyio
async def test_a_hung_upstream_fetch_fails_fast_to_the_build_road() -> None:
    """An upstream `EnsurePath` that never answers must not hang the goal.

    The live daemon waited 25 minutes on two inputs with no timeout and no
    log line. Issue #79.
    """
    store = FakeLocalStore()
    never = anyio.Event()

    async def never_answers(request: Any, **kwargs: Any) -> Any:
        store.requests.append((type(request).__name__, kwargs.get("client") is not None))
        if isinstance(request, EnsurePathRequest):
            await never.wait()
            raise AssertionError("unreachable: the import budget cancels the wait")
        return IsValidPathResponse(valid=False)

    store.execute = never_answers  # type: ignore[method-assign]  # the fake states one behaviour for one test
    goal = _goal(store, _client(_options(substituters="file:///cache")))
    goal.engine.ctx.settings = PynixdSettings(substitution_import_timeout=0.01)

    started = time.monotonic()
    assert await goal._try_substitute_upstream(_PATH) is None
    assert time.monotonic() - started < 5.0
