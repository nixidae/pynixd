"""EnsurePath and QueryValidPaths with `substitute`, on pynixd's substitution goals.

`Store::ensurePath` (`build/entry-points.cc:99`) fails with the words of
`tryThrowBuildError`, which for a substitution goal are "path '...' is
required, but there is no substituter that can build it"
(`substitution-goal.cc:160`). `Store::substitutePaths` (`store-api.cc:724`)
skips derivations and turns a failure into a warning (`store-api.cc:738`).
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from nix_daemon_protocol.ensure_path import EnsurePathRequest
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest
from pynixd.config import LocalSocketStoreSpec
from pynixd.exceptions import BackendError
from pynixd.goals.engine import GoalEngine
from pynixd.goals.query_missing import client_names_a_substituter
from pynixd.goals.results import goal_failure, goal_success
from pynixd.goals.substitute import SubstituteAttempt
from pynixd.goals.substitute_paths import ensure_path, no_substituter, pynixd_substitutes, substitute_paths
from pynixd.op_routes import OpRoute
from pynixd.serde import IsValidPathResponse, StorePath as WirePath
from pynixd.store.local_db import LocalDBStore
from pynixd_store.hintfmt import magenta
from pynixd_store.store_path import StorePath

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

HELD = StorePath("/nix/store/00000000000000000000000000000001-held")
BROKEN = StorePath("/nix/store/00000000000000000000000000000002-broken")
MISSING = StorePath("/nix/store/00000000000000000000000000000003-missing")
DRV = StorePath("/nix/store/00000000000000000000000000000004-held.drv")
OTHER_BROKEN = StorePath("/nix/store/00000000000000000000000000000005-broken")


def _attempt(path: StorePath) -> SubstituteAttempt:
    if path in (BROKEN, OTHER_BROKEN):
        return SubstituteAttempt(found=True, result=goal_failure("pynixd: failed to substitute: the NAR is gone"))
    if path == MISSING:
        return SubstituteAttempt(found=False, result=goal_failure(f"pynixd: no substituter has path: {path}"))
    result = goal_success()
    result.produced_paths.add(path)
    return SubstituteAttempt(found=True, result=result)


class _Engine:
    """Answers each path as its name says, and roots what it made valid.

    A failed fetch writes its cause to the client, as `SubstitutePathGoal` does.
    """

    def __init__(self) -> None:
        self.asked: list[StorePath] = []
        self.substitutes: list[bool] = []

    async def substituter_ids_for(self, client: object) -> tuple[str, ...]:
        return ()

    async def substitute_paths(
        self,
        paths: Iterable[StorePath],
        hold: Any = None,
        *,
        substitutes: bool = True,
        ids: tuple[str, ...] | None = None,
        client: _Client | None = None,
    ) -> dict[StorePath, SubstituteAttempt]:
        self.substitutes.append(substitutes)
        attempts = {path: _attempt(path) for path in paths}
        for path in attempts:
            if path in (BROKEN, OTHER_BROKEN) and client is not None:
                await client.send(SimpleNamespace(text="\x1b[31;1merror:\x1b[0m the NAR is gone\n"))
        self.asked.extend(attempts)
        for attempt in attempts.values():
            for produced in attempt.result.produced_paths:
                await hold(produced)
        return attempts


class _Client:
    def __init__(self, *, use_substitutes: int = 1, **overrides: str) -> None:
        self.options = SimpleNamespace(use_substitutes=use_substitutes, overrides=overrides)
        self.logs: list[str] = []

    async def send(self, message: Any) -> None:
        self.logs.append(message.text)


class _Holds:
    def __init__(self) -> None:
        self.paths: list[StorePath] = []

    async def __call__(self, path: StorePath) -> None:
        self.paths.append(path)


async def _ensure(path: StorePath, client: _Client, holds: _Holds | None = None, engine: _Engine | None = None) -> Any:
    request = EnsurePathRequest(path=WirePath(path=str(path)))
    return await ensure_path(engine or _Engine(), request, client, holds or _Holds())  # type: ignore[arg-type]  # the fakes stand for the engine and the client


@pytest.mark.anyio
class TestEnsurePath:
    async def test_a_held_path_is_substituted_and_rooted(self) -> None:
        holds = _Holds()
        response = await _ensure(HELD, _Client(), holds)
        assert response.value == 1
        assert holds.paths == [HELD]

    async def test_a_miss_fails_with_nix_words_and_logs_nothing(self) -> None:
        client = _Client()
        with pytest.raises(BackendError, match=re.escape(no_substituter(MISSING))):
            await _ensure(MISSING, client)
        assert client.logs == []

    async def test_the_message_colours_the_path(self) -> None:
        assert no_substituter(MISSING) == (
            f"path '{magenta(MISSING)}' is required, but there is no substituter that can build it"
        )

    async def test_a_failed_fetch_reaches_the_client_and_fails_the_same(self) -> None:
        client = _Client()
        with pytest.raises(BackendError, match=re.escape(no_substituter(BROKEN))):
            await _ensure(BROKEN, client)
        assert len(client.logs) == 1
        assert "error:" in client.logs[0]
        assert "the NAR is gone" in client.logs[0]

    async def test_without_substitute_the_engine_is_told(self) -> None:
        """A worker with `use-substitutes` off has no substituter (`worker.cc:32`)."""
        engine = _Engine()
        await _ensure(HELD, _Client(use_substitutes=0), engine=engine)
        assert engine.substitutes == [False]


async def _substitute(paths: set[StorePath], client: _Client) -> tuple[QueryValidPathsRequest, _Engine, _Holds]:
    engine, holds = _Engine(), _Holds()
    request = QueryValidPathsRequest(paths={WirePath(path=str(p)) for p in paths}, substitute=1)
    answer = await substitute_paths(engine, request, client, holds)  # type: ignore[arg-type]  # the fakes stand for the engine and the client
    return answer, engine, holds


@pytest.mark.anyio
class TestSubstitutePaths:
    async def test_derivations_are_skipped_and_the_query_drops_the_flag(self) -> None:
        answer, engine, holds = await _substitute({HELD, DRV, MISSING}, _Client())
        assert sorted(engine.asked) == sorted([HELD, MISSING])
        assert holds.paths == [HELD]
        assert answer.substitute == 0
        assert {str(p) for p in answer.paths} == {str(HELD), str(DRV), str(MISSING)}

    async def test_a_miss_is_silent(self) -> None:
        client = _Client()
        await _substitute({MISSING}, client)
        assert client.logs == []

    async def test_one_failure_warns_with_its_own_words(self) -> None:
        client = _Client()
        await _substitute({BROKEN, HELD}, client)
        assert "the NAR is gone" in client.logs[0]
        assert client.logs[-1] == f"\x1b[35;1mwarning:\x1b[0m {no_substituter(BROKEN)}\n"

    async def test_two_failures_warn_with_one_line_naming_both(self) -> None:
        client = _Client()
        await _substitute({BROKEN, OTHER_BROKEN}, client)
        names = f"'{BROKEN}', '{OTHER_BROKEN}'"
        assert client.logs[-1] == f"\x1b[35;1mwarning:\x1b[0m build of {magenta(names)} failed\n"


def _local(root: Path, **routes: OpRoute) -> LocalDBStore:
    return LocalDBStore(
        LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root, op_routes=routes, pynixd_writes=True)
    )


class _Queue:
    def __init__(self, *uncovered: str) -> None:
        self.uncovered = uncovered

    async def substituters_for(self, client: Any) -> tuple[object, ...] | None:
        """The daemon for a client naming no backend's substituter or an uncovered one; else this fake, as pynixd's backends."""
        options = getattr(client, "options", None)
        if options is not None and not options.use_substitutes:
            return ()
        return None if client_names_a_substituter(client) or self.uncovered else (self,)


class TestWhoSubstitutes:
    async def test_the_daemon_by_default(self, tmp_path: Path) -> None:
        assert not await pynixd_substitutes(_local(tmp_path), _Client(), _Queue())  # type: ignore[arg-type]  # fakes

    async def test_pynixd_on_the_pynixd_route_of_ensure_path(self, tmp_path: Path) -> None:
        store = _local(tmp_path, EnsurePath=OpRoute.pynixd)
        assert await pynixd_substitutes(store, _Client(), _Queue())  # type: ignore[arg-type]  # fakes

    async def test_the_daemon_without_a_queue(self, tmp_path: Path) -> None:
        store = _local(tmp_path, EnsurePath=OpRoute.pynixd)
        assert not await pynixd_substitutes(store, _Client(), None)  # type: ignore[arg-type]  # fakes

    async def test_the_daemon_for_a_client_that_names_a_substituter(self, tmp_path: Path) -> None:
        store = _local(tmp_path, EnsurePath=OpRoute.pynixd)
        client = _Client(substituters="file:///tmp/cache")
        assert not await pynixd_substitutes(store, client, _Queue())  # type: ignore[arg-type]  # fakes

    async def test_the_daemon_for_a_substituter_no_backend_stands_for(self, tmp_path: Path) -> None:
        store = _local(tmp_path, EnsurePath=OpRoute.pynixd)
        assert not await pynixd_substitutes(store, _Client(), _Queue("s3://cache"))  # type: ignore[arg-type]  # fakes

    def test_the_route_needs_pynixd_writes(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="writes the store"):
            LocalDBStore(
                LocalSocketStoreSpec(
                    store_id=StoreId("local"), store_path=tmp_path, op_routes={"EnsurePath": OpRoute.pynixd}
                )
            )

    def test_the_route_has_no_shadow(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="cannot run beside the daemon"):
            _local(tmp_path, EnsurePath=OpRoute.shadow)


class _ValidStore:
    """Holds `HELD` alone."""

    async def execute(self, request: Any, mark: bool = True) -> IsValidPathResponse:
        return IsValidPathResponse(valid=str(request.path) == str(HELD))


@pytest.mark.anyio
class TestWithoutSubstitutes:
    async def test_a_valid_path_succeeds_and_a_missing_one_misses(self) -> None:
        """`PathSubstitutionGoal::init` asks the store before any substituter (`substitution-goal.cc:36`).

        `nix-shell.sh:81` of the Nix functional suite runs a shebang script
        with `--no-substitute`, and nix-shell sends EnsurePath for its valid
        derivation.
        """
        engine = GoalEngine(SimpleNamespace(local_store=_ValidStore(), scheduler=None))  # type: ignore[arg-type]  # the store is all this reads
        holds = _Holds()
        attempts = await engine.substitute_paths([HELD, MISSING], hold=holds, substitutes=False)
        assert attempts[HELD].found
        assert attempts[HELD].result.produced_paths == {HELD}
        assert not attempts[MISSING].found
        assert holds.paths == [HELD]
