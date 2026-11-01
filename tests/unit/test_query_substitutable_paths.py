"""pynixd's answer to `QuerySubstitutablePaths`, from the scheduler's race.

`SubstitutionQueue.can_substitute` asks every substituter backend at once and
takes the first hit; the answer is the paths it finds. Nix answers nothing
without `substitute` (`store-api.cc:506`). A client that names substituters
of its own gets the daemon's answer, because pynixd's backends are not those.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_substitutable_paths import QuerySubstitutablePathsRequest
from pynixd.config import LocalSocketStoreSpec
from pynixd.serde import StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd.substitution_queue import SubstitutionAvailability

if TYPE_CHECKING:
    from pathlib import Path

HELD = StorePath(path="/nix/store/00000000000000000000000000000001-held")
SLOW = StorePath(path="/nix/store/00000000000000000000000000000002-slow")
MISSING = StorePath(path="/nix/store/00000000000000000000000000000003-missing")


class _Queue:
    """Holds `HELD` and `SLOW`; `SLOW` answers late, so the paths are asked at once or the test times out."""

    def __init__(self) -> None:
        self.asked: list[StorePath] = []
        self.both_asked = anyio.Event()

    async def can_substitute(self, path: StorePath) -> SubstitutionAvailability:
        self.asked.append(path)
        if len(self.asked) == 3:
            self.both_asked.set()
        if path == SLOW:
            await self.both_asked.wait()
        return SubstitutionAvailability(available=path != MISSING)


def _store(root: Path, queue: _Queue | None) -> LocalDBStore:
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root))
    store.substitution_queue = queue  # type: ignore[assignment]  # the queue's one method this answer calls
    return store


def _client(*, use_substitutes: int = 1, **overrides: str) -> Any:
    return SimpleNamespace(options=SimpleNamespace(use_substitutes=use_substitutes, overrides=overrides))


async def _ask(store: LocalDBStore, client: Any = None) -> set[StorePath] | None:
    request = QuerySubstitutablePathsRequest(paths={HELD, SLOW, MISSING})
    with anyio.fail_after(5):
        answer = await store.own_query_substitutable_paths(request, client=client)
    return None if answer is None else set(answer.paths)


@pytest.mark.anyio
class TestQuerySubstitutablePaths:
    async def test_the_paths_the_race_finds_asked_at_once(self, tmp_path: Path) -> None:
        queue = _Queue()
        assert await _ask(_store(tmp_path, queue), _client()) == {HELD, SLOW}
        assert sorted(queue.asked) == sorted([HELD, SLOW, MISSING])

    async def test_without_substitute_nothing_is_substitutable(self, tmp_path: Path) -> None:
        queue = _Queue()
        assert await _ask(_store(tmp_path, queue), _client(use_substitutes=0)) == set()
        assert queue.asked == []

    async def test_a_client_that_names_substituters_gets_the_daemon(self, tmp_path: Path) -> None:
        queue = _Queue()
        client = _client(substituters="file:///tmp/cache")
        assert await _ask(_store(tmp_path, queue), client) is None
        assert queue.asked == []

    async def test_no_queue_gets_the_daemon(self, tmp_path: Path) -> None:
        assert await _ask(_store(tmp_path, None), _client()) is None
