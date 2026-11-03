"""pynixd's answers to `QuerySubstitutablePathInfo` and `QuerySubstitutablePathInfos`.

`querySubstitutablePathInfos` (`misc.cc:90`) takes, for each path, what the
highest-priority substituter holding it knows: the deriver, the references,
the narinfo's `FileSize` and the NAR size. `SubstitutionQueue.get_substituter`
picks that substituter. Nix answers nothing without `substitute`
(`misc.cc:92`), and writes the answers in the order of a `std::map`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.query_substitutable_path_info import QuerySubstitutablePathInfoRequest
from nix_daemon_protocol.query_substitutable_path_infos import QuerySubstitutablePathInfosRequest
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd.config import LocalSocketStoreSpec
from pynixd.goals.query_missing import client_names_a_substituter
from pynixd.serde import ContentAddress, NARHash, StorePath, Time
from pynixd.store.local_db import LocalDBStore
from pynixd.substitution_queue import SubstitutionCandidate

if TYPE_CHECKING:
    from pathlib import Path

HELD = StorePath(path="/nix/store/00000000000000000000000000000002-held")
SLOW = StorePath(path="/nix/store/00000000000000000000000000000001-slow")
MISSING = StorePath(path="/nix/store/00000000000000000000000000000003-missing")
DERIVER = StorePath(path="/nix/store/00000000000000000000000000000004-held.drv")


def _candidate(path: StorePath) -> SubstitutionCandidate:
    info = UnkeyedValidPathInfo(
        deriver=DERIVER,
        nar_hash=NARHash(hash="0" * 64),
        references={path, SLOW},
        registration_time=Time(ts=1),
        nar_size=456,
        ultimate=False,
        sigs=set(),
        ca=ContentAddress(value=""),
    )
    return SubstitutionCandidate(
        store=SimpleNamespace(store_id=StoreId("cache")),  # type: ignore[arg-type]  # the answer reads no store field
        path_info=ValidPathInfo(path=path, info=info),
        download_size=123,
    )


class _Queue:
    """Holds `HELD` and `SLOW`; `SLOW` answers once every path was asked, so they are asked at once."""

    def __init__(self, *uncovered: str) -> None:
        self.asked: list[StorePath] = []
        self.all_asked = anyio.Event()
        self.uncovered = uncovered
        self.expected = 3

    async def substituters_for(self, client: Any) -> tuple[object, ...] | None:
        """The daemon for a client naming no backend's substituter or an uncovered one; else this fake, as pynixd's backends."""
        options = getattr(client, "options", None)
        if options is not None and not options.use_substitutes:
            return ()
        return None if client_names_a_substituter(client) or self.uncovered else (self,)

    async def get_substituter(self, path: StorePath, stores: object = None) -> SubstitutionCandidate | None:
        self.asked.append(path)
        if len(self.asked) == self.expected:
            self.all_asked.set()
        if path == SLOW:
            await self.all_asked.wait()
        return None if path == MISSING else _candidate(path)


def _store(root: Path, queue: _Queue | None) -> LocalDBStore:
    store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=root))
    store.substitution_queue = queue  # type: ignore[assignment]  # the queue's one method these answers call
    return store


def _client(*, use_substitutes: int = 1, **overrides: str) -> Any:
    return SimpleNamespace(options=SimpleNamespace(use_substitutes=use_substitutes, overrides=overrides))


async def _infos(store: LocalDBStore, client: Any = None, *, ca: str = "") -> Any:
    request = QuerySubstitutablePathInfosRequest(
        paths={HELD: ContentAddress(""), SLOW: ContentAddress(ca), MISSING: ContentAddress("")}
    )
    with anyio.fail_after(5):
        return await store.own_query_substitutable_path_infos(request, client=client)


@pytest.mark.anyio
class TestQuerySubstitutablePathInfos:
    async def test_each_path_a_backend_holds_in_map_order(self, tmp_path: Path) -> None:
        queue = _Queue()
        answer = await _infos(_store(tmp_path, queue), _client())
        assert [info.path for info in answer.infos] == [SLOW, HELD]
        held = answer.infos[1]
        assert held.deriver == DERIVER
        assert held.references == {HELD, SLOW}
        assert (held.download_size, held.nar_size) == (123, 456)
        assert sorted(queue.asked) == sorted([HELD, SLOW, MISSING])

    async def test_without_substitute_nothing_is_substitutable(self, tmp_path: Path) -> None:
        queue = _Queue()
        answer = await _infos(_store(tmp_path, queue), _client(use_substitutes=0))
        assert answer.infos == []
        assert queue.asked == []

    async def test_a_content_address_gets_the_daemon(self, tmp_path: Path) -> None:
        """Nix makes the path again for each substituter's store directory (`misc.cc:106`)."""
        queue = _Queue()
        assert await _infos(_store(tmp_path, queue), _client(), ca="text:sha256:" + "0" * 52) is None
        assert queue.asked == []

    async def test_a_client_that_names_substituters_gets_the_daemon(self, tmp_path: Path) -> None:
        queue = _Queue()
        assert await _infos(_store(tmp_path, queue), _client(substituters="file:///tmp/cache")) is None
        assert queue.asked == []

    async def test_a_daemon_substituter_no_backend_stands_for_gets_the_daemon(self, tmp_path: Path) -> None:
        queue = _Queue("file:///tmp/cache")
        assert await _infos(_store(tmp_path, queue), _client()) is None
        assert queue.asked == []

    async def test_no_queue_gets_the_daemon(self, tmp_path: Path) -> None:
        assert await _infos(_store(tmp_path, None), _client()) is None


@pytest.mark.anyio
class TestQuerySubstitutablePathInfo:
    async def test_a_held_path_answers_its_info(self, tmp_path: Path) -> None:
        queue = _Queue()
        queue.expected = 1
        request = QuerySubstitutablePathInfoRequest(path=HELD)
        answer = await _store(tmp_path, queue).own_query_substitutable_path_info(request, client=_client())
        assert answer.found
        assert answer.info.deriver == DERIVER
        assert (answer.info.download_size, answer.info.nar_size) == (123, 456)

    async def test_a_missing_path_answers_not_found(self, tmp_path: Path) -> None:
        queue = _Queue()
        queue.expected = 1
        request = QuerySubstitutablePathInfoRequest(path=MISSING)
        answer = await _store(tmp_path, queue).own_query_substitutable_path_info(request, client=_client())
        assert not answer.found
        assert answer.info is None

    async def test_without_substitute_the_path_is_not_found(self, tmp_path: Path) -> None:
        request = QuerySubstitutablePathInfoRequest(path=HELD)
        answer = await _store(tmp_path, _Queue()).own_query_substitutable_path_info(
            request, client=_client(use_substitutes=0)
        )
        assert not answer.found
