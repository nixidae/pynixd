"""The deployment can force substitute-on-destination. Issue #54.

`nix copy --substitute-on-destination` is a client flag, and
`nix-copy-ssh-common.sh:69-70` of the Nix functional suite asserts what it
buys: a copy from a store that lacks the path fetches it from the
destination's substituter instead of failing. A client that forgets the
flag sends the whole closure over the link even when the destination sits
next to a cache that holds every path, so the deployment takes the choice
over with `force_substitute_on_destination`: every client op 31 that did
not set the flag is forwarded with it set, and the upstream daemon runs
`substitutePaths` ahead of `queryValidPaths` (`daemon.cc:377`).

Two halves make that complete. The proxy rewrites the flag, and the
SQLite fast path yields on it: SQLite holds no substituter, so answering
from it would call a substitutable path invalid, with or without the
setting.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec, PynixdSettings
from pynixd.proxy import DaemonProxy
from pynixd.serde import QueryValidPathsRequest, QueryValidPathsResponse
from pynixd.store.local_db import LocalDBStore


class _RecordingStore:
    """A local store that answers empty and keeps what it was asked."""

    def __init__(self) -> None:
        self.seen: list[QueryValidPathsRequest] = []

    async def execute(self, request: Any, client: Any = None) -> QueryValidPathsResponse:
        self.seen.append(request)
        return QueryValidPathsResponse(paths=set())


def _proxy(force: bool) -> tuple[Any, _RecordingStore]:
    """A proxy whose local store records op 31, with the setting on or off.

    Built with `object.__new__`, so the instance is a `DaemonProxy` and
    internal `self.` calls resolve: calling `DaemonProxy.execute` unbound
    on a `SimpleNamespace` fails attribute lookup for the helpers it calls.
    """
    store = _RecordingStore()
    proxy = cast("Any", object.__new__(DaemonProxy))
    proxy.ctx = SimpleNamespace(
        settings=PynixdSettings(force_substitute_on_destination=force),
        local_store=store,
        store_for_output_path=lambda _path: None,
    )
    proxy.client = None
    return proxy, store


async def _forwarded_substitute(force: bool, sent: int | None) -> int | None:
    proxy, store = _proxy(force)
    await proxy.execute(QueryValidPathsRequest(paths=set(), substitute=sent))
    return store.seen[0].substitute


async def test_an_unset_flag_is_forwarded_unset_by_default() -> None:
    """Off keeps the bytes: the client asked, pynixd forwards."""
    assert await _forwarded_substitute(False, 0) == 0


async def test_an_unset_flag_is_forced_when_the_deployment_says_so() -> None:
    """On rewrites the flag, so the upstream substitutes from its own caches."""
    assert await _forwarded_substitute(True, 0) == 1


async def test_a_set_flag_is_never_cleared() -> None:
    """Forcing only adds: a client that asked keeps its answer."""
    assert await _forwarded_substitute(True, 1) == 1


async def test_an_old_client_is_covered_too() -> None:
    """`None` is a client whose protocol predates the flag, and forcing covers
    it: the upstream reads the field exactly when its version carries it."""
    assert await _forwarded_substitute(True, None) == 1


async def test_the_sqlite_fast_path_yields_on_substitute() -> None:
    """SQLite holds no substituter, so it must not answer a substitute query.

    Answering from the mirror would call a path the substituters hold
    invalid. Yielding (`None`) falls through to the wire in
    `DaemonStore.execute`, where the daemon substitutes first. The store is
    never started here: the yield happens before any database is touched.
    """
    store = LocalDBStore(
        LocalSocketStoreSpec(
            store_id=StoreId("local"),
            store_path=Path("/"),
        )
    )
    assert await store.own_query_valid_paths(QueryValidPathsRequest(paths=set(), substitute=1)) is None
