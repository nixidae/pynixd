"""EnsurePath and QueryValidPaths with `substitute`, from a real binary cache, on pynixd's goals.

Every test store takes the `pynixd` route of EnsurePath (`TEST_OP_ROUTES`),
so the goal engine fetches here and the daemon does not. A cache written by
`nix copy` and served over HTTP stands for the substituter: a fake answer
proves nothing about what a remote holds.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from aiohttp import web

from nix_daemon_protocol.ensure_path import EnsurePathRequest
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest
from nix_daemon_protocol.store_path import StorePath
from pynixd import Server
from pynixd.config import HTTPBinaryCacheSpec
from pynixd.connection import Connection
from pynixd.exceptions import BackendError
from pynixd.store import LocalDBStore, Store
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

_PARENT = (
    'with import <nixpkgs> {}; runCommand "pynixd-substitute-parent" {} '
    '"echo ${builtins.toFile "pynixd-substitute-child" "the child of the substitute test"} > $out"'
)
"""Two paths of this test alone, `parent` naming `child`, so a fetch must bring the closure."""

MISSING = "/nix/store/00000000000000000000000000000000-pynixd-nobody-has-this"


@dataclass
class _Lab:
    target: Path
    cache_dir: Path
    socket: Path
    parent: str
    child: str
    unique: str
    """A path in the cache that nothing references."""


async def _valid(store_path: Path) -> set[str]:
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "path-info", "--store", str(store_path), "--all"], verbose=False
    )
    return {line.strip() for line in stdout.splitlines() if line.strip()}


@pytest.fixture
async def lab(tmp_path: Path) -> _Lab:
    """A cache that holds `parent`, `child` and `unique`, and an empty store to fetch them into."""
    source, target, cache_dir = tmp_path / "source", tmp_path / "target", tmp_path / "cache"
    for directory in (source, target, cache_dir):
        directory.mkdir()
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "build", "--impure", "--no-link", "--print-out-paths", "--expr", _PARENT], verbose=False
    )
    parent = stdout.strip()
    await run_subproc([str(NIX_BIN), "copy", "--no-check-sigs", "--to", str(source), parent])
    text = tmp_path / "unique.txt"
    text.write_text("a path only the cache has\n")  # noqa: ASYNC240 -- test setup
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "store", "add-path", "--store", str(source), str(text)], verbose=False
    )
    unique = stdout.strip()
    held = await _valid(source)
    child = next(path for path in held if path not in {parent, unique})
    await run_subproc(
        [str(NIX_BIN), "copy", "--no-check-sigs", "--from", str(source), "--to", f"file://{cache_dir}", *sorted(held)]
    )
    return _Lab(
        target=target, cache_dir=cache_dir, socket=tmp_path / "pynixd.sock", parent=parent, child=child, unique=unique
    )


@asynccontextmanager
async def _client(lab: _Lab) -> AsyncIterator[Connection]:
    """A client of pynixd over the empty store, with the served cache as its one substituter."""
    app = web.Application()
    app.router.add_static("/", lab.cache_dir)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    stores: dict[StoreId, Store] = {
        StoreId("local"): LocalDBStore(make_test_spec(store_id="local", store_path=lab.target, no_probe=True)),
        # Trusted: `nix copy` signs nothing, and the signature check is not what this tests.
        StoreId("upstream"): HTTPBinaryCacheSpec(
            store_id=StoreId("upstream"), url=f"http://{host}:{port}/", trusted=True
        ).to_store("upstream"),
    }
    try:
        async with Server(stores=stores, unix_path=lab.socket, ssh_port=None, http_port=None):
            link = await open_unix_connection(lab.socket)
            conn = Connection(UnixNixReader(link, identifier="t"), UnixNixWriter(link, identifier="t"), "t")
            await conn.connect()
            try:
                yield conn
            finally:
                await conn.close()
    finally:
        await runner.cleanup()


async def test_ensure_path_fetches_the_closure(lab: _Lab) -> None:
    """`Store::ensurePath` (`build/entry-points.cc:99`): the path and everything it names become valid."""
    async with _client(lab) as conn:
        response = await conn.call(EnsurePathRequest(path=StorePath(path=lab.parent)), raise_on_error=True)
    assert response.value == 1
    assert await _valid(lab.target) == {lab.parent, lab.child}


async def test_ensure_path_of_a_path_nobody_has_fails_with_nix_words(lab: _Lab) -> None:
    """`tryThrowBuildError` of a goal with no substitute (`substitution-goal.cc:160`)."""
    async with _client(lab) as conn:
        with pytest.raises(BackendError, match="is required, but there is no substituter that can build it"):
            await conn.call(EnsurePathRequest(path=StorePath(path=MISSING)), raise_on_error=True)
    assert await _valid(lab.target) == set()


async def test_query_valid_paths_substitutes_first(lab: _Lab) -> None:
    """`daemon.cc:334` runs `substitutePaths` before `queryValidPaths`: a cached path comes back valid."""
    paths = {StorePath(path=lab.unique), StorePath(path=MISSING)}
    async with _client(lab) as conn:
        response = await conn.call(QueryValidPathsRequest(paths=paths, substitute=1), raise_on_error=True)
    assert {str(path) for path in response.paths} == {lab.unique}
    assert await _valid(lab.target) == {lab.unique}


async def test_query_valid_paths_without_the_flag_fetches_nothing(lab: _Lab) -> None:
    async with _client(lab) as conn:
        response = await conn.call(
            QueryValidPathsRequest(paths={StorePath(path=lab.unique)}, substitute=0), raise_on_error=True
        )
    assert response.paths == set()
    assert await _valid(lab.target) == set()
