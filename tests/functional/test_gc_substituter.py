"""The collector drops what an upstream substituter already holds.

Issue #52. Every test here uses a real store, a real binary cache written by
`nix copy`, and a real HTTP server, because the rule is about what a remote
answers and a fake answer proves nothing about that.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest
from aiohttp import web

from nix_daemon_protocol import CollectGarbageRequest
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.wire_ops import WireRequest
from pynixd import Server
from pynixd.config import HTTPBinaryCacheSpec
from pynixd.connection import Connection
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.gc import Collector
from pynixd.store import LocalDBStore, Store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


@asynccontextmanager
async def _serve(directory: Path) -> AsyncIterator[str]:
    """Serve *directory* as a binary cache, and give back its base URL."""
    app = web.Application()
    app.router.add_static("/", directory)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        host, port = runner.addresses[0][:2]
        yield f"http://{host}:{port}/"
    finally:
        await runner.cleanup()


@dataclass
class _Lab:
    store_path: Path
    cache_dir: Path
    unique: str
    """A path that only the local store has."""
    spare: str
    """A second added file. Nothing references it, so it is droppable alone."""
    parent: str
    child: str
    """`parent` names `child` in its contents, so `child` is a reference of it."""


async def _valid(store_path: Path) -> set[str]:
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "path-info", "--store", str(store_path), "--all"],
        verbose=False,
    )
    return {line.strip() for line in stdout.splitlines() if line.strip()}


async def _closure(store_path: Path, path: str) -> set[str]:
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "path-info", "--store", str(store_path), "--recursive", path],
        verbose=False,
    )
    return {line.strip() for line in stdout.splitlines() if line.strip()}


async def _push(lab: _Lab, paths: set[str]) -> None:
    await run_subproc(
        [
            str(NIX_BIN),
            "copy",
            "--no-check-sigs",
            "--from",
            str(lab.store_path),
            "--to",
            f"file://{lab.cache_dir}",
            *sorted(paths),
        ],
    )


@asynccontextmanager
async def _pynixd(lab: _Lab, url: str | None, gc_defer: bool = True) -> AsyncIterator[Server]:
    """A server over the lab store, with *url* as the only deferred substituter."""
    # `no_probe`: the capability probe adds `probe-feature-*` paths to the
    # store, and the pass would then be measured against a store that grew
    # under it.
    spec = make_test_spec(
        store_id="local",
        store_path=lab.store_path,
        no_probe=True,
        # The tests below run EXECUTE: the permit is this lab run's opt-in.
        gc_allow_execute=True,
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    if url is not None:
        cache = HTTPBinaryCacheSpec(store_id=StoreId("upstream"), url=url, gc_defer=gc_defer)
        stores[StoreId("upstream")] = cache.to_store("upstream")
    async with Server(stores=stores, ssh_port=None, http_port=None) as server:
        yield server


_PARENT = (
    'with import <nixpkgs> {}; runCommand "pynixd-gc-parent" {} '
    '"echo ${builtins.toFile "pynixd-gc-child" "the child of the gc test"} > $out"'
)
"""Two paths of this test alone, with one reference between them.

**Not a package of nixpkgs.** `LocalStore::findRuntimeRoots` reads `/proc` of
the whole machine, and this store keeps the `/nix/store` prefix, so every
library a process of the host has mapped is a root here. `glibc` is alive in
this store although nothing in it refers to `glibc`, and a test built on a real
closure measures that instead of the rule.
"""


@pytest.fixture
async def lab(tmp_path: Path) -> _Lab:
    """A store with two files, and a pair of paths that reference each other."""
    store_path = tmp_path / "store"
    cache_dir = tmp_path / "cache"
    store_path.mkdir()
    cache_dir.mkdir()

    async def _add(name: str, text: str) -> str:
        source = tmp_path / name
        source.write_text(text)  # noqa: ASYNC240 -- test setup
        _rc, stdout, _stderr, _both = await run_subproc(
            [str(NIX_BIN), "store", "add-path", "--store", str(store_path), str(source)],
            verbose=False,
        )
        return stdout.strip()

    unique = await _add("only-here.txt", "a path no substituter has\n")
    spare = await _add("spare.txt", "a path the cache has, and nothing needs\n")

    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "build", "--impure", "--no-link", "--print-out-paths", "--expr", _PARENT],
        verbose=False,
    )
    parent = stdout.strip()
    await run_subproc([str(NIX_BIN), "copy", "--no-check-sigs", "--to", str(store_path), parent])
    child = next(path for path in await _closure(store_path, parent) if path != parent)

    return _Lab(
        store_path=store_path,
        cache_dir=cache_dir,
        unique=unique,
        spare=spare,
        parent=parent,
        child=child,
    )


async def test_the_pass_drops_what_the_cache_has_and_keeps_what_it_lacks(lab: _Lab) -> None:
    valid = await _valid(lab.store_path)
    await _push(lab, valid - {lab.unique})

    async with _serve(lab.cache_dir) as url, _pynixd(lab, url) as server:
        resp = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)

    assert {str(path) for path in resp.store_paths} == valid - {lab.unique}
    assert resp.bytes > 0
    assert await _valid(lab.store_path) == {lab.unique}


async def test_one_path_the_cache_lacks_keeps_its_whole_closure(lab: _Lab) -> None:
    """The referrer rule.

    `gc.cc:581` deletes the referrers of a named path as well, so a path unique
    to this store must keep every cached path under it. The cache holds `child`
    here, and the pass leaves it because `parent` is not in the cache and needs
    it. The spare file, which the cache also holds and nothing references,
    still goes.
    """
    valid = await _valid(lab.store_path)
    await _push(lab, valid - {lab.parent, lab.unique})

    async with _serve(lab.cache_dir) as url, _pynixd(lab, url) as server:
        resp = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)

    assert {str(path) for path in resp.store_paths} == {lab.spare}
    assert await _valid(lab.store_path) == {lab.parent, lab.child, lab.unique}


async def test_an_unreachable_substituter_keeps_everything(lab: _Lab) -> None:
    valid = await _valid(lab.store_path)
    await _push(lab, valid)

    # A port nothing listens on. Every narinfo request fails, and a store that
    # cannot answer confirms nothing.
    async with _pynixd(lab, "http://127.0.0.1:1/") as server:
        resp = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)

    assert resp.store_paths == set()
    assert await _valid(lab.store_path) == valid


async def test_a_substituter_nobody_deferred_to_drops_nothing(lab: _Lab) -> None:
    """The default. `gc_defer` is off everywhere, so an air-gapped lab keeps
    every path until somebody names a substituter."""
    valid = await _valid(lab.store_path)
    await _push(lab, valid)

    async with _serve(lab.cache_dir) as url, _pynixd(lab, url, gc_defer=False) as server:
        resp = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)

    assert resp.store_paths == set()
    assert await _valid(lab.store_path) == valid


async def test_chunk_refusals_keep_roots_and_release_cross_chunk_references(
    lab: _Lab,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #12: one rooted path cannot prevent the rest of a group leaving."""
    monkeypatch.setattr("pynixd.gc._DELETE_BATCH_SIZE", 2)
    root = lab.store_path / "nix/var/nix/gcroots/chunk-test"
    root.symlink_to(lab.unique)  # noqa: ASYNC240 -- test setup
    async with _pynixd(lab, None) as server:
        response = await Collector(server.ctx)._delete(
            [lab.child, lab.unique, lab.parent, lab.spare],
            {},
            [],
            None,
        )
    assert {str(path) for path in response.store_paths} == {lab.child, lab.parent, lab.spare}
    assert await _valid(lab.store_path) == {lab.unique}
    assert response.bytes > 0


async def test_real_daemon_deletes_205_paths_in_three_requests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store_path = tmp_path / "chunk-store"
    store_path.mkdir()  # noqa: ASYNC240 -- test setup
    paths = []
    for i in range(205):
        source = tmp_path / f"chunk-{i}"
        source.write_text(f"chunk test {i}\n")  # noqa: ASYNC240 -- test setup
        _rc, stdout, _stderr, _both = await run_subproc(
            [str(NIX_BIN), "store", "add-path", "--store", str(store_path), str(source)],
            verbose=False,
        )
        paths.append(stdout.strip())
    assert len(paths) == 205
    lab = _Lab(store_path, tmp_path / "cache", "", "", "", "")
    calls: list[int] = []
    original = Connection.call

    async def record(self: Connection, request: WireRequest, **kwargs: Any) -> Any:
        if isinstance(request, CollectGarbageRequest):
            calls.append(len(request.paths_to_delete))
        return await original(self, request, **kwargs)

    monkeypatch.setattr(Connection, "call", record)
    async with _pynixd(lab, None) as server:
        response = await Collector(server.ctx)._delete(paths, {}, [], None)
    assert {str(path) for path in response.store_paths} == set(paths)
    assert calls == [100, 100, 5]
    assert await _valid(store_path) == set()


async def test_execute_orders_cross_group_referrers_before_deletion(lab: _Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pynixd.gc._DELETE_BATCH_SIZE", 1)
    monkeypatch.setattr("pynixd.gc._by_weight", lambda weights, pressure: [lab.child, lab.parent])
    calls: list[set[str]] = []
    original = Connection.call

    async def record(self: Connection, request: WireRequest, **kwargs: Any) -> Any:
        if isinstance(request, CollectGarbageRequest):
            calls.append({str(path) for path in request.paths_to_delete})
        return await original(self, request, **kwargs)

    monkeypatch.setattr(Connection, "call", record)
    async with _pynixd(lab, None) as server:
        collector = Collector(server.ctx)
        monkeypatch.setattr(collector, "plan", AsyncMock(return_value={StorePath(lab.child), StorePath(lab.parent)}))
        response = await collector.run(PynixdGCAction.EXECUTE)
    assert {str(path) for path in response.store_paths} == {lab.child, lab.parent}
    assert calls == [{lab.parent}, {lab.child}]
