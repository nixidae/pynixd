"""EnsurePath and QueryValidPaths with `substitute`, from a real binary cache, on pynixd's goals.

Every test store takes the `pynixd` route of EnsurePath (`TEST_OP_ROUTES`),
so the goal engine fetches here and the daemon does not. A cache written by
`nix copy` stands for the substituter, served over HTTP and read as a
`file://` directory, or written to an S3 bucket, configured in pynixd or named by the client: a fake
answer proves nothing about what a remote holds.
"""

from __future__ import annotations

import contextlib
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
from pynixd.config import HTTPBinaryCacheSpec, LocalBinaryCacheSpec, S3BinaryCacheSpec
from pynixd.connection import Connection
from pynixd.exceptions import BackendError
from pynixd.serde import SetOptionsRequest
from pynixd.store import LocalDBStore, Store
from pynixd.store_url import spec_for
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.client_options import set_options
from tests._conftest.nix_cli_store import nix_cli_store
from tests._conftest.nix_config import for_test_store
from tests._conftest.s3_bucket import s3_bucket
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from pynixd.substitution_queue import SubstitutionQueue

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
    transport: str
    """`http`: the cache served by aiohttp; `file`: the directory itself.

    `client-http` and `client-file`: the same, named by the client's
    `substituters`, with no backend configured in pynixd.
    """
    socket: Path
    parent: str
    child: str
    unique: str
    """A path in the cache that nothing references."""
    queue: SubstitutionQueue | None = None
    s3_url: str | None = None
    """The `s3://` URL of the bucket, for the `s3` transports."""


async def _valid(store_path: Path) -> set[str]:
    async with nix_cli_store(store_path) as store:
        _rc, stdout, _stderr, _both = await run_subproc(
            [str(NIX_BIN), "path-info", "--store", store, "--all"], verbose=False
        )
    return {line.strip() for line in stdout.splitlines() if line.strip()}


@pytest.fixture(params=["http", "file", "s3", "client-http", "client-file", "client-s3"])
async def lab(
    tmp_path_factory: pytest.TempPathFactory, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[_Lab]:
    """A cache that holds `parent`, `child` and `unique`, and an empty store to fetch them into.

    Not `tmp_path`: the parameter puts a `[` into its name, and Nix cannot parse that in a `file://` URL. The
    `s3` caches are a bucket that Nix's own S3 store writes.
    """
    tmp_path = tmp_path_factory.mktemp(f"substitute-{request.param}")
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
    with contextlib.ExitStack() as stack:
        cache_url = f"file://{cache_dir}"
        s3_url = None
        if request.param.endswith("s3"):
            endpoint = stack.enter_context(s3_bucket(monkeypatch, tmp_path / "xdg-cache")).endpoint
            s3_url = cache_url = f"s3://cache?endpoint={endpoint}&region=us-east-1"
        await run_subproc(
            [str(NIX_BIN), "copy", "--no-check-sigs", "--from", str(source), "--to", cache_url, *sorted(held)]
        )
        yield _Lab(
            target=target,
            cache_dir=cache_dir,
            transport=request.param,
            socket=tmp_path / "pynixd.sock",
            parent=parent,
            child=child,
            unique=unique,
            s3_url=s3_url,
        )


def _options(**overrides: str) -> SetOptionsRequest:
    """A `SetOptions` request with every field, because no field has a default."""
    return set_options(overrides=overrides)


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
    # Trusted: `nix copy` signs nothing, and the signature check is not what this tests.
    url = (
        f"http://{host}:{port}/"
        if lab.transport.endswith("http")
        else lab.s3_url
        if lab.s3_url is not None
        else f"file://{lab.cache_dir}"
    )
    # No substituter but the cache: the guest's root store holds `parent` and
    # `child` too, since the fixture built them there. nixidae/nixidae#1208.
    local = make_test_spec(
        store_id="local", store_path=lab.target, no_probe=True, nix_config=for_test_store(substituters=())
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(local)}
    if lab.transport == "http":
        stores[StoreId("upstream")] = HTTPBinaryCacheSpec(url=url, trusted=True).to_store("upstream")
    elif lab.transport == "file":
        stores[StoreId("upstream")] = LocalBinaryCacheSpec(path=lab.cache_dir, trusted=True).to_store("upstream")
    elif lab.transport == "s3":
        spec = spec_for(f"{url}&trusted=1")
        assert isinstance(spec, S3BinaryCacheSpec)
        stores[StoreId("upstream")] = spec.to_store("upstream")
    try:
        async with Server(stores=stores, unix_path=lab.socket, ssh_port=None, http_port=None) as server:
            link = await open_unix_connection(lab.socket)
            conn = Connection(UnixNixReader(link, identifier="t"), UnixNixWriter(link, identifier="t"), "t")
            await conn.connect()
            if lab.transport.startswith("client"):
                trusted = "&trusted=1" if "?" in url else "?trusted=1"
                await conn.call(_options(substituters=f"{url}{trusted}"), raise_on_error=True)
            lab.queue = server.ctx.scheduler.substitution_queue if server.ctx.scheduler is not None else None
            try:
                yield conn
            finally:
                await conn.close()
    finally:
        await runner.cleanup()


async def test_ensure_path_fetches_the_closure(lab: _Lab) -> None:
    """`Store::ensurePath` (`build/entry-points.cc:99`): the path and everything it names become valid.

    A client-named cache gets a backend of pynixd's; the daemon would fetch it
    too, so the backend is what proves pynixd did.
    """
    async with _client(lab) as conn:
        response = await conn.call(EnsurePathRequest(path=StorePath(path=lab.parent)), raise_on_error=True)
        if lab.transport.startswith("client"):
            assert lab.queue is not None
            assert len(lab.queue.client_substituters.stores()) == 1
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
