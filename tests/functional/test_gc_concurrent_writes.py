"""A path written while a collection runs on the no-daemon route stays (nixidae/nixidae#1440).

Nix keeps writes and its collector apart with temporary roots and the GC lock
(`gc.cc`): a root that a client adds after the collector read the roots
reaches it over the roots socket. pynixd collects and writes in one process
with no nix-daemon, so each write here must reach that socket too.

Each test holds the collection at Nix's second synchronisation point, after
the roots and before any delete (`gc.cc:537`, `gc-non-blocking.sh`), writes,
then lets it go on. A path added before the collection, with no root, is the
negative control: it goes.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from functools import partial
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.gc import Collector
from pynixd.store import LocalDBStore, Store
from pynixd_store.collect import SYNC_AFTER_ROOTS
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc
from tests.functional.test_gc_max_age import MAX_AGE, _backdate, _freeze_tracker

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

NIX_STORE = NIX_BIN.with_name("nix-store")
DEP = (
    'derivation { name = "gc-concurrent-dep"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo dep > $out" ]; }'
)
TOP = (
    f"let dep = {DEP}; in "
    'derivation { name = "gc-concurrent-top"; system = builtins.currentSystem; builder = "/bin/sh"; '
    'args = [ "-c" "echo ${dep} > $out" ]; }'
)


@asynccontextmanager
async def _serve(tmp_path: Path, **spec_args: Any) -> AsyncIterator[tuple[str, Server]]:  # `make_test_spec`'s own
    """A store with no nix-daemon: every operation on pynixd's own route."""
    root = tmp_path / "store"
    socket = tmp_path / "pynixd.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        daemon=False,
        nix_config=for_test_store(substituters=()),
        **spec_args,
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None) as s:
        yield f"unix://{socket}?root={root}", s


@pytest.fixture
def hold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The FIFO each collection reads after its roots: the collector runs in this process."""
    fifo = tmp_path / "gc-sync"
    os.mkfifo(fifo)
    monkeypatch.setenv(SYNC_AFTER_ROOTS, str(fifo))
    return fifo


@asynccontextmanager
async def _held(fifo: Path) -> AsyncIterator[None]:
    """Enter once a collection waits at the FIFO; it goes on when the block ends."""
    with anyio.fail_after(60):
        # `open` for writing returns when the reader opens: the collector is at its synchronisation point.
        fd = await anyio.to_thread.run_sync(partial(os.open, fifo, os.O_WRONLY), abandon_on_cancel=True)
    try:
        yield
    finally:
        os.close(fd)


async def _add(url: str, tmp_path: Path, name: str) -> str:
    source = tmp_path / "files" / name
    source.parent.mkdir(exist_ok=True)
    source.write_text(f"{name}\n")  # noqa: ASYNC240 -- test setup
    _rc, out, _err, _both = await run_subproc([NIX_BIN, "store", "add-path", "--store", url, str(source)])
    return out.strip()


async def _valid(url: str) -> set[str]:
    _rc, out, _err, _both = await run_subproc([NIX_BIN, "path-info", "--store", url, "--all"])
    return set(out.split())


async def _collect(url: str) -> None:
    with anyio.fail_after(60):
        await run_subproc([NIX_BIN, "store", "gc", "--store", url])


async def test_a_path_added_during_a_collection_stays(tmp_path: Path, hold: Path) -> None:
    """`AddToStore` (`gc-non-blocking.sh:30`)."""
    async with _serve(tmp_path) as (url, _server):
        before = await _add(url, tmp_path, "before")
        async with anyio.create_task_group() as tg:
            tg.start_soon(_collect, url)
            async with _held(hold):
                during = await _add(url, tmp_path, "during")
        valid = await _valid(url)
    assert before not in valid, "the collection deletes a path no root keeps"
    assert during in valid


async def test_a_path_copied_during_a_collection_stays(tmp_path: Path, hold: Path) -> None:
    """`AddMultipleToStore`: `nix copy` into the store while it collects."""
    source = f"local?root={tmp_path / 'source'}"
    copied = await _add(source, tmp_path, "copied")
    async with _serve(tmp_path) as (url, _server):
        before = await _add(url, tmp_path, "before")
        async with anyio.create_task_group() as tg:
            tg.start_soon(_collect, url)
            async with _held(hold):
                await run_subproc([NIX_BIN, "copy", "--no-check-sigs", "--from", source, "--to", url, copied])
        valid = await _valid(url)
    assert before not in valid
    assert copied in valid


async def test_a_path_substituted_during_a_collection_stays(tmp_path: Path, hold: Path) -> None:
    """A substitution goal roots its path (`substitution-goal.cc`) before it fetches."""
    source, cache = f"local?root={tmp_path / 'source'}", tmp_path / "cache"
    fetched = await _add(source, tmp_path, "fetched")
    await run_subproc([NIX_BIN, "copy", "--no-check-sigs", "--from", source, "--to", f"file://{cache}", fetched])
    # The client names the cache: the guest's own configuration sends an empty `substituters`, which a
    # trusted client's override puts in place of the store's, as nix-daemon does.
    substituters = ("--option", "substituters", f"file://{cache}?trusted=1")
    async with _serve(tmp_path) as (url, _server):
        before = await _add(url, tmp_path, "before")
        assert fetched not in await _valid(url)
        async with anyio.create_task_group() as tg:
            tg.start_soon(_collect, url)
            async with _held(hold):
                await run_subproc([NIX_STORE, "--store", url, *substituters, "--realise", fetched])
        valid = await _valid(url)
    assert before not in valid
    assert fetched in valid


async def test_a_build_during_a_collection_keeps_its_inputs_and_outputs(tmp_path: Path, hold: Path) -> None:
    """A build that starts and ends while the collector waits keeps its derivation, its input and its output."""
    async with _serve(tmp_path) as (url, _server):
        before = await _add(url, tmp_path, "before")
        async with anyio.create_task_group() as tg:
            tg.start_soon(_collect, url)
            async with _held(hold):
                _rc, out, _err, _both = await run_subproc(
                    [NIX_BIN, "build", "--store", url, "--impure", "--no-link", "--print-out-paths", "--expr", TOP]
                )
        top = out.strip()
        _rc, closure, _err, _both = await run_subproc(
            [NIX_BIN, "path-info", "--store", url, "--recursive", "--derivation", "--impure", "--expr", TOP]
        )
        valid = await _valid(url)
    assert before not in valid
    assert top in valid
    assert set(closure.split()) <= valid, "the derivations and the input"


async def test_the_pass_keeps_a_planned_path_a_client_roots_mid_collection(tmp_path: Path, hold: Path) -> None:
    """pynixd's own pass plans before it collects: a temporary root that arrives in between keeps the path."""
    async with _serve(tmp_path, postgres=None, gc_max_age=MAX_AGE, gc_allow_execute=True) as (url, server):
        _freeze_tracker(server)
        rooted = await _add(url, tmp_path, "rooted")
        unrooted = await _add(url, tmp_path, "unrooted")
        await _backdate(tmp_path / "store", {rooted, unrooted}, 2 * MAX_AGE)
        async with anyio.create_task_group() as tg:
            tg.start_soon(partial(Collector(server.ctx).run, PynixdGCAction.EXECUTE))
            async with _held(hold):
                # `ensurePath` takes a temporary root before it asks for validity (`store-api.cc`).
                await run_subproc([NIX_STORE, "--store", url, "--realise", rooted])
        valid = await _valid(url)
    assert unrooted not in valid, "the pass deletes what it planned"
    assert rooted in valid


async def test_a_collection_cut_off_mid_walk_lets_writes_through(tmp_path: Path, hold: Path) -> None:
    """A client that leaves while its collection waits does not leave the store hung (nixidae/nixidae#1439)."""
    async with _serve(tmp_path) as (url, _server):
        async with anyio.create_task_group() as tg:
            tg.start_soon(_collect, url)
            async with _held(hold):
                tg.cancel_scope.cancel()
        with anyio.fail_after(30):
            after = await _add(url, tmp_path, "after")
            await _collect_kept(url, hold)
        valid = await _valid(url)
    assert after not in valid, "the next collection runs to the end"


async def _collect_kept(url: str, hold: Path) -> None:
    """One more collection, let through its synchronisation point at once."""
    async with anyio.create_task_group() as tg:
        tg.start_soon(_collect, url)
        async with _held(hold):
            pass
