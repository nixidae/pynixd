"""pynixd's engine links an added path into `.links` as the daemon does, under `auto-optimise-store`.

`LocalStore::addToStore` and `addToStoreFromDump` call `optimisePath` after the
canonicalisation (`local-store.cc:1151`, `:1313`). pydd's suite builds in a store
whose `nix.conf` sets it, so every add went to the daemon until the engine did it.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, Any

from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

ADDS = ("AddToStore", "AddToStoreNar", "AddMultipleToStore")


def _fallthroughs() -> float:
    """Writes the engine route sent to the daemon after all (`write_router.REASONS`)."""
    return sum(
        REGISTRY.get_sample_value("pynixd_op_fallthrough_total", {"store_id": "local", "op": op}) or 0.0 for op in ADDS
    )


@contextlib.asynccontextmanager
async def _serve(base: Path, route: OpRoute) -> AsyncIterator[tuple[str, Path]]:
    root = base / route.name / "store"
    socket = base / f"{route.name}.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        op_routes=dict.fromkeys(ADDS, route),
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}", root


def _tree(base: Path, name: str) -> Path:
    tree = base / name
    tree.mkdir()
    (tree / "same").write_text("the same content in both trees\n")
    (tree / "own").write_text(f"only in {name}\n")
    return tree


async def _added(tmp_path: Path, route: OpRoute, *, optimise: bool) -> dict[str, Any]:
    base = tmp_path / f"{route.name}-{int(optimise)}"
    base.mkdir()
    flags = ["--option", "auto-optimise-store", "true" if optimise else "false"]
    # The NAR imports come from a plain local store, which no daemon serves.
    source = base / "source"
    _rc, imported, _err, _both = await run_subproc(
        [NIX_BIN, "store", "add", "--store", source, *flags, _tree(base, "nar")]
    )
    async with _serve(base, route) as (url, root):
        _rc, dumped, _err, _both = await run_subproc(
            [NIX_BIN, "store", "add", "--store", url, *flags, _tree(base, "dump")]
        )
        await run_subproc([NIX_BIN, "copy", "--no-check-sigs", "--from", source, "--to", url, *flags, imported.strip()])
    store = root / "nix/store"
    links = {entry.stat().st_ino for entry in (store / ".links").iterdir()} if (store / ".links").exists() else set()
    files = {}
    for path in (dumped.strip(), imported.strip()):
        for name in ("same", "own"):
            st = (store / path.rsplit("/", 1)[-1] / name).lstat()
            files[f"{path.rsplit('-', 1)[-1]}/{name}"] = (st.st_nlink, st.st_ino in links)
    return files


async def test_an_added_path_is_optimised_as_the_daemon_optimises_it(tmp_path: Path) -> None:
    """The session's override decides, both ways: `ClientSettings::apply` (`daemon.cc:233`)."""
    results = {}
    for optimise in (True, False):
        before = _fallthroughs()
        here = await _added(tmp_path, OpRoute.pynixd, optimise=optimise)
        assert _fallthroughs() == before, "the engine route sent an add to the daemon"
        there = await _added(tmp_path, OpRoute.daemon, optimise=optimise)
        assert here == there, optimise
        results[optimise] = here
    # The shared file: one inode for both paths and `.links`. Each other file: itself and its link.
    assert results[True] == {"dump/same": (3, True), "dump/own": (2, True), "nar/same": (3, True), "nar/own": (2, True)}
    assert results[False] == dict.fromkeys(("dump/same", "dump/own", "nar/same", "nar/own"), (1, False))
