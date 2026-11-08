"""The mirror agrees with Nix about what is alive, or nothing plans from it.

`RootsTracker.refresh` walks the lab store's roots and closes them in one
query; `RETURN_LIVE` asks Nix for the same answer. Any divergence either
way fails the test: the cutover gate is zero divergence sustained, and this
is where it is measured. The lab store keeps the `/nix/store` prefix, so a
test on a real closure would measure the host's mapped libraries instead;
these are three files of the test alone, and one gcroot.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from nix_daemon_protocol.collect_garbage import CollectGarbageRequest
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.protocol import GCAction
from pynixd import Server
from pynixd.store import LocalDBStore, Store
from pynixd_store.liveness import RootsTracker
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

_MAX_FREED = 2**63 - 1


@asynccontextmanager
async def _pynixd(store_path: Path) -> AsyncIterator[Server]:
    """A server over the lab store. Its opening migrates pynixd's tables in."""
    # Nix's daemon gives the reference answer.
    spec = make_test_spec(store_id="local", store_path=store_path, no_probe=True, daemon=True)
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(stores=stores, ssh_port=None, http_port=None) as server:
        yield server


async def _add(store_path: Path, work: Path, name: str, text: str) -> str:
    source = work / name
    source.write_text(text)  # noqa: ASYNC240 -- test setup
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "store", "add-path", "--store", str(store_path), str(source)],
        verbose=False,
    )
    return stdout.strip()


async def test_tracker_matches_nix(tmp_path: Path) -> None:
    """Same store, same roots, same live set -- from two implementations."""
    store_path = tmp_path / "store"
    store_path.mkdir()
    rooted = await _add(store_path, tmp_path, "rooted.txt", "a gcroot names this\n")
    await _add(store_path, tmp_path, "dead.txt", "nothing names this\n")

    state_dir = store_path / "nix" / "var" / "nix"
    (state_dir / "gcroots" / "auto").mkdir(parents=True)
    (state_dir / "gcroots" / "auto" / "test-root").symlink_to(rooted)

    async with _pynixd(store_path) as server:
        local = server.ctx.local_store
        assert isinstance(local, LocalDBStore)
        tracker = RootsTracker(
            state_dir=state_dir,
            store_dir="/nix/store",
            db_path=state_dir / "db" / "db.sqlite",
        )
        mine = tracker.refresh()
        resp = await local.call(
            CollectGarbageRequest(
                action=GCAction.RETURN_LIVE,
                paths_to_delete=set(),
                ignore_liveness=0,
                max_freed=_MAX_FREED,
                obsolete1=0,
                obsolete2=0,
                obsolete3=0,
            )
        )
        theirs = {str(path) for path in resp.paths_deleted}

    assert mine == theirs, f"only tracker: {sorted(mine - theirs)}, only nix: {sorted(theirs - mine)}"
