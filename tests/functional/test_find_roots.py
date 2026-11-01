"""pynixd's FindRoots answer is the daemon's, on a store that holds only the test's paths.

Each root shape of `findRoots` (`gc.cc:231`): a direct link, an indirect link
through a link outside the state directory, a plain file, a profile, and a
link to a path the store does not hold. The host's processes name host paths,
which this store does not hold, so the process walk adds nothing on either
side and the two answers must be equal.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from nix_daemon_protocol.find_roots import FindRootsRequest
from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store import LocalDBStore, Store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from nix_daemon_protocol.find_roots import FindRootsResponse


@asynccontextmanager
async def _pynixd(store_path: Path) -> AsyncIterator[Server]:
    spec = make_test_spec(store_id="local", store_path=store_path, no_probe=True)
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(stores=stores, ssh_port=None, http_port=None) as server:
        yield server


async def _add(store_path: Path, work: Path, name: str) -> str:
    source = work / name
    source.write_text(f"{name}\n")  # noqa: ASYNC240 -- test setup
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "store", "add-path", "--store", str(store_path), str(source)],
        verbose=False,
    )
    return stdout.strip()


def _entries(response: FindRootsResponse) -> set[tuple[str, str]]:
    return {(entry.link, entry.target) for entry in response.roots}


async def test_pynixd_answers_what_the_daemon_answers(tmp_path: Path) -> None:
    store_path = tmp_path / "store"
    store_path.mkdir()
    direct, indirect, plain, profile = [await _add(store_path, tmp_path, name) for name in ("d", "i", "p", "f")]

    state_dir = store_path / "nix" / "var" / "nix"
    auto = state_dir / "gcroots" / "auto"
    auto.mkdir(parents=True)
    (state_dir / "profiles").mkdir(exist_ok=True)
    (auto / "direct").symlink_to(f"{direct}/sub")
    (tmp_path / "result").symlink_to(indirect)
    (auto / "indirect").symlink_to(tmp_path / "result")
    (state_dir / "gcroots" / plain.removeprefix("/nix/store/")).write_text("")  # noqa: ASYNC240 -- test setup
    (state_dir / "profiles" / "mine").symlink_to(profile)
    (state_dir / "gcroots" / "not-held").symlink_to("/nix/store/00000000000000000000000000000000-gone")

    async with _pynixd(store_path) as server:
        local = server.ctx.local_store
        assert isinstance(local, LocalDBStore)
        ours = await local.own_find_roots(FindRootsRequest())
        theirs = await local.call(FindRootsRequest())

    assert ours is not None
    assert _entries(ours) == _entries(theirs)
    assert {(str(auto / "direct"), direct), (str(tmp_path / "result"), indirect)} <= _entries(ours)


async def test_a_stale_auto_link_goes(tmp_path: Path) -> None:
    """Nix unlinks it during the walk (`gc.cc:268`); so does pynixd's answer, before the daemon sees it."""
    store_path = tmp_path / "store"
    store_path.mkdir()
    await _add(store_path, tmp_path, "x")
    auto = store_path / "nix" / "var" / "nix" / "gcroots" / "auto"
    auto.mkdir(parents=True)
    (auto / "stale").symlink_to(tmp_path / "gone")

    async with _pynixd(store_path) as server:
        local = server.ctx.local_store
        assert isinstance(local, LocalDBStore)
        await local.own_find_roots(FindRootsRequest())

    assert not (auto / "stale").is_symlink()
