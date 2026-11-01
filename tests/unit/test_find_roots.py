"""pynixd's answer to `FindRoots`: every root and each link that names it.

`LocalStore::findRoots(censor=false)` at `gc.cc:319` walks `gcroots` and
`profiles` (`gc.cc:231`), the processes (`local-gc.cc:61`) and the temporary
roots (`gc.cc:177`). A root in the first three counts only when its store path
is valid; a temporary root needs no check. Each link is the file that names
the root, and a temporary root is `{temp:<file name>}`. The walk also unlinks
a stale link under `gcroots/auto`, as Nix does there (`gc.cc:268`).
`tests/functional/test_find_roots.py` holds the answer against the daemon's.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.find_roots import FindRootsRequest
from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec
from pynixd.liveness import unlink_stale_auto_roots, walk_roots
from pynixd.local_store_db import LocalStoreDB
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout
from tests.unit.temp_root_owner import live_temp_root

if TYPE_CHECKING:
    from pathlib import Path

A = "00000000000000000000000000000001-a"
B = "00000000000000000000000000000002-b"
C = "00000000000000000000000000000003-c"
D = "00000000000000000000000000000004-d"


def _state(root: Path, store_dir: str) -> tuple[Path, Path]:
    """Every root shape, under `root/state` and a fake `root/proc`."""
    state = root / "state"
    auto = state / "gcroots" / "auto"
    auto.mkdir(parents=True)
    (state / "profiles").mkdir()
    (state / "temproots").mkdir()
    (auto / "direct").symlink_to(f"{store_dir}/{A}/bin/hello")
    (auto / "indirect").symlink_to(root / "result")
    (root / "result").symlink_to(f"{store_dir}/{B}")
    (state / "gcroots" / C).write_text("")  # noqa: ASYNC240 -- test setup
    (state / "profiles" / "default").symlink_to(f"{store_dir}/{A}")

    proc = root / "proc"
    (proc / "42" / "fd").mkdir(parents=True)
    (proc / "42" / "exe").symlink_to(f"{store_dir}/{D}/bin/prog")
    (proc / "42" / "fd" / "3").symlink_to(f"{store_dir}/{A}")
    (proc / "42" / "maps").write_text(f"7f00-7f01 r--p 00000000 00:01 2 {store_dir}/{B}/lib/libb.so\n")  # noqa: ASYNC240 -- test setup
    (proc / "42" / "environ").write_bytes(f"X={store_dir}/{C}\x00".encode())  # noqa: ASYNC240 -- test setup
    return state, proc


class TestWalkRoots:
    def test_each_root_has_the_link_that_names_it(self, tmp_path: Path) -> None:
        store_dir = str(tmp_path / "store")
        state, proc = _state(tmp_path, store_dir)
        checked, temporary = walk_roots(state, store_dir, proc)
        assert checked == {
            f"{store_dir}/{A}": {
                str(state / "gcroots" / "auto" / "direct"),
                str(state / "profiles" / "default"),
                str(proc / "42" / "fd" / "3"),
            },
            f"{store_dir}/{B}": {str(tmp_path / "result"), str(proc / "42" / "maps")},
            f"{store_dir}/{C}": {str(state / "gcroots" / C), str(proc / "42" / "environ")},
            f"{store_dir}/{D}": {str(proc / "42" / "exe")},
        }
        assert temporary == {}

    def test_a_temporary_root_is_labelled_by_its_file(self, tmp_path: Path) -> None:
        """Any name but a dotfile counts, and a path still being written does not (`gc.cc:169`)."""
        store_dir = str(tmp_path / "store")
        state, proc = _state(tmp_path, store_dir)
        content = f"{store_dir}/{A}\x00{store_dir}/{B}\x00{store_dir}/{C}".encode()
        with live_temp_root(state, "4242", content), live_temp_root(state, "foreign", f"{store_dir}/{D}\x00".encode()):
            _checked, temporary = walk_roots(state, store_dir, proc)
        assert temporary == {
            f"{store_dir}/{A}": {"{temp:4242}"},
            f"{store_dir}/{B}": {"{temp:4242}"},
            f"{store_dir}/{D}": {"{temp:foreign}"},
        }

    def test_a_dead_owners_file_seeds_nothing(self, tmp_path: Path) -> None:
        """Nix reaps the file of a process it can lock; a foreign name pynixd leaves in place."""
        store_dir = str(tmp_path / "store")
        state, proc = _state(tmp_path, store_dir)
        (state / "temproots" / "99").write_bytes(f"{store_dir}/{A}\x00".encode())  # noqa: ASYNC240 -- test setup
        (state / "temproots" / "foreign").write_bytes(f"{store_dir}/{A}\x00".encode())  # noqa: ASYNC240 -- test setup
        _checked, temporary = walk_roots(state, store_dir, proc)
        assert temporary == {}
        assert not (state / "temproots" / "99").exists()
        assert (state / "temproots" / "foreign").exists()


class TestStaleAutoRoots:
    def test_only_a_link_to_nothing_under_auto_goes(self, tmp_path: Path) -> None:
        store_dir = str(tmp_path / "store")
        state, _proc = _state(tmp_path, store_dir)
        auto = state / "gcroots" / "auto"
        (auto / "stale").symlink_to(tmp_path / "gone")
        (auto / "into-store").symlink_to(f"{store_dir}/{D}")
        (tmp_path / "dangling-intermediate").symlink_to(tmp_path / "also-gone")
        (auto / "through-a-dangling-link").symlink_to(tmp_path / "dangling-intermediate")
        (state / "gcroots" / "outside-auto").symlink_to(tmp_path / "gone")

        unlink_stale_auto_roots(state, store_dir)

        assert sorted(path.name for path in auto.iterdir()) == [
            "direct",
            "indirect",
            "into-store",
            "through-a-dangling-link",
        ]
        assert (state / "gcroots" / "outside-auto").is_symlink()


def _store_db(root: Path, valid: list[str]) -> None:
    db_path = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        for path in valid:
            conn.execute("INSERT INTO ValidPaths (path, registrationTime) VALUES (?, 0)", (path,))


@pytest.mark.anyio
class TestTheAnswer:
    async def test_a_root_counts_when_its_path_is_valid_and_a_temporary_root_always(self, tmp_path: Path) -> None:
        """The process walk reads the real `/proc`, whose paths this database does not hold."""
        _store_db(tmp_path, [f"/nix/store/{A}"])
        state = tmp_path / "nix" / "var" / "nix"
        (state / "gcroots").mkdir(parents=True)
        (state / "temproots").mkdir()
        (state / "gcroots" / "valid").symlink_to(f"/nix/store/{A}")
        (state / "gcroots" / "invalid").symlink_to(f"/nix/store/{B}")
        store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=tmp_path, managed=True))
        with live_temp_root(state, "4242", f"/nix/store/{C}\x00".encode()):
            async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as store.db:
                answer = await store.own_find_roots(FindRootsRequest())
        assert {(entry.link, entry.target) for entry in answer.roots} == {
            (str(state / "gcroots" / "valid"), f"/nix/store/{A}"),
            ("{temp:4242}", f"/nix/store/{C}"),
        }

    async def test_a_daemon_that_is_not_pynixd_s_answers_for_a_user(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The system daemon runs as root and sees processes an unprivileged pynixd cannot."""
        monkeypatch.setattr("os.geteuid", lambda: 1000)
        store = LocalDBStore(LocalSocketStoreSpec(store_id=StoreId("local"), store_path=tmp_path, managed=False))
        assert await store.own_find_roots(FindRootsRequest()) is None
