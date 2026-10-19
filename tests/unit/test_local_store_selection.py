"""The local store honours `use_db`, whichever way it is built.

`PynixdSettings.to_stores` hardcoded `LocalStore` for the implicit local
store, while `Server.__init__` called `spec.to_store()` and honoured the
option. So the SQLite fast paths were on for a programmatic server and for
every test, and off for every deployment of `pynixd daemon` -- the one
configuration nobody could change, because `use_db` was never read there.

See issue nixidae/nixidae#202.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec, PynixdSettings
from pynixd.local_store_db import LocalStoreDB, resolve_db_path
from pynixd.store.local_daemon import LocalStore
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout


def test_the_implicit_local_store_uses_the_database() -> None:
    """The default. `use_db` defaults to true, so the daemon gets a fast path."""
    stores = PynixdSettings(unix_path=None).to_stores()
    local = stores[StoreId("local")]
    assert isinstance(local, LocalDBStore), (
        f"the implicit local store is a {type(local).__name__}, so `pynixd daemon` "
        f"runs without the SQLite fast paths that `use_db` promises"
    )


def test_use_db_false_is_honoured() -> None:
    """The option means something in both directions, or it means nothing."""
    settings = PynixdSettings(
        unix_path=None,
        stores={"local": LocalSocketStoreSpec(use_db=False)},
    )
    local = settings.to_stores()[StoreId("local")]
    assert type(local) is LocalStore


def test_a_configured_local_store_still_uses_the_database() -> None:
    """The path that already worked keeps working."""
    settings = PynixdSettings(
        unix_path=None,
        stores={"local": LocalSocketStoreSpec()},
    )
    assert isinstance(settings.to_stores()[StoreId("local")], LocalDBStore)


def test_both_construction_paths_agree() -> None:
    """The defect was a disagreement, so this is the property to hold.

    `Server.__init__` builds the implicit local store from the same spec.
    Comparing the classes states that neither may drift again.
    """
    from_settings = PynixdSettings(unix_path=None).to_stores()[StoreId("local")]
    spec = LocalSocketStoreSpec(store_id=StoreId("local"), monitor=False)
    from_server = spec.to_store(str(StoreId("local")))
    assert type(from_settings) is type(from_server)


class TestOverlayRefusal:
    """A `local-overlay-store` must never use the SQLite fast paths.

    They read one database. An overlay store keeps its lower store's paths in
    another one, and `LocalOverlayStore::isValidPathUncached` asks `LocalStore`
    first, then `lowerStore`, and only then copies the info up. Reading the
    upper database alone calls a valid path invalid.

    This is a wrong answer and not a slow one, so it is refused rather than
    left to the operator.
    """

    def _store(self, extra_args: list[str]) -> LocalDBStore:
        return LocalDBStore(
            LocalSocketStoreSpec(
                store_id=StoreId("local"),
                store_path=Path("/"),
                extra_args=extra_args,
            )
        )

    def test_an_overlay_store_is_refused(self) -> None:
        store = self._store(["--store", "local-overlay://?lower-store=/nix/lower"])
        refusal = store._refuses_a_database()
        assert refusal is not None
        assert "local-overlay" in refusal

    def test_a_plain_store_is_not_refused(self) -> None:
        assert self._store([])._refuses_a_database() is None

    def test_an_unrelated_argument_is_not_refused(self) -> None:
        assert self._store(["--option", "cores", "4"])._refuses_a_database() is None


@pytest.mark.anyio
async def test_a_store_root_pynixd_cannot_write_yields_an_inactive_instance() -> None:
    """What makes "on by default" safe, and what this test found broken.

    `open` must never raise. A store whose database pynixd cannot read gets
    an inactive instance, every fast path of `LocalDBStore` returns `None`
    for that, and `DaemonStore.execute` falls through to the wire.

    `resolve_db_path` used to `mkdir(parents=True)` the database directory and
    let the `OSError` out, so a store root on a read-only file system -- or
    owned by another user -- raised out of `open` and took the daemon's
    startup with it. That is exactly the case `use_db` defaulting to true has
    to survive.
    """
    unwritable = Path("/nonexistent-store-root-for-this-test")
    assert not unwritable.exists(), "this test needs a root that is not there"

    assert resolve_db_path(StoreLayout.chroot(unwritable)) is None
    async with await LocalStoreDB.open(StoreLayout.chroot(unwritable)) as db:
        assert not db.active
        assert db.db_path is None


@pytest.mark.anyio
async def test_a_writable_store_root_is_resolved(tmp_path: Path) -> None:
    """The ordinary case still resolves, and still creates the directory.

    A managed daemon writes its database there, so the directory has to exist.
    The correction above is about what happens when it cannot be made.
    """
    db_path = resolve_db_path(StoreLayout.chroot(tmp_path))
    assert db_path is not None
    assert db_path == tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    assert db_path.parent.is_dir()


def test_the_inactive_constructor_answers_nothing() -> None:
    db = LocalStoreDB.inactive(StoreLayout.chroot(None))
    assert not db.active
    assert db.read_only
