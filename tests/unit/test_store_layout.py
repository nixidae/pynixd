"""The two shapes of a local store, and what each one gives pynixd.

Nix moves a store two ways, and they are not the same way. A chroot store
puts the files under a root and moves no store path. A relocated store moves
the store path itself. pynixd served the first alone until issue nixidae/nixidae#215, and
four readers had the chroot layout written into them.

`pynixd_store/store_layout.py` states the difference, and these tests state it too.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from pynixd.config import LocalSocketStoreSpec
from pynixd_store.store_layout import DEFAULT_STATE_DIR, DEFAULT_STORE_DIR, StoreLayout

ROOT = Path("/srv/chroot")
STORE = Path("/srv/relocated/store")
STATE = Path("/srv/relocated/var/nix")
LOG = Path("/srv/relocated/var/log/nix")


# ── The chroot store ────────────────────────────────────────────────


def test_the_store_of_the_machine() -> None:
    layout = StoreLayout.chroot(Path("/"))
    assert layout.store_dir == DEFAULT_STORE_DIR
    assert layout.real_store_dir == DEFAULT_STORE_DIR
    assert layout.state_dir == DEFAULT_STATE_DIR
    assert not layout.relocated


def test_no_root_is_the_store_of_the_machine() -> None:
    assert StoreLayout.chroot(None) == StoreLayout.chroot(Path("/"))


def test_a_chroot_store_moves_the_files_and_not_the_paths() -> None:
    """`local-fs-store.hh:54-70` of Nix, measured against 2.34.8."""
    layout = StoreLayout.chroot(ROOT)
    assert layout.store_dir == DEFAULT_STORE_DIR
    assert layout.real_store_dir == ROOT / "nix" / "store"
    assert layout.state_dir == ROOT / "nix" / "var" / "nix"
    assert layout.log_dir == ROOT / "nix" / "var" / "log" / "nix"


def test_a_chroot_store_is_a_store_argument() -> None:
    assert StoreLayout.chroot(ROOT).daemon_arguments() == ["--store", str(ROOT)]
    assert StoreLayout.chroot(ROOT).daemon_environment() == {}


def test_the_root_comes_back_out_of_the_two_directories() -> None:
    assert StoreLayout.chroot(ROOT).chroot_root() == ROOT
    assert StoreLayout.chroot(Path("/")).chroot_root() == Path("/")


# ── The relocated store ─────────────────────────────────────────────


def test_a_relocated_store_moves_the_paths_themselves() -> None:
    layout = StoreLayout.relocated_store(STORE, STATE, LOG)
    assert layout.store_dir == STORE
    assert layout.real_store_dir == STORE
    assert layout.state_dir == STATE
    assert layout.log_dir == LOG
    assert layout.relocated


def test_a_relocated_store_logs_to_nix_s_fallback_with_no_name() -> None:
    """Nix's compiled `logDir` (`store-api.cc:1271`); the config never relies on it."""
    assert StoreLayout.relocated_store(STORE, STATE).log_dir == Path("/nix/var/log/nix")


def test_a_relocated_store_names_its_three_directories_and_no_argument() -> None:
    """`--store <dir>` would read the directory as a root and nest a store."""
    layout = StoreLayout.relocated_store(STORE, STATE, LOG)
    assert layout.daemon_arguments() == []
    assert layout.daemon_environment() == {
        "NIX_STORE_DIR": str(STORE),
        "NIX_STATE_DIR": str(STATE),
        "NIX_LOG_DIR": str(LOG),
    }


def test_a_relocated_store_has_no_chroot_root() -> None:
    with pytest.raises(ValueError, match="no chroot root"):
        StoreLayout.relocated_store(STORE, STATE, LOG).chroot_root()


# ── What the four readers ask ───────────────────────────────────────


def test_the_database_follows_the_state() -> None:
    assert StoreLayout.chroot(ROOT).db_path == ROOT / "nix" / "var" / "nix" / "db" / "db.sqlite"
    assert StoreLayout.relocated_store(STORE, STATE, LOG).db_path == STATE / "db" / "db.sqlite"


def test_the_build_directory_follows_the_state() -> None:
    assert StoreLayout.chroot(ROOT).build_dir == ROOT / "nix" / "var" / "nix" / "builds"
    assert StoreLayout.relocated_store(STORE, STATE, LOG).build_dir == STATE / "builds"


def test_the_links_directory_follows_the_real_store() -> None:
    """`.links` sits beside the store files, which a chroot store moves."""
    assert StoreLayout.chroot(ROOT).links_dir == ROOT / "nix" / "store" / ".links"
    assert StoreLayout.relocated_store(STORE, STATE, LOG).links_dir == STORE / ".links"


def test_the_socket_follows_the_state() -> None:
    """The path that pynixd used before it had a layout, for a chroot store."""
    chroot = StoreLayout.chroot(ROOT).socket_path("pynixd-nix")
    assert chroot == ROOT / "nix" / "var" / "nix" / "daemon-socket" / "pynixd-nix"
    relocated = StoreLayout.relocated_store(STORE, STATE, LOG).socket_path("pynixd-nix")
    assert relocated == STATE / "daemon-socket" / "pynixd-nix"


# ── The configuration ───────────────────────────────────────────────


def test_a_spec_with_no_store_dir_is_a_chroot_store() -> None:
    spec = LocalSocketStoreSpec(store_path=ROOT)
    assert spec.layout() == StoreLayout.chroot(ROOT)


def test_a_spec_with_the_three_names_is_a_relocated_store() -> None:
    spec = LocalSocketStoreSpec(store_dir=STORE, state_dir=STATE, log_dir=LOG)
    assert spec.layout() == StoreLayout.relocated_store(STORE, STATE, LOG)


def test_a_store_dir_with_no_state_dir_is_refused() -> None:
    """A default of `/nix/var/nix` would put the roots of a test store there."""
    with pytest.raises(ValidationError, match="store_dir needs state_dir"):
        LocalSocketStoreSpec(store_dir=STORE, log_dir=LOG)


def test_a_state_dir_with_no_store_dir_is_refused() -> None:
    with pytest.raises(ValidationError, match="state_dir needs store_dir"):
        LocalSocketStoreSpec(state_dir=STATE)


def test_a_relocated_store_with_no_log_dir_is_refused() -> None:
    """Nix reads `NIX_LOG_DIR` independent of the state, so the spec names it."""
    with pytest.raises(ValidationError, match="store_dir needs log_dir"):
        LocalSocketStoreSpec(store_dir=STORE, state_dir=STATE)


def test_a_log_dir_with_no_store_dir_is_refused() -> None:
    with pytest.raises(ValidationError, match="log_dir needs store_dir"):
        LocalSocketStoreSpec(log_dir=LOG)


def test_the_two_shapes_are_not_the_same_layout() -> None:
    """A root of `<x>` and a relocation to `<x>` name different directories."""
    same = Path("/srv/one")
    chroot = StoreLayout.chroot(same)
    relocated = StoreLayout.relocated_store(same, same / "var", same / "var" / "log" / "nix")
    assert chroot != relocated
    assert chroot.real_store_dir != relocated.real_store_dir
    assert chroot.store_dir != relocated.store_dir
