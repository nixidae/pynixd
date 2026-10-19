"""The two store directories, and the difference between them.

Nix keeps `storeDir` and `realStoreDir` apart, and so does this package. A
chroot store makes them differ: `--store <root>` moves the files to
`<root>/nix/store` and leaves `builtins.storeDir` at `/nix/store`. Measured
against Nix 2.34.8.

pynixd read the wire value where it needed the file-system one, and the
`inputSrcs` of every derivation it sent lost the inputs it could not read.
Issue nixidae/nixidae#212.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.store_dir import (
    on_disk,
    reading_store_dir,
    real_store_dir,
    reset_store_dir,
    set_real_store_dir,
    set_store_dir,
    store_dir,
    store_prefix,
)


@pytest.fixture(autouse=True)
def clean_store_dir():
    reset_store_dir()
    yield
    reset_store_dir()


def test_the_default_is_the_store_of_nix():
    assert store_dir() == "/nix/store"
    assert store_prefix() == "/nix/store/"


def test_the_environment_gives_the_store_dir(monkeypatch):
    monkeypatch.setenv("NIX_STORE_DIR", "/relocated/nix/store")
    reset_store_dir()
    assert store_dir() == "/relocated/nix/store"


def test_a_separator_at_the_end_goes_away(monkeypatch):
    monkeypatch.setenv("NIX_STORE_DIR", "/relocated/nix/store/")
    reset_store_dir()
    assert store_dir() == "/relocated/nix/store"


def test_the_real_dir_follows_the_store_dir_until_it_is_set():
    set_store_dir("/relocated/nix/store")
    assert real_store_dir() == "/relocated/nix/store"


def test_a_chroot_store_keeps_the_two_apart():
    # `--store /chroot`: the files move, and the store path does not.
    set_real_store_dir("/chroot/nix/store")
    assert store_dir() == "/nix/store"
    assert real_store_dir() == "/chroot/nix/store"
    assert on_disk("/nix/store/abc-foo") == "/chroot/nix/store/abc-foo"


def test_a_relocated_store_puts_the_two_together():
    # `NIX_STORE_DIR=/r/nix/store`: the store path moves with the files.
    set_store_dir("/r/nix/store")
    set_real_store_dir("/r/nix/store")
    assert on_disk("/r/nix/store/abc-foo") == "/r/nix/store/abc-foo"


def test_on_disk_refuses_a_path_of_another_store():
    with pytest.raises(ValueError, match="not a path of the store"):
        on_disk("/somewhere/else/abc-foo")


def test_a_relative_directory_is_refused():
    with pytest.raises(ValueError, match="must be an absolute path"):
        set_store_dir("nix/store")
    with pytest.raises(ValueError, match="must be an absolute path"):
        set_real_store_dir("nix/store")


def test_the_prefix_follows_the_setter():
    """The cached prefix must not outlive the directory it was built from."""
    assert store_prefix() == "/nix/store/"
    set_store_dir("/relocated/nix/store")
    assert store_prefix() == "/relocated/nix/store/"


def test_the_prefix_follows_the_scoped_reader():
    """`reading_store_dir` moves the directory for a block, and moves it back."""
    assert store_prefix() == "/nix/store/"
    with reading_store_dir("/recording/nix/store"):
        assert store_prefix() == "/recording/nix/store/"
    assert store_prefix() == "/nix/store/"
