"""`delete_path` (`deletePath`): the whole tree goes, whatever its modes, and nothing a link names."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from pynixd_store.add import delete_path

if TYPE_CHECKING:
    from pathlib import Path


def test_unwritable_and_unreadable_directories_go(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    for name, mode in (("read-only", 0o555), ("closed", 0o000)):
        directory = tree / name
        directory.mkdir(parents=True)
        (directory / "file").write_text("x")
        directory.chmod(mode)
    tree.chmod(0o555)
    delete_path(tree)
    assert not tree.exists()


def test_what_a_link_names_stays(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    outside.chmod(0o555)
    (tmp_path / "kept").write_text("x")
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "to-directory").symlink_to(outside)
    (tree / "to-file").symlink_to(tmp_path / "kept")
    (tree / "dangling").symlink_to(tmp_path / "absent")
    delete_path(tree)
    assert not tree.exists()
    assert outside.stat().st_mode & 0o777 == 0o555
    assert (tmp_path / "kept").read_text() == "x"


def test_a_deep_tree_goes(tmp_path: Path) -> None:
    tree = tmp_path / "tree"
    tree.mkdir()
    fds = [os.open(tree, os.O_RDONLY)]
    for _ in range(400):
        os.mkdir("d", dir_fd=fds[-1])
        fds.append(os.open("d", os.O_RDONLY, dir_fd=fds[-1]))
    for fd in fds:
        os.close(fd)
    delete_path(tree)
    assert not tree.exists()


def test_a_file_a_link_and_nothing(tmp_path: Path) -> None:
    (tmp_path / "file").write_text("x")
    (tmp_path / "link").symlink_to(tmp_path / "file")
    delete_path(tmp_path / "link")
    assert (tmp_path / "file").exists()
    delete_path(tmp_path / "file")
    delete_path(tmp_path / "file")
    assert list(tmp_path.iterdir()) == []
