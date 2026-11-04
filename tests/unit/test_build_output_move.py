"""`pynixd_store.builder.outputs._move`: a canonicalised directory output reaches the store unprivileged."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from pynixd_store.add import canonicalise
from pynixd_store.builder.outputs import _move

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.skipif(os.geteuid() == 0, reason="root renames a 0555 directory without the write bit")
def test_a_canonicalised_directory_moves_to_another_parent(tmp_path: Path) -> None:
    """The rename rewrites the directory's `..`, which needs write permission on it."""
    chroot, store = tmp_path / "chroot", tmp_path / "store"
    (chroot / "out" / "sub").mkdir(parents=True)
    (chroot / "out" / "sub" / "file").write_text("file\n")
    store.mkdir()
    canonicalise(chroot / "out")
    assert oct((chroot / "out").stat().st_mode & 0o777) == "0o555"

    _move(chroot / "out", store / "out")
    canonicalise(store / "out")

    assert (store / "out" / "sub" / "file").read_text() == "file\n"
    assert oct((store / "out").stat().st_mode & 0o777) == "0o555"
