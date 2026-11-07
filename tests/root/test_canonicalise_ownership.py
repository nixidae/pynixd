"""`canonicalisePathMetaData` with a build's uid range (`posix-fs-canonicalise.cc:91`). Root only: it chowns."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from pynixd_store.add import InvalidOwnership, canonicalise

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="chown needs root")

BUILD_UID = 30001


def _output(tmp_path: Path) -> Path:
    out = tmp_path / "out"
    out.mkdir()
    (out / "made").write_text("by the build\n")
    for path in (out, out / "made"):
        os.chown(path, BUILD_UID, 30000)
    return out


def test_a_file_the_build_user_does_not_own_is_refused(tmp_path: Path) -> None:
    """`ln /etc/shadow $out/foo` must not make root's file the store's."""
    out = _output(tmp_path)
    (out / "foreign").write_text("root's\n")

    with pytest.raises(InvalidOwnership):
        canonicalise(out, uid_range=(BUILD_UID, BUILD_UID))


def test_a_hard_link_to_a_file_already_chowned_passes(tmp_path: Path) -> None:
    """`touch $out/foo; ln $out/foo $out/bar`: the second name finds the inode root owns already."""
    out = _output(tmp_path)
    os.link(out / "made", out / "zzz-link")

    canonicalise(out, uid_range=(BUILD_UID, BUILD_UID))

    assert (out / "zzz-link").stat().st_uid == 0
    assert (out / "made").stat().st_ino == (out / "zzz-link").stat().st_ino
