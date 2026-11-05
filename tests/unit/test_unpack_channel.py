"""`builtin:unpack-channel` (`builtins/unpack-channel.cc`) on a host directory, through libarchive."""

from __future__ import annotations

import io
import tarfile
from typing import TYPE_CHECKING

import pytest

from pynixd_store.builder.unpack_channel import UnpackError, unpack_channel
from pynixd_store.hintfmt import magenta

if TYPE_CHECKING:
    from pathlib import Path


def _tar(path: Path, members: dict[str, bytes | None]) -> str:
    with tarfile.open(path, "w:gz") as tar:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            if data is None:
                info.type, info.mode = tarfile.DIRTYPE, 0o200
                tar.addfile(info)
            else:
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
    return str(path)


def test_the_one_top_entry_takes_the_channel_name(tmp_path: Path) -> None:
    src = _tar(tmp_path / "c.tar.gz", {"top/": None, "top/default.nix": b"1\n"})
    out = tmp_path / "out"

    unpack_channel(str(out), "nixpkgs", src)

    assert (out / "nixpkgs" / "default.nix").read_bytes() == b"1\n"
    assert [p.name for p in out.iterdir()] == ["nixpkgs"]


def test_two_top_entries_are_refused_in_nix_words(tmp_path: Path) -> None:
    src = _tar(tmp_path / "two.tar.gz", {"one": b"1", "two": b"2"})

    with pytest.raises(UnpackError) as e:
        unpack_channel(str(tmp_path / "out"), "two", src)

    assert str(e.value) == f"channel tarball '{magenta(src)}' contains more than one file"


def test_a_channel_name_with_a_separator_is_refused(tmp_path: Path) -> None:
    with pytest.raises(UnpackError) as e:
        unpack_channel(str(tmp_path / "out"), "a/b", str(tmp_path / "none"))

    assert str(e.value) == f"channelName is not allowed to contain filesystem separators, got {magenta('a/b')}"


def test_a_file_that_is_no_archive_is_libarchives_refusal(tmp_path: Path) -> None:
    src = tmp_path / "garbage"
    src.write_bytes(b"not an archive\n" * 64)

    with pytest.raises(UnpackError) as e:
        unpack_channel(str(tmp_path / "out"), "garbage", str(src))

    assert str(e.value).startswith("failed to open archive: "), e.value
