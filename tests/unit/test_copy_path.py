"""`nar_fs.copy_path` makes what a NAR round trip makes, and `copy_file_data` falls back as coreutils' `cp` does.

coreutils 9.12, `copy.c` and `copy-file-data.c`, is the model: a clone, then `copy_file_range`
until it refuses before the first byte, then a read and a write.
"""

from __future__ import annotations

import errno
import fcntl
import os
import stat
from typing import TYPE_CHECKING

import pytest

from pynixd_store import nar_fs
from pynixd_store.nar_fs import archive_settings, copy_file_data, copy_path, dump_path, restore_path

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path


def _tree(at: Path) -> None:
    """Every kind of entry a NAR holds, with names and sizes that have tripped copies before."""
    at.mkdir()
    (at / "empty").write_bytes(b"")
    (at / "small").write_bytes(b"hello\n")
    (at / "large").write_bytes(os.urandom(3 * 1024 * 1024 + 17))
    (at / "tool").write_bytes(b"#!/bin/sh\n")
    (at / "tool").chmod(0o555)
    (at / "only-owner-x").write_bytes(b"x")
    (at / "only-owner-x").chmod(0o500)
    (at / "nested" / "deeper" / "deepest").mkdir(parents=True)
    (at / "nested" / "deeper" / "deepest" / "leaf").write_bytes(b"leaf")
    (at / "empty-dir").mkdir()
    (at / "rel-link").symlink_to("small")
    (at / "abs-link").symlink_to("/nix/store/00000000000000000000000000000000-elsewhere")
    (at / "dangling").symlink_to("does-not-exist")
    (at / "dir-link").symlink_to("nested")
    (at / "ünïcode name").write_bytes(b"u")
    (at / os.fsdecode(b"bytes-\xff-name")).write_bytes(b"b")


def _reader(pieces: Iterator[bytes]) -> Callable[[int], bytes]:
    buffer = bytearray()

    def read(n: int) -> bytes:
        while len(buffer) < n:
            piece = next(pieces, None)
            if piece is None:
                break
            buffer.extend(piece)
        data = bytes(buffer[:n])
        del buffer[:n]
        return data

    return read


def _round_trip(src: Path, dest: Path) -> None:
    restore_path(dest, _reader(iter(dump_path(src))))


def _modes(at: Path) -> dict[str, tuple[int, str]]:
    """Each entry's type, permission bits and symlink target, by relative path."""
    found: dict[str, tuple[int, str]] = {}
    for root, dirs, files in os.walk(at):
        for name in [*dirs, *files]:
            path = os.path.join(root, name)
            mode = os.lstat(path).st_mode
            target = os.readlink(path) if stat.S_ISLNK(mode) else ""
            found[os.path.relpath(path, at)] = (mode, target)
    return found


def test_a_copy_is_what_a_nar_round_trip_makes(tmp_path: Path) -> None:
    _tree(tmp_path / "src")
    copy_path(tmp_path / "src", tmp_path / "copied")
    _round_trip(tmp_path / "src", tmp_path / "restored")
    assert b"".join(dump_path(tmp_path / "copied")) == b"".join(dump_path(tmp_path / "src"))
    assert _modes(tmp_path / "copied") == _modes(tmp_path / "restored")


def test_with_the_case_hack_a_copy_still_matches_the_round_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The case hack renames entries on the way through a NAR, so the copy takes that way."""
    monkeypatch.setattr(archive_settings, "use_case_hack", True)
    _tree(tmp_path / "src")
    (tmp_path / "src" / "Small").write_bytes(b"upper")
    copy_path(tmp_path / "src", tmp_path / "copied")
    _round_trip(tmp_path / "src", tmp_path / "restored")
    assert sorted(os.listdir(tmp_path / "copied")) == sorted(os.listdir(tmp_path / "restored"))
    assert b"".join(dump_path(tmp_path / "copied")) == b"".join(dump_path(tmp_path / "restored"))


def test_a_fifo_is_refused(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    os.mkfifo(tmp_path / "src" / "pipe")
    with pytest.raises(ValueError, match="unsupported type"):
        copy_path(tmp_path / "src", tmp_path / "copied")


class Calls:
    """Which of the three ways `copy_file_data` took, with the kernel's answers replaced."""

    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        clone: OSError | None = None,
        copy_range: list[OSError | int | None] | None = None,
    ) -> None:
        self.ways: list[str] = []
        answers = iter(copy_range or [])
        real_range = os.copy_file_range
        real_read = os.read

        def ioctl(_fd: int, request: int, _arg: int) -> int:
            if request != nar_fs.FICLONE:
                raise AssertionError(f"ioctl {request:#x}")
            self.ways.append("clone")
            if clone is not None:
                raise clone
            raise OSError(errno.EOPNOTSUPP, "no clone in this test")

        def copy_file_range(src: int, dst: int, count: int) -> int:
            self.ways.append("copy_file_range")
            answer = next(answers, None)
            if isinstance(answer, OSError):
                raise answer
            if isinstance(answer, int):
                return answer
            return real_range(src, dst, count)

        def read(fd: int, n: int) -> bytes:
            if not self.ways or self.ways[-1] != "read":
                self.ways.append("read")
            return real_read(fd, n)

        monkeypatch.setattr(fcntl, "ioctl", ioctl)
        monkeypatch.setattr(os, "copy_file_range", copy_file_range)
        monkeypatch.setattr(os, "read", read)


def _copy(tmp_path: Path, data: bytes) -> bytes:
    src, dest = tmp_path / "in", tmp_path / "out"
    src.write_bytes(data)
    with src.open("rb") as source, dest.open("xb") as target:
        copy_file_data(source.fileno(), target.fileno())
    return dest.read_bytes()


DATA = os.urandom(200_000)


def test_a_refused_clone_falls_back_to_copy_file_range(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = Calls(monkeypatch)
    assert _copy(tmp_path, DATA) == DATA
    assert calls.ways[:2] == ["clone", "copy_file_range"]
    assert "read" not in calls.ways


@pytest.mark.parametrize(
    "refusal",
    [errno.EXDEV, errno.EOPNOTSUPP, errno.ENOSYS, errno.EPERM, errno.EINVAL, errno.ENOENT],
    ids=errno.errorcode.get,
)
def test_copy_file_range_refused_before_the_first_byte_falls_back_to_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, refusal: int
) -> None:
    """`is_CLONENOTSUP` (`copy-file-data.c:92`); `EPERM` is a seccomp filter, `ENOENT` CIFS (`:171`)."""
    calls = Calls(monkeypatch, copy_range=[OSError(refusal, os.strerror(refusal))])
    assert _copy(tmp_path, DATA) == DATA
    assert calls.ways == ["clone", "copy_file_range", "read"]


def test_copy_file_range_answering_nothing_at_first_falls_back_to_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """procfs answers 0 for a file with data (`copy-file-data.c:139`)."""
    calls = Calls(monkeypatch, copy_range=[0])
    assert _copy(tmp_path, DATA) == DATA
    assert calls.ways == ["clone", "copy_file_range", "read"]


def test_a_failure_after_the_first_byte_is_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A partial copy has bytes in the target; reading the rest from the start would double them."""
    # The first call copies every byte; the second, which would answer 0, refuses instead.
    Calls(monkeypatch, copy_range=[None, OSError(errno.EXDEV, "late")])
    with pytest.raises(OSError, match="late"):
        _copy(tmp_path, DATA)


def test_efbig_ends_the_kernel_copy_and_the_read_finishes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Linux 6.17 answers EFBIG at the source's end (`copy-file-data.c:152`)."""
    calls = Calls(monkeypatch, copy_range=[OSError(errno.EFBIG, "too big")])
    assert _copy(tmp_path, DATA) == DATA
    assert calls.ways == ["clone", "copy_file_range", "read"]


def test_a_clone_that_fails_with_eio_is_an_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """It may leave the target half made (bugs.gnu.org/60489), so no copy goes on top of it."""
    calls = Calls(monkeypatch, clone=OSError(errno.EIO, "clone broke"))
    with pytest.raises(OSError, match="clone broke"):
        _copy(tmp_path, DATA)
    assert calls.ways == ["clone"]
