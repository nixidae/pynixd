"""`use-case-hack`: names that differ in case alone, on a file system that cannot tell them apart.

Nix's default on darwin, whose /nix is case-insensitive unless the volume
was made otherwise. `parse` (`archive.cc:256-270`) gives a name that equals
an earlier one under `strcasecmp` the suffix `~nix~case~hack~N`, and
`dumpPath` (`archive.cc:86-101`) removes it. Each case runs on pynixd's
`nar_fs` and on `nix-store --restore` and `--dump` with the same setting:
the Nix in pynixd's process applies its own copy of the setting, so the two
must write and read the same names.
"""

from __future__ import annotations

import io
import os
import subprocess
from typing import TYPE_CHECKING

import pytest

from pynixd.nar import NarDirectory, NarDirectoryEntry, NarRegular, write_nar
from pynixd_store import nar_fs
from pynixd_store.nar_fs import BadArchive, dump_path, restore_path
from tests._conftest.config import NIX_BIN

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

NIX_STORE = NIX_BIN.parent / "nix-store"
HACK = "~nix~case~hack~"


def _nar(*names: str) -> bytes:
    """A directory of one-byte files, in the byte order a NAR needs."""
    return write_nar(NarDirectory(entries=[NarDirectoryEntry(name, NarRegular(name.encode())) for name in names]))


def _nix_store(case_hack: bool, *args: str, stdin: bytes = b"") -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [str(NIX_STORE), "--option", "use-case-hack", "true" if case_hack else "false", *args],
        input=stdin,
        capture_output=True,
        check=False,
    )


def _case_sensitive(path: Path) -> bool:
    (path / "probe").write_bytes(b"")
    try:
        return not (path / "PROBE").exists()
    finally:
        (path / "probe").unlink()


@pytest.fixture(params=[True, False], ids=["hack", "no-hack"])
def case_hack(request: pytest.FixtureRequest) -> Iterator[bool]:
    before = nar_fs.archive_settings.use_case_hack
    nar_fs.archive_settings.use_case_hack = request.param
    try:
        yield request.param
    finally:
        nar_fs.archive_settings.use_case_hack = before


def test_names_that_differ_in_case_restore_and_dump_as_nix_does(tmp_path: Path, case_hack: bool) -> None:
    if not case_hack and not _case_sensitive(tmp_path):
        pytest.skip("without the hack, a case-insensitive file system cannot hold both names")
    nar = _nar("A", "B", "a", "b", "c")
    restore_path(tmp_path / "pynixd", io.BytesIO(nar).read)
    nix = _nix_store(case_hack, "--restore", str(tmp_path / "nix"), stdin=nar)
    assert nix.returncode == 0, nix.stderr

    on_disk = sorted(os.listdir(tmp_path / "pynixd"))
    assert on_disk == sorted(os.listdir(tmp_path / "nix"))
    assert on_disk == (["A", "B", f"a{HACK}1", f"b{HACK}1", "c"] if case_hack else ["A", "B", "a", "b", "c"])
    # Each file keeps its own content under the hacked name.
    assert (tmp_path / "pynixd" / on_disk[2]).read_bytes() == b"a"

    assert b"".join(dump_path(tmp_path / "pynixd")) == nar
    assert _nix_store(case_hack, "--dump", str(tmp_path / "pynixd")).stdout == nar


def test_each_further_collision_counts_up(tmp_path: Path, case_hack: bool) -> None:
    if not case_hack:
        pytest.skip("the count is the hack's")
    nar = _nar("AB", "Ab", "aB", "ab")
    restore_path(tmp_path / "t", io.BytesIO(nar).read)
    assert sorted(os.listdir(tmp_path / "t")) == ["AB", f"Ab{HACK}1", f"aB{HACK}2", f"ab{HACK}3"]
    assert b"".join(dump_path(tmp_path / "t")) == nar


def test_a_name_that_is_a_hacked_name_is_refused_as_nix_refuses_it(tmp_path: Path, case_hack: bool) -> None:
    if not case_hack:
        pytest.skip("only the hack makes the name collide")
    nar = _nar("A", f"A{HACK}1", "a")
    nix = _nix_store(True, "--restore", str(tmp_path / "nix"), stdin=nar)
    message = f"NAR contains file name 'a' that collides with case-hacked file name 'A{HACK}1'"
    assert nix.returncode != 0
    assert message in nix.stderr.decode()
    with pytest.raises(BadArchive, match=message):
        restore_path(tmp_path / "pynixd", io.BytesIO(nar).read)


def test_a_dump_refuses_two_names_the_hack_makes_one(tmp_path: Path, case_hack: bool) -> None:
    (tmp_path / "t").mkdir()
    (tmp_path / "t" / "a").write_bytes(b"1")
    (tmp_path / "t" / f"a{HACK}1").write_bytes(b"2")
    if not case_hack:
        assert sorted(entry for entry in os.listdir(tmp_path / "t")) == ["a", f"a{HACK}1"]
        assert b"".join(dump_path(tmp_path / "t")) == _nix_store(False, "--dump", str(tmp_path / "t")).stdout
        return
    nix = _nix_store(True, "--dump", str(tmp_path / "t"))
    assert nix.returncode != 0
    assert "file name collision between" in nix.stderr.decode()
    with pytest.raises(ValueError, match="file name collision between"):
        b"".join(dump_path(tmp_path / "t"))
