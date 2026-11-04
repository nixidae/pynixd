"""`rewriteOutput` and the non-waiting lock that a fixed output with the wrong hash takes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pynixd_store.add import PathLock
from pynixd_store.builder.outputs import _Reader, _rewritten

if TYPE_CHECKING:
    from pathlib import Path

OLD = b"a" * 32
NEW = b"b" * 32


def test_a_match_across_two_pieces_is_rewritten() -> None:
    data = b"x" * 5 + OLD + b"y" + OLD
    for cut in range(len(data) + 1):
        assert b"".join(_rewritten([data[:cut], data[cut:]], {OLD: NEW})) == b"x" * 5 + NEW + b"y" + NEW, cut


def test_every_hash_of_the_map_is_rewritten_in_one_pass() -> None:
    """`outputRewrites` moves several outputs at once; a new hash is never read again as an old one."""
    other, final = b"c" * 32, b"d" * 32
    data = OLD + b"/" + other + NEW
    for cut in range(len(data) + 1):
        assert b"".join(_rewritten([data[:cut], data[cut:]], {OLD: NEW, other: final, NEW: OLD})) == (
            NEW + b"/" + final + OLD
        ), cut


def test_the_reader_serves_short_reads_then_end_of_file() -> None:
    read = _Reader([b"abc", b"", b"de"])
    assert [read(2), read(2), read(2), read(2)] == [b"ab", b"c", b"de", b""]


def test_a_held_lock_is_not_waited_for(tmp_path: Path) -> None:
    held = PathLock(tmp_path / "p")
    held.acquire()
    other = PathLock(tmp_path / "p")
    assert not other.try_acquire()
    held.release(delete=True)
    assert other.try_acquire()
    other.release(delete=True)
