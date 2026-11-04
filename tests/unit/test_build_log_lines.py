"""`pynixd_store.builder.buildlog` cuts lines as `BuildLog::operator()` does (`build/build-log.cc:11`)."""

from __future__ import annotations

from pynixd_store.builder.buildlog import BuildLog


def _all(*pieces: bytes) -> list[str]:
    log = BuildLog()
    lines = [line for piece in pieces for line in log.feed(piece)]
    return lines + log.flush()


def test_lines_end_at_a_newline_and_the_last_one_at_flush() -> None:
    assert _all(b"one\ntw", b"o\nthree") == ["one", "two", "three"]


def test_a_carriage_return_overwrites_from_the_start() -> None:
    assert _all(b"50%\r100%\n") == ["100%"]


def test_a_shorter_overwrite_keeps_nothing_of_the_longer_line() -> None:
    """`flushLine` cuts the line at the column, so `\\rab` over `abcdef` is `ab`."""
    assert _all(b"abcdef\rab\n") == ["ab"]


def test_crlf_split_across_pieces_ends_the_line() -> None:
    assert _all(b"windows\r", b"\nnext\n") == ["windows", "next"]


def test_an_empty_line_is_a_line() -> None:
    assert _all(b"a\n\nb\n") == ["a", "", "b"]


def test_bytes_that_are_not_utf8_survive() -> None:
    assert _all(b"\xff\n")[0].encode(errors="surrogateescape") == b"\xff"
