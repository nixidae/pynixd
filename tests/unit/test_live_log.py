"""`LiveLog`: a sender reads whole messages from any boundary, in memory and after the spill to a file."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from pynixd_store import live_log
from pynixd_store.live_log import LiveLog

if TYPE_CHECKING:
    from pathlib import Path


def _messages(count: int, size: int) -> list[bytes]:
    return [bytes([index % 251]) * size for index in range(count)]


def _read_all(log: LiveLog) -> list[bytes]:
    pieces, offset = [], 0
    while offset < log.size:
        piece = log.piece(offset)
        pieces.append(piece)
        offset += len(piece)
    return pieces


def _boundaries(messages: list[bytes]) -> set[int]:
    ends, offset = set(), 0
    for message in messages:
        offset += len(message)
        ends.add(offset)
    return ends


def test_each_piece_ends_on_a_message_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(live_log, "PIECE_BYTES", 100)
    log = LiveLog()
    messages = _messages(50, 37)
    for message in messages:
        log.append(message)

    pieces = _read_all(log)

    assert len(pieces) > 1
    offset, ends = 0, _boundaries(messages)
    for piece in pieces:
        offset += len(piece)
        assert offset in ends
    assert b"".join(pieces) == b"".join(messages)


def test_a_large_log_moves_to_a_file_and_reads_the_same(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(live_log, "SPILL_BYTES", 1000)
    monkeypatch.setattr(live_log, "PIECE_BYTES", 300)
    log = LiveLog(tmp_path)
    messages = _messages(200, 64)
    for message in messages:
        log.append(message)

    assert log._map is not None  # noqa: SLF001 -- whether it spilled is the subject
    assert b"".join(_read_all(log)) == b"".join(messages)
    assert log.get_bytes() == b"".join(messages)
    # Unlinked at once: the file lives only while the log holds it.
    assert list(tmp_path.iterdir()) == []


def test_a_closed_log_takes_nothing_more() -> None:
    log = LiveLog()
    log.append(b"one")
    log.close()

    with pytest.raises(RuntimeError):
        log.append(b"two")
