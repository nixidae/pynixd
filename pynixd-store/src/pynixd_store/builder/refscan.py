"""`scanForReferences` (`path-references.cc:48`): which candidate store paths a path's NAR names."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from ..nar_fs import dump_path
from ..utils import NIX32_CHARS

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

HASH_LEN = 32
"""`StorePath::HashLen`."""

_RUN = re.compile(rb"[" + re.escape(NIX32_CHARS.encode()) + rb"]{%d,}" % HASH_LEN)


class RefScanner:
    """`RefScanSink` (`references.cc:41`): every window of `HASH_LEN` nix32 characters is a candidate.

    Nix tests each offset; only a run of nix32 characters can hold a hash, so this
    finds the runs first. A hash split across two pieces is found through the tail
    each piece keeps.
    """

    def __init__(self, hashes: Iterable[str]) -> None:
        self._wanted = {h.encode() for h in hashes}
        self.seen: set[str] = set()
        self._tail = b""

    def feed(self, data: bytes) -> None:
        if not self._wanted:
            return
        buf = self._tail + data
        for run in _RUN.finditer(buf):
            text = run.group()
            for i in range(len(text) - HASH_LEN + 1):
                window = text[i : i + HASH_LEN]
                if window in self._wanted:
                    self._wanted.discard(window)
                    self.seen.add(window.decode())
        self._tail = buf[-(HASH_LEN - 1) :]


def scan_for_references(path: Path, candidates: Iterable[str]) -> set[str]:
    """The store paths of *candidates* whose hash part occurs in the NAR of *path*.

    Blocking: call it from a worker thread.
    """
    by_hash = {_hash_part(c): c for c in candidates}
    scanner = RefScanner(by_hash)
    for piece in dump_path(path):
        scanner.feed(piece)
    return {by_hash[h] for h in scanner.seen}


def _hash_part(store_path: str) -> str:
    return store_path.rsplit("/", 1)[-1][:HASH_LEN]
