"""`pynixd_store.builder.refscan` finds what `RefScanSink` finds (`references.cc:19`)."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from pynixd_store.builder.refscan import RefScanner, scan_for_references

if TYPE_CHECKING:
    from pathlib import Path

A = "/nix/store/0c2zl7vgd9y8nzxq1dz0f5jmcg0a7a9w-a"
B = "/nix/store/1ffzx4n7wq7yl3kyb0bvj3r4gfxc0nl3-b"
C = "/nix/store/5b0zd9jg7sbl4g9fxnrvv8qmwyh5ph0m-c"


def _hash(path: str) -> str:
    return path.split("/")[-1][:32]


def test_a_hash_split_across_pieces_is_found() -> None:
    scanner = RefScanner([_hash(A)])
    for byte in f"prefix {A}/bin/sh suffix".encode():
        scanner.feed(bytes([byte]))
    assert scanner.seen == {_hash(A)}


def test_a_hash_inside_a_longer_run_is_found() -> None:
    """Nix tests every offset, so nix32 characters on both sides do not hide a hash."""
    scanner = RefScanner([_hash(B)])
    scanner.feed(f"zz{_hash(B)}zz".encode())
    assert scanner.seen == {_hash(B)}


def test_binary_noise_finds_nothing() -> None:
    scanner = RefScanner([_hash(A), _hash(B)])
    scanner.feed(os.urandom(1 << 16).replace(b"\0", b"\1"))
    scanner.feed(_hash(A)[:31].encode())
    assert scanner.seen == set()


def test_the_nar_names_file_contents_symlink_targets_and_names(tmp_path: Path) -> None:
    out = tmp_path / "out"
    (out / "bin").mkdir(parents=True)
    (out / "bin" / "run").write_text(f"#!{A}/bin/sh\n")
    (out / "lib").symlink_to(f"{B}/lib")
    (out / _hash(C)).write_text("")
    other = "/nix/store/9w0b4dw3hglzm1yv7m1q5w0lvlyk5shm-unused"
    assert scan_for_references(out, [A, B, C, other]) == {A, B, C}
