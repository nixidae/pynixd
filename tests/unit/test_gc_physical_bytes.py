"""Block-byte accounting dedupes hardlinks and skips holes. Issue #68."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pynixd_store.gc import physical_batch_total


def _write(path: Path, size: int) -> None:
    path.write_bytes(b"x" * size)


@pytest.mark.anyio
async def test_sparse_file_counts_blocks_not_holes(tmp_path: Path) -> None:
    """An 8 MiB sparse file holds kilobytes, and the total says so."""
    sparse = tmp_path / "sparse.img"
    with sparse.open("wb") as handle:
        handle.seek(8 * 1024 * 1024)
        handle.write(b"\0")
    logical = sparse.stat().st_size
    assert logical == 8 * 1024 * 1024 + 1

    total = physical_batch_total([str(sparse)], tmp_path)

    assert total is not None
    assert total < logical
    assert total == sparse.stat().st_blocks * 512


@pytest.mark.anyio
async def test_hardlink_counts_once_across_top_paths(tmp_path: Path) -> None:
    """Two top paths sharing one inode pay its blocks a single time."""
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    _write(first / "data", 4096)
    os.link(first / "data", second / "data")

    total = physical_batch_total([str(first), str(second)], tmp_path)
    single = physical_batch_total([str(first)], tmp_path)

    assert total is not None
    assert single is not None
    # The second top path adds its own directory inode and nothing more. A
    # directory takes a block on ext4 (CI) and none on tmpfs (the guests).
    assert total == single + os.stat(second).st_blocks * 512


@pytest.mark.anyio
async def test_unmeasurable_answers_none(tmp_path: Path) -> None:
    """No layout, or a path that vanished: the caller falls back."""
    assert physical_batch_total([str(tmp_path / "x")], None) is None
    assert physical_batch_total([str(tmp_path / "gone")], tmp_path) is None


@pytest.mark.anyio
async def test_chroot_paths_remap_under_the_store_dir(tmp_path: Path) -> None:
    """`/nix/store/...` top paths read under a relocated `store_dir`."""
    store = tmp_path / "store"
    (store / "hash-name").mkdir(parents=True)
    _write(store / "hash-name" / "f", 4096)

    total = physical_batch_total(["/nix/store/hash-name"], store)

    assert total is not None
    assert total > 0
