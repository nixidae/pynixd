"""A GC pass on a real file system deletes what frees its target on disk, not in NAR bytes.

A tmpfs holds an optimised store. The oldest, biggest dead path shares its
file with a live one through `.links`, so its delete frees nothing; a small
unique path follows it. A pass sized by NAR bytes takes the shared path
alone, frees nothing and stops over target (dynhetz, 2026-11-27: 17.2 GB of
NAR from two sparse images freed 144 MB). A pass sized by the blocks its
deletes free takes exactly what reaches the target, and the ordinary dead
paths after it stay. Without the targeted `.links` cleanup no delete here
frees anything, so the test covers that too. The root suite runs it
because only root mounts a tmpfs.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.gc import Collector
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore, Store
from pynixd_store.gc import pressure_usage
from tests.conftest import NIX_BIN, make_test_spec, run_subproc
from tests.functional.test_gc_max_age import MAX_AGE, _backdate, _freeze_tracker, _valid

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="a tmpfs mount needs root")

UNIT = 8 * 1024 * 1024
DAY = 86_400


@dataclass(frozen=True)
class _Disk:
    volume: Path
    store_path: Path
    live: str
    live_bytes: bytes
    shared: str
    """Dead, 4 units, idle 30 days, the same bytes as `live`: one inode, so its delete frees nothing."""
    tiny: str
    """Dead, half a unit of its own, idle 100 days: second in the order."""
    unique: list[str]
    """Dead, one unit each of their own, idle 20, 16, 12, 8 and 4 days."""


async def _add(store_path: Path, work: Path, name: str, data: bytes) -> str:
    source = work / name
    source.write_bytes(data)  # noqa: ASYNC240 -- test setup
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "store", "add-path", "--store", str(store_path), str(source)], verbose=False
    )
    return stdout.strip()


@asynccontextmanager
async def _disk(tmp_path: Path) -> AsyncIterator[_Disk]:
    volume = tmp_path / "volume"
    volume.mkdir()
    await run_subproc(["mount", "-t", "tmpfs", "-o", "size=128M,mode=0755", "tmpfs", volume])
    try:
        store_path = volume / "store"
        store_path.mkdir()
        work = tmp_path / "work"
        work.mkdir()
        live_bytes = os.urandom(4 * UNIT)
        live = await _add(store_path, work, "live.bin", live_bytes)
        shared = await _add(store_path, work, "shared.bin", live_bytes)
        tiny = await _add(store_path, work, "tiny.bin", os.urandom(UNIT // 2))
        unique = [await _add(store_path, work, f"unique-{index}.bin", os.urandom(UNIT)) for index in range(5)]
        await run_subproc(
            [
                str(NIX_BIN.parent / "nix-store"),
                "--store",
                str(store_path),
                "--realise",
                live,
                "--add-root",
                str(tmp_path / "live-root"),
                "--indirect",
            ],
            verbose=False,
        )
        await run_subproc([str(NIX_BIN), "store", "optimise", "--store", str(store_path)], verbose=False)
        inodes = {(store_path / path.lstrip("/")).stat().st_ino for path in (live, shared)}  # noqa: ASYNC240
        assert len(inodes) == 1, "optimise left the shared file apart from the live one"
        yield _Disk(volume, store_path, live, live_bytes, shared, tiny, unique)
    finally:
        await run_subproc(["umount", volume])


@asynccontextmanager
async def _pynixd(disk: _Disk) -> AsyncIterator[Server]:
    spec = make_test_spec(
        store_id="local",
        store_path=disk.store_path,
        no_probe=True,
        postgres=None,
        gc_max_age=MAX_AGE,
        gc_allow_execute=True,
        op_routes={"CollectGarbage": OpRoute.pynixd},
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(stores=stores, ssh_port=None, http_port=None) as server:
        _freeze_tracker(server)
        yield server


async def test_a_pass_counts_what_its_deletes_free_on_disk(tmp_path: Path) -> None:
    """The target sits 1.4 units under the disk: shared frees 0, tiny 0.5, unique[0] 1, and the pass stops there."""
    async with _disk(tmp_path) as disk, _pynixd(disk) as server:
        await _backdate(disk.store_path, {disk.shared}, 30 * DAY)
        await _backdate(disk.store_path, {disk.tiny}, 100 * DAY)
        for index, path in enumerate(disk.unique):
            await _backdate(disk.store_path, {path}, (20 - 4 * index) * DAY)
        await _backdate(disk.store_path, {disk.live}, 0)
        # A collection deletes `gc-reserved-space` first (`engine.py`, as `gc.cc`): 8 MiB the plan never counts.
        (disk.store_path / "nix/var/nix/db/reserved").unlink(missing_ok=True)  # noqa: ASYNC240
        used, total = pressure_usage(disk.volume)
        target = (used - 1.4 * UNIT) / total
        await Collector(server.ctx).run(PynixdGCAction.EXECUTE, target_usage=target)
        used, total = pressure_usage(disk.volume)
        assert used / total <= target
        assert await _valid(disk.store_path) == {disk.live, *disk.unique[1:]}
        assert (disk.store_path / disk.live.lstrip("/")).read_bytes() == disk.live_bytes  # noqa: ASYNC240
