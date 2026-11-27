"""A GC run on a real file system reaches its target when a delete frees less than its NAR size.

The plan counts NAR bytes; the disk counts blocks. Here a tmpfs holds an
optimised store, and one dead path shares its file with a live one through
`.links`: deleting it frees nothing. The first pass stops where its plan
expects the target, still over it on disk. `_until_under_target` reads the
settled disk and passes again by the share that reached it. The test also
proves that the targeted `.links` cleanup returns blocks: an optimised
unique file has a second name in `.links`, and without the cleanup no
delete frees anything. The root suite runs it because only root mounts a
tmpfs.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.daemon_extensions import PynixdCollectGarbageResponse, PynixdGCAction
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
    shared: str
    """Dead, oldest, and the same bytes as `live`: one inode in `.links`, so its delete frees nothing."""
    unique: list[str]
    """Dead, oldest first, each its own bytes."""
    live_bytes: bytes


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
        live_bytes = os.urandom(UNIT)
        live = await _add(store_path, work, "live.bin", live_bytes)
        shared = await _add(store_path, work, "shared.bin", live_bytes)
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
        yield _Disk(volume, store_path, live, shared, unique, live_bytes)
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


async def _collect(disk: _Disk) -> tuple[float, float]:
    """Run one GC over a target two and a half units under the disk. `(target, usage after)`."""
    async with _pynixd(disk) as server:
        await _backdate(disk.store_path, {disk.shared}, 30 * DAY)
        for index, path in enumerate(disk.unique):
            await _backdate(disk.store_path, {path}, (20 - 4 * index) * DAY)
        await _backdate(disk.store_path, {disk.live}, 0)
        # A collection deletes `gc-reserved-space` first (`engine.py`, as `gc.cc`): 8 MiB the plan never counts.
        (disk.store_path / "nix/var/nix/db/reserved").unlink(missing_ok=True)  # noqa: ASYNC240
        used, total = pressure_usage(disk.volume)
        # The plan takes shared, unique[0] and unique[1] (3 units of NAR) to free 2.5; the disk frees 2.
        target = (used - 2.5 * UNIT) / total
        await Collector(server.ctx).run(PynixdGCAction.EXECUTE, target_usage=target)
    used, total = pressure_usage(disk.volume)
    return target, used / total


async def test_a_run_passes_again_until_the_disk_is_under_target(tmp_path: Path) -> None:
    async with _disk(tmp_path) as disk:
        target, after = await _collect(disk)
        assert after <= target
        assert await _valid(disk.store_path) == {disk.live, *disk.unique[3:]}
        assert (disk.store_path / disk.live.lstrip("/")).read_bytes() == disk.live_bytes


async def test_one_pass_alone_stays_over_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The negative control: without the second pass, the run ends over its target with the plan's paths gone."""

    async def one_pass(
        self: Collector, response: PynixdCollectGarbageResponse, *_: object
    ) -> PynixdCollectGarbageResponse:
        return response

    monkeypatch.setattr(Collector, "_until_under_target", one_pass)
    async with _disk(tmp_path) as disk:
        target, after = await _collect(disk)
        assert after > target
        assert await _valid(disk.store_path) == {disk.live, *disk.unique[2:]}
