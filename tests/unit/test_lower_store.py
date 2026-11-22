"""A store over an immutable lower store reads each path's files where they are (nixidae/nixidae#1560, spike).

A build pod's store sits over its nixkube farm, a read-only volume with its own
database. The farm's path joins the pod's database once an output refers to it,
for the foreign key on references; its files stay in the farm all the same.
"""

from __future__ import annotations

import hashlib
import os
import stat
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import anyio.to_thread

from nix_daemon_protocol.content_address import ContentAddress as WireContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd_store.create_store import open_local_store
from pynixd_store.engine import StoreEngine
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.lower_store_engine import LowerStoreEngine
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

# 2.35 keys realisations by derivation path (`BuildTraceV3`), which `by_path=True` reads.
NIX_VERSION = "2.35.0"
# `experimental-features` without `ca-derivations`: Nix makes no realisation table, as in a farm made so.
CONFIG: dict[str, Any] = {
    name: {"value": value}
    for name, value in {
        "require-sigs": False,
        "trusted-public-keys": [],
        "secret-key-files": [],
        "auto-optimise-store": False,
        "fsync-store-paths": False,
        "experimental-features": [],
    }.items()
}
FARM_INPUT = StorePath(path=f"/nix/store/{'1' * 32}-farm-input")
OWN_INPUT = StorePath(path=f"/nix/store/{'2' * 32}-own-input")
OUTPUT = StorePath(path=f"/nix/store/{'3' * 32}-output")


async def _config() -> dict[str, Any]:
    return CONFIG


def _info(path: StorePath, references: set[StorePath]) -> ValidPathInfo:
    return ValidPathInfo(
        path=path,
        info=UnkeyedValidPathInfo(
            nar_hash=NARHash(hash=hashlib.sha256(path.name.encode()).hexdigest()),
            references=references,
            registration_time=Time(ts=0),
            nar_size=1,
            ultimate=False,
            sigs=set(),
            ca=WireContentAddress(value=""),
        ),
    )


CA: dict[str, Any] = {"experimental-features": {"value": ["ca-derivations"]}}


async def _filled(layout: StoreLayout, paths: list[ValidPathInfo], config: dict[str, Any], nix_version: str) -> None:
    """A store at *layout* that holds *paths*, each a file, closed as Nix leaves one."""
    big_lock = await anyio.to_thread.run_sync(open_local_store, layout, config, nix_version)
    try:
        engine = StoreEngine(layout, writes=True, read_config=_config, on_change=lambda _: None, name="fill")
        async with await LocalStoreDB.open(layout) as engine.db:
            for info in paths:
                layout.real_path(info.path).write_text(info.path.name)
            await engine.register_valid_paths(paths)
    finally:
        os.close(big_lock)


def _freeze(root: Path) -> None:
    """No one may write the lower store, as no one may write a farm volume."""
    writable = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH
    for path in sorted(root.rglob("*"), reverse=True):
        if not path.is_symlink():
            path.chmod(path.stat().st_mode & ~writable)


def _thaw(root: Path) -> None:
    for path in sorted(root.rglob("*")):
        if not path.is_symlink():
            path.chmod(path.stat().st_mode | stat.S_IWUSR)


@asynccontextmanager
async def _pod_store(
    tmp_path: Path,
    *,
    farm_config: dict[str, Any] | None = None,
    farm_nix: str = NIX_VERSION,
    pod_nix: str = NIX_VERSION,
) -> AsyncIterator[LowerStoreEngine]:
    """A pod's store over a farm. The farm has no realisation table unless *farm_config* turns CA on."""
    lower_root, upper_root = tmp_path / "farm", tmp_path / "build"
    await _filled(StoreLayout.chroot(lower_root), [_info(FARM_INPUT, set())], farm_config or {}, farm_nix)
    _freeze(lower_root)
    lower = StoreLayout.chroot(lower_root).as_read_only(immutable=True)
    layout = StoreLayout.chroot(upper_root).with_lower(lower)
    # The pod's own store has the realisation table: its `NIX_CONFIG` turns `ca-derivations` on.
    big_lock = await anyio.to_thread.run_sync(open_local_store, layout, CA, pod_nix)
    try:
        engine = LowerStoreEngine(layout, writes=True, read_config=_config, on_change=lambda _: None, name="pod")
        async with await LocalStoreDB.open(layout) as engine.db, await LocalStoreDB.open(lower) as engine.lower.db:
            await engine.load_lower()
            yield engine
    finally:
        os.close(big_lock)
        _thaw(lower_root)


async def test_a_farm_path_keeps_its_files_in_the_farm_after_an_output_refers_to_it(tmp_path: Path) -> None:
    async with _pod_store(tmp_path) as engine:
        assert engine.layout.lower is not None
        farm, build = engine.layout.lower.real_store_dir, engine.layout.real_store_dir
        assert await engine.is_valid_path(str(FARM_INPUT))
        assert engine.files_of(FARM_INPUT) == farm / FARM_INPUT.name

        build.joinpath(OWN_INPUT.name).write_text("own")
        build.joinpath(OUTPUT.name).write_text("out")
        await engine.register_valid_paths([_info(OWN_INPUT, set()), _info(OUTPUT, {FARM_INPUT, OWN_INPUT})])

        # The output's reference put the farm path in the pod's database; its files did not move.
        assert FARM_INPUT in await StoreEngine.query_valid_paths(engine, [str(FARM_INPUT)])
        assert engine.files_of(FARM_INPUT) == farm / FARM_INPUT.name
        assert engine.files_of(FARM_INPUT).read_text() == FARM_INPUT.name
        assert engine.files_of(OWN_INPUT) == build / OWN_INPUT.name
        assert engine.files_of(OUTPUT) == build / OUTPUT.name
        assert await engine.query_closure([str(OUTPUT)]) == {OUTPUT, FARM_INPUT, OWN_INPUT}
        # A collection of the pod's store frees nothing of the farm's.
        assert await engine.delete_store_path(FARM_INPUT.name) == 0
        assert (farm / FARM_INPUT.name).exists()


async def test_a_farm_without_realisations_answers_none_and_takes_the_pods(tmp_path: Path) -> None:
    async with _pod_store(tmp_path) as engine:
        drv = f"{'4' * 32}-x.drv"
        assert await engine.query_realisation(drv, "out", by_path=True) is None
        engine.layout.real_store_dir.joinpath(OUTPUT.name).write_text("out")
        await engine.register_valid_paths([_info(OUTPUT, set())])
        await engine.register_drv_output(drv, "out", str(OUTPUT), [], by_path=True)
        found = await engine.query_realisation(drv, "out", by_path=True)
        assert found is not None
        assert found[0] == OUTPUT


async def test_a_farm_of_another_nix_keys_realisations_its_own_way(tmp_path: Path) -> None:
    """The farm's 2.35 keys by path; the pod's 2.34 asks by hash, which the farm has no table for."""
    async with _pod_store(tmp_path, farm_config=CA, farm_nix="2.35.0", pod_nix="2.34.0") as engine:
        key = f"sha256:{'5' * 64}"
        assert await engine.query_realisation(key, "out", by_path=False) is None
        engine.layout.real_store_dir.joinpath(OUTPUT.name).write_text("out")
        await engine.register_valid_paths([_info(OUTPUT, set())])
        await engine.register_drv_output(key, "out", str(OUTPUT), [], by_path=False)
        found = await engine.query_realisation(key, "out", by_path=False)
        assert found is not None
        assert found[0] == OUTPUT
