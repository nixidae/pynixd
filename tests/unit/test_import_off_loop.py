"""An import into pynixd's own store makes no file call on the event loop (nixidae/nixidae#1545).

The functional suite hung 8 tests under host I/O load with the loop inside `PathLock.release`.
`LoopFileCalls` records every audited file call on the loop's thread while the import runs.
"""

from __future__ import annotations

import hashlib
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import anyio.to_thread

from nix_daemon_protocol.content_address import ContentAddress as WireContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd_store.add import add_dump, add_nar
from pynixd_store.content_address import Method
from pynixd_store.create_store import open_local_store
from pynixd_store.engine import StoreEngine
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.nar_fs import dump_path
from pynixd_store.source import Pieces
from pynixd_store.store_layout import StoreLayout
from tests._conftest.loop_files import LoopFileCalls

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

NIX_VERSION = "2.34.0"


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


async def _config() -> dict[str, Any]:
    return CONFIG


@asynccontextmanager
async def _engine(root: Path) -> AsyncIterator[StoreEngine]:
    layout = StoreLayout.chroot(root)
    big_lock = await anyio.to_thread.run_sync(open_local_store, layout, {}, NIX_VERSION)
    try:
        engine = StoreEngine(layout, writes=True, read_config=_config, on_change=lambda _: None, name="test")
        async with await LocalStoreDB.open(layout) as engine.db:
            yield engine
    finally:
        os.close(big_lock)


async def _pieces(data: bytes) -> AsyncIterator[bytes]:
    for start in range(0, len(data), 4096):
        yield data[start : start + 4096]


def _tree(at: Path) -> bytes:
    at.mkdir()
    (at / "bin").mkdir()
    (at / "bin" / "tool").write_bytes(b"#!/bin/sh\n" * 500)
    (at / "bin" / "tool").chmod(0o555)
    (at / "data").write_bytes(b"x" * 100_000)
    (at / "link").symlink_to("data")
    return b"".join(dump_path(at))


async def test_add_nar_makes_no_file_call_on_the_loop(tmp_path: Path) -> None:
    nar = _tree(tmp_path / "src")
    path = StorePath(path=f"/nix/store/{'1' * 32}-tree")
    info = ValidPathInfo(
        path=path,
        info=UnkeyedValidPathInfo(
            nar_hash=NARHash(hash=hashlib.sha256(nar).hexdigest()),
            references=set(),
            registration_time=Time(ts=0),
            nar_size=len(nar),
            ultimate=False,
            sigs=set(),
            ca=WireContentAddress(value=""),
        ),
    )
    async with _engine(tmp_path / "root") as engine:
        with LoopFileCalls() as loop:
            await add_nar(engine, info, Pieces(_pieces(nar)), repair=False, auto_optimise=False, fsync=False)
        assert await engine.is_valid_path_uncached(str(path))
    assert loop.calls == []


async def test_add_dump_makes_no_file_call_on_the_loop(tmp_path: Path) -> None:
    nar = _tree(tmp_path / "src")

    async def root(_path: str) -> None:
        return

    async with _engine(tmp_path / "root") as engine:
        with LoopFileCalls() as loop:
            path = await add_dump(
                engine,
                "tree",
                Method.NAR,
                "sha256",
                frozenset(),
                Pieces(_pieces(nar)),
                root=root,
                auto_optimise=False,
                fsync=False,
            )
        assert await engine.is_valid_path_uncached(path)
    assert loop.calls == []
