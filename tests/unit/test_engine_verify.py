"""The engine verifies the store as Nix does (`LocalStore::verifyStore`).

`verifyStore` cross-checks the database and the store directory: a valid path
the directory lost is invalidated when its referrers can go too, and, with
`check_contents`, a modified file is an error and a missing hash is filled.
These tests build a store with Nix's own schema, so the walk, the referrer
recursion and the fix-ups run against real rows and files.
"""

from __future__ import annotations

import fcntl
import os
import shutil
import sqlite3
from contextlib import asynccontextmanager, closing
from functools import partial
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.protocol import Verbosity
from pynixd_store import verify
from pynixd_store.engine import StoreEngine
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout
from pynixd_store.utils import nix32_encode

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

HELLO = b"hello\n"

_SCHEMA = """
create table ValidPaths (
    id integer primary key autoincrement not null,
    path text unique not null, hash text not null, registrationTime integer not null,
    deriver text, narSize integer, ultimate integer, sigs text, ca text);
create table Refs (
    referrer integer not null, reference integer not null, primary key (referrer, reference),
    foreign key (referrer) references ValidPaths(id) on delete cascade,
    foreign key (reference) references ValidPaths(id) on delete restrict);
create trigger DeleteSelfRefs before delete on ValidPaths
  begin delete from Refs where referrer = old.id and reference = old.id; end;
create table DerivationOutputs (
    drv integer not null, id text not null, path text not null, primary key (drv, id),
    foreign key (drv) references ValidPaths(id) on delete cascade);
"""


async def _no_config() -> dict[str, Any]:
    raise AssertionError("verifyStore reads no Nix setting")


class _Lines:
    """What the engine reports, as the daemon's logger would receive it."""

    def __init__(self) -> None:
        self.lines: list[tuple[Verbosity, str]] = []

    async def report(self, level: Verbosity, text: str) -> None:
        self.lines.append((level, text))

    def at(self, level: Verbosity) -> list[str]:
        return [text for got, text in self.lines if got == level]


def _write_path(layout: StoreLayout, name: str, content: bytes) -> tuple[str, str, int]:
    """Create a store path (a directory with one file) and return its logical path, hash and size."""
    directory = layout.real_store_dir / name
    directory.mkdir()
    (directory / "data").write_bytes(content)
    (directory / "data").chmod(0o444)
    directory.chmod(0o555)
    nar_hex, nar_size = verify.nar_hash_and_size(directory)
    return f"/nix/store/{name}", f"sha256:{nar_hex}", nar_size


def _remove(directory: Path) -> None:
    """Remove a store path `_write_path` made: its directory is read-only, so `rmtree` alone fails."""
    directory.chmod(0o755)
    shutil.rmtree(directory)


def _make_store(layout: StoreLayout) -> None:
    layout.real_store_dir.mkdir(parents=True)
    layout.db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(layout.db_path)) as conn, conn:
        conn.executescript(_SCHEMA)


def _insert(layout: StoreLayout, path: str, nar_hash: str, nar_size: int) -> int:
    with closing(sqlite3.connect(layout.db_path)) as conn, conn:
        cursor = conn.execute(
            "insert into ValidPaths (path, hash, registrationTime, narSize) values (?, ?, 1, ?)",
            (path, nar_hash, nar_size),
        )
        return int(cursor.lastrowid or 0)


def _add_ref(layout: StoreLayout, referrer: int, reference: int) -> None:
    with closing(sqlite3.connect(layout.db_path)) as conn, conn:
        conn.execute("insert into Refs (referrer, reference) values (?, ?)", (referrer, reference))


def _is_valid(layout: StoreLayout, path: str) -> bool:
    with closing(sqlite3.connect(layout.db_path)) as conn:
        return conn.execute("select 1 from ValidPaths where path = ?", (path,)).fetchone() is not None


def _stored_hash_size(layout: StoreLayout, path: str) -> tuple[str, int]:
    with closing(sqlite3.connect(layout.db_path)) as conn:
        row = conn.execute("select hash, narSize from ValidPaths where path = ?", (path,)).fetchone()
    return row[0], row[1]


@asynccontextmanager
async def _engine(root: Path, *, writes: bool = True) -> AsyncIterator[StoreEngine]:
    layout = StoreLayout.chroot(root)
    engine = StoreEngine(layout, writes=writes, read_config=_no_config, on_change=lambda _: None, name="test")
    async with await LocalStoreDB.open(layout) as engine.db:
        yield engine


async def test_a_sound_store_reports_no_error(tmp_path: Path) -> None:
    layout = StoreLayout.chroot(tmp_path)
    _make_store(layout)
    path, nar_hash, nar_size = _write_path(layout, "00000000000000000000000000000001-a", HELLO)
    _insert(layout, path, nar_hash, nar_size)

    lines = _Lines()
    async with _engine(tmp_path) as engine:
        assert await engine.verify_store(check_contents=True, report=lines.report) is False
    assert _is_valid(layout, path)
    # `local-store.cc:1382-1415`: Nix's lines, in Nix's order, at Nix's levels.
    assert lines.at(Verbosity.INFO) == [
        "reading the Nix store...",
        "checking path existence...",
        "checking link hashes...",
        "checking store hashes...",
    ]
    assert lines.at(Verbosity.TALKATIVE) == [f"checking contents of '{path}'"]


async def test_a_disappeared_path_is_invalidated(tmp_path: Path) -> None:
    """A valid path the directory lost, with no valid referrer, is removed from the database."""
    layout = StoreLayout.chroot(tmp_path)
    _make_store(layout)
    path, nar_hash, nar_size = _write_path(layout, "00000000000000000000000000000001-a", HELLO)
    _insert(layout, path, nar_hash, nar_size)
    # The files are gone, the row stays: exactly what verifyStore repairs.
    _remove(layout.real_store_dir / "00000000000000000000000000000001-a")

    lines = _Lines()
    async with _engine(tmp_path) as engine:
        errors = await engine.verify_store(check_contents=False, report=lines.report)

    assert errors is False
    assert not _is_valid(layout, path)
    assert f"path '{path}' disappeared, removing from database..." in lines.at(Verbosity.INFO)


async def test_a_disappeared_path_with_a_valid_referrer_is_an_error(tmp_path: Path) -> None:
    layout = StoreLayout.chroot(tmp_path)
    _make_store(layout)
    gone, gone_hash, gone_size = _write_path(layout, "00000000000000000000000000000001-gone", HELLO)
    keep, keep_hash, keep_size = _write_path(layout, "00000000000000000000000000000002-keep", b"keep\n")
    gone_id = _insert(layout, gone, gone_hash, gone_size)
    keep_id = _insert(layout, keep, keep_hash, keep_size)
    _add_ref(layout, keep_id, gone_id)  # keep refers to gone
    _remove(layout.real_store_dir / "00000000000000000000000000000001-gone")

    lines = _Lines()
    async with _engine(tmp_path) as engine:
        errors = await engine.verify_store(check_contents=False, report=lines.report)

    assert errors is True
    # The lost path keeps its row, because a valid path still refers to it.
    assert _is_valid(layout, gone)
    assert lines.at(Verbosity.ERROR) == [f"path '{gone}' disappeared, but it still has valid referrers!"]


async def test_a_modified_file_is_detected(tmp_path: Path) -> None:
    layout = StoreLayout.chroot(tmp_path)
    _make_store(layout)
    path, nar_hash, nar_size = _write_path(layout, "00000000000000000000000000000001-a", HELLO)
    _insert(layout, path, nar_hash, nar_size)
    # Change the content after the hash is recorded.
    data = layout.real_store_dir / "00000000000000000000000000000001-a" / "data"
    data.chmod(0o644)
    data.write_bytes(b"tampered\n")

    lines = _Lines()
    async with _engine(tmp_path) as engine:
        assert await engine.verify_store(check_contents=True, report=lines.report) is True

    got = f"sha256:{nix32_encode(bytes.fromhex(verify.nar_hash_and_size(data.parent)[0]))}"
    expected = f"sha256:{nix32_encode(bytes.fromhex(nar_hash.removeprefix('sha256:')))}"
    assert lines.at(Verbosity.ERROR) == [f"path '{path}' was modified! expected hash '{expected}', got '{got}'"]


async def test_a_missing_hash_is_filled(tmp_path: Path) -> None:
    """A null `narHash` is filled from the file, as `verifyStore` does; it is not an error."""
    layout = StoreLayout.chroot(tmp_path)
    _make_store(layout)
    path, nar_hash, nar_size = _write_path(layout, "00000000000000000000000000000001-a", HELLO)
    _insert(layout, path, f"sha256:{'0' * 64}", 0)

    lines = _Lines()
    async with _engine(tmp_path) as engine:
        assert await engine.verify_store(check_contents=True, report=lines.report) is False

    assert _stored_hash_size(layout, path) == (nar_hash, nar_size)
    assert lines.at(Verbosity.INFO)[-2:] == [
        f"fixing missing hash on '{path}'",
        f"updating size field on '{path}' to {nar_size}",
    ]


async def test_the_engine_refuses_to_verify_with_writes_off(tmp_path: Path) -> None:
    layout = StoreLayout.chroot(tmp_path)
    engine = StoreEngine(layout, writes=False, read_config=_no_config, on_change=lambda _: None, name="test")
    with pytest.raises(RuntimeError, match="pynixd_writes is off"):
        await engine.verify_store(check_contents=False, report=_Lines().report)


async def test_a_modified_link_is_an_error(tmp_path: Path) -> None:
    """A `.links` entry whose content no longer hashes to its name (`local-store.cc:1404`)."""
    layout = StoreLayout.chroot(tmp_path)
    _make_store(layout)
    layout.links_dir.mkdir()
    probe = tmp_path / "probe"
    probe.write_bytes(HELLO)
    name = verify.link_hash(probe)
    (layout.links_dir / name).write_bytes(b"tampered\n")
    got = verify.link_hash(layout.links_dir / name)

    lines = _Lines()
    async with _engine(tmp_path) as engine:
        assert await engine.verify_store(check_contents=True, report=lines.report) is True

    assert lines.at(Verbosity.TALKATIVE) == [f'checking contents of "{name}"']
    assert lines.at(Verbosity.ERROR) == [
        f"link \"{layout.links_dir / name}\" was modified! expected hash {name}, got '{got}'"
    ]


async def test_a_held_gc_lock_is_reported_then_waited_for(tmp_path: Path) -> None:
    """`FdLock` prints its wait message only when the lock is taken (`pathlocks.cc:150`)."""
    layout = StoreLayout.chroot(tmp_path)
    _make_store(layout)
    collector = os.open(layout.state_dir / verify.GC_LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(collector, fcntl.LOCK_EX)
    lines = _Lines()
    try:
        async with _engine(tmp_path) as engine, anyio.create_task_group() as tg:
            tg.start_soon(partial(engine.verify_store, check_contents=False, report=lines.report))
            while "waiting for the big garbage collector lock..." not in lines.at(Verbosity.INFO):
                await anyio.sleep(0.01)
            assert "checking path existence..." not in lines.at(Verbosity.INFO)
            fcntl.flock(collector, fcntl.LOCK_UN)
    finally:
        os.close(collector)

    assert lines.at(Verbosity.INFO)[-1] == "checking path existence..."
