"""The engine collects garbage as Nix 2.35 does (`LocalStore::collectGarbage`, `gc.cc:356`).

Each test builds a store with Nix's own schema and its own dead paths: a
chroot store keeps the `/nix/store` prefix, so every path a host process maps
is a runtime root, and a copied package would never be dead.
"""

from __future__ import annotations

import fcntl
import os
import sqlite3
from contextlib import asynccontextmanager, closing
from functools import partial
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.protocol import GCAction, Verbosity
from pynixd_store import verify
from pynixd_store.collect import GCOptions, GCSettings, path_fmt
from pynixd_store.engine import StoreEngine
from pynixd_store.errors import PynixdError
from pynixd_store.hintfmt import magenta
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout
from pynixd_store.temp_roots import TempRoots

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

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

A = "/nix/store/00000000000000000000000000000001-a"
B = "/nix/store/00000000000000000000000000000002-b"
C = "/nix/store/00000000000000000000000000000003-c"
DRV = "/nix/store/00000000000000000000000000000004-c.drv"


class _Lines:
    def __init__(self) -> None:
        self.lines: list[tuple[Verbosity, str]] = []

    async def report(self, level: Verbosity, text: str) -> None:
        self.lines.append((level, text))

    def at(self, level: Verbosity) -> list[str]:
        return [text for got, text in self.lines if got == level]


async def _no_config() -> dict[str, Any]:
    raise AssertionError("collectGarbage gets its settings from the caller")


class _Store:
    def __init__(self, root: Path) -> None:
        self.layout = StoreLayout.chroot(root)
        self.layout.real_store_dir.mkdir(parents=True)
        self.layout.links_dir.mkdir()
        self.layout.db_path.parent.mkdir(parents=True)
        # `LocalStore` makes both at open (`local-store.cc:146`); a walk that cannot read one says so.
        (self.layout.state_dir / "gcroots").mkdir()
        (self.layout.state_dir / "profiles").mkdir()
        with closing(sqlite3.connect(self.layout.db_path)) as conn, conn:
            conn.executescript(_SCHEMA)
        self.ids: dict[str, int] = {}

    def real(self, path: str) -> Path:
        return self.layout.real_store_dir / path.rsplit("/", 1)[-1]

    def add(self, path: str, *, refs: tuple[str, ...] = (), deriver: str | None = None, size: int = 100) -> None:
        directory = self.real(path)
        directory.mkdir()
        (directory / "data").write_bytes(b"x" * size)
        (directory / "data").chmod(0o444)
        directory.chmod(0o555)
        with closing(sqlite3.connect(self.layout.db_path)) as conn, conn:
            cursor = conn.execute(
                "insert into ValidPaths (path, hash, registrationTime, deriver, narSize) values (?, ?, 1, ?, ?)",
                (path, f"sha256:{'0' * 64}", deriver, size),
            )
            self.ids[path] = int(cursor.lastrowid or 0)
            for ref in refs:
                conn.execute("insert into Refs (referrer, reference) values (?, ?)", (self.ids[path], self.ids[ref]))

    def output(self, drv: str, name: str, path: str) -> None:
        with closing(sqlite3.connect(self.layout.db_path)) as conn, conn:
            conn.execute("insert into DerivationOutputs (drv, id, path) values (?, ?, ?)", (self.ids[drv], name, path))

    def root(self, path: str, name: str = "root") -> None:
        (self.layout.state_dir / "gcroots" / name).symlink_to(path)

    def valid(self, path: str) -> bool:
        with closing(sqlite3.connect(self.layout.db_path)) as conn:
            return conn.execute("select 1 from ValidPaths where path = ?", (path,)).fetchone() is not None

    def gone(self, path: str) -> bool:
        return not self.valid(path) and not self.real(path).exists()


@asynccontextmanager
async def _engine(store: _Store) -> AsyncIterator[StoreEngine]:
    engine = StoreEngine(store.layout, writes=True, read_config=_no_config, on_change=lambda _: None, name="test")
    async with await LocalStoreDB.open(store.layout) as engine.db:
        yield engine


async def _collect(
    store: _Store, options: GCOptions, lines: _Lines, settings: GCSettings | None = None
) -> tuple[set[str], int]:
    async with _engine(store) as engine:
        results = await engine.collect_garbage(options, settings=settings or GCSettings(), report=lines.report)
    return results.paths, results.bytes_freed


async def test_the_whole_store_loses_what_no_root_keeps(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    store.add(B)
    store.add(A, refs=(B,))
    store.add(C)
    store.root(C)
    (store.layout.real_store_dir / "garbage").write_bytes(b"junk")

    lines = _Lines()
    paths, freed = await _collect(store, GCOptions(GCAction.DELETE_DEAD), lines)

    assert paths == {A, B, "/nix/store/garbage"}
    assert freed == 204
    assert store.gone(A)
    assert store.gone(B)
    assert store.valid(C)
    assert store.real(C).exists()
    info = lines.at(Verbosity.INFO)
    assert info[:2] == ["finding garbage collector roots...", "deleting garbage..."]
    assert info[-2] == "deleting unused links..."
    # The value is the links directory's own blocks, which differ between file systems.
    assert info[-1].startswith("note: hard linking is currently saving ")
    # A referrer goes before the path it references.
    assert info.index(f"deleting '{A}'") < info.index(f"deleting '{B}'")


async def test_a_named_path_with_an_unnamed_referrer_is_alive(tmp_path: Path) -> None:
    """`gc.cc:784`: the error names the path, and nothing is deleted."""
    store = _Store(tmp_path)
    store.add(B)
    store.add(A, refs=(B,))

    with pytest.raises(PynixdError, match="since it is still alive") as error:
        await _collect(store, GCOptions(GCAction.DELETE_SPECIFIC, frozenset({B})), _Lines())

    assert "\x1b[35;1m" + B in str(error.value)
    assert store.valid(A)
    assert store.valid(B)


async def test_named_paths_go_with_their_named_referrers(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    store.add(B)
    store.add(A, refs=(B,))
    store.add(C)

    lines = _Lines()
    paths, _ = await _collect(store, GCOptions(GCAction.DELETE_SPECIFIC, frozenset({A, B})), lines)

    assert paths == {A, B}
    assert store.valid(C)
    assert lines.at(Verbosity.INFO)[1] == "deleting specified paths..."


async def test_the_referrers_go_when_the_request_says_so(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    store.add(B)
    store.add(A, refs=(B,))

    paths, _ = await _collect(store, GCOptions(GCAction.DELETE_DEAD, frozenset({B}), delete_referrers=True), _Lines())

    assert paths == {A, B}


async def test_the_whole_store_cannot_be_deleted_by_name(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    with pytest.raises(PynixdError, match="Cannot delete the entire store"):
        await _collect(store, GCOptions(GCAction.DELETE_SPECIFIC), _Lines())


async def test_live_and_dead_are_answered_and_nothing_goes(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    store.add(B)
    store.add(A, refs=(B,))
    store.add(C)
    store.root(A)

    live, _ = await _collect(store, GCOptions(GCAction.RETURN_LIVE), _Lines())
    dead, _ = await _collect(store, GCOptions(GCAction.RETURN_DEAD), _Lines())

    assert {A, B} <= live
    assert C not in live
    assert dead == {C}
    assert store.valid(C)
    assert store.real(C).exists()


async def test_max_freed_stops_the_collection(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    store.add(A)
    store.add(B)

    lines = _Lines()
    paths, _ = await _collect(store, GCOptions(GCAction.DELETE_DEAD, max_freed=1), lines)

    assert len(paths) == 1
    assert "deleted more than 1 bytes; stopping" in lines.at(Verbosity.INFO)
    # The links sweep still runs after the stop.
    assert lines.at(Verbosity.INFO)[-2] == "deleting unused links..."


async def test_a_temporary_root_file_keeps_its_path(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    store.add(A)
    roots = TempRoots(store.layout.state_dir)
    assert await roots.add(A)
    try:
        paths, _ = await _collect(store, GCOptions(GCAction.DELETE_DEAD), _Lines())
    finally:
        await roots.close()

    assert paths == set()
    assert store.valid(A)


async def test_ignore_liveness_deletes_a_path_a_root_keeps(tmp_path: Path) -> None:
    """`gc.cc:521`: `findRootsNoTemp` does not run, so no `gcroots` link or runtime root keeps a path."""
    store = _Store(tmp_path)
    store.add(A)
    store.root(A)
    with pytest.raises(PynixdError, match="still alive"):
        await _collect(store, GCOptions(GCAction.DELETE_SPECIFIC, frozenset({A})), _Lines())

    paths, _ = await _collect(
        store, GCOptions(GCAction.DELETE_SPECIFIC, frozenset({A}), ignore_liveness=True), _Lines()
    )

    assert paths == {A}
    assert store.gone(A)


@pytest.mark.parametrize("in_test", [False, True])
async def test_only_a_nix_test_walks_past_a_temporary_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, in_test: bool
) -> None:
    """Determinate v3.24.0 `gc.cc:770`: the walk skips the check; the deletion loop still keeps the path (`:757`).

    Outside a test the walk stops at the root set, which holds each temporary root as upstream's does (`gc.cc:531`).
    """
    if in_test:
        monkeypatch.setenv("_NIX_IN_TEST", str(tmp_path))
    else:
        monkeypatch.delenv("_NIX_IN_TEST", raising=False)
    store = _Store(tmp_path)
    store.add(A)
    roots = TempRoots(store.layout.state_dir)
    assert await roots.add(A)
    lines = _Lines()
    try:
        paths, _ = await _collect(store, GCOptions(GCAction.DELETE_DEAD, ignore_liveness=True), lines)
    finally:
        await roots.close()

    assert paths == set()
    assert store.valid(A)
    walked = f"not deleting '{A}' because it became a temporary root after initial scan"
    stopped = f"cannot delete '{A}' because it's a root"
    assert (walked if in_test else stopped) in lines.at(Verbosity.DEBUG)
    assert (stopped if in_test else walked) not in lines.at(Verbosity.DEBUG)


async def test_a_root_given_to_the_socket_during_the_collection_is_kept(tmp_path: Path) -> None:
    """`gc.cc:463`: a client that roots a path while the lock is held reaches the collector over the socket."""
    store = _Store(tmp_path)
    store.add(A)
    store.add(B)
    roots = TempRoots(store.layout.state_dir)
    lines = _Lines()

    async def report(level: Verbosity, text: str) -> None:
        await lines.report(level, text)
        if text == "deleting garbage...":
            # The lock is held, so the root cannot go to the file the collector read.
            with anyio.fail_after(5):
                assert await roots.add(A)

    try:
        async with _engine(store) as engine:
            results = await engine.collect_garbage(
                GCOptions(GCAction.DELETE_DEAD), settings=GCSettings(), report=report
            )
    finally:
        await roots.close()

    assert results.paths == {B}
    assert store.valid(A)
    assert (Verbosity.DEBUG, f"got new GC root '{A}'") in lines.lines


async def test_keep_derivations_keeps_the_deriver_of_a_live_output(tmp_path: Path) -> None:
    """`keep-derivations` widens the walk of the whole store, and only that walk in 2.35 (`gc.cc:604`)."""
    store = _Store(tmp_path)
    store.add(DRV)
    store.add(C, deriver=DRV)
    store.output(DRV, "out", C)
    store.root(C)

    paths, _ = await _collect(store, GCOptions(GCAction.DELETE_DEAD), _Lines())
    assert paths == set()
    assert store.valid(DRV)

    paths, _ = await _collect(store, GCOptions(GCAction.DELETE_DEAD), _Lines(), GCSettings(keep_derivations=False))
    assert paths == {DRV}


async def test_a_trusted_override_reaches_the_settings() -> None:
    config = {"keep-outputs": {"value": False}, "keep-derivations": {"value": True}}
    settings = GCSettings.from_nix_config(config, {"gc-keep-outputs": "true", "keep-derivations": "false"})
    assert settings == GCSettings(keep_outputs=True, keep_derivations=False)
    with pytest.raises(PynixdError, match="Boolean setting 'keep-outputs' has invalid value 'maybe'"):
        GCSettings.from_nix_config(config, {"keep-outputs": "maybe"})


async def test_a_held_gc_lock_is_reported_then_waited_for(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    holder = os.open(store.layout.state_dir / verify.GC_LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(holder, fcntl.LOCK_SH)
    lines = _Lines()
    try:
        async with _engine(store) as engine, anyio.create_task_group() as tg:
            tg.start_soon(
                partial(
                    engine.collect_garbage, GCOptions(GCAction.DELETE_DEAD), settings=GCSettings(), report=lines.report
                )
            )
            while "waiting for the big garbage collector lock..." not in lines.at(Verbosity.INFO):
                await anyio.sleep(0.01)
            assert "finding garbage collector roots..." not in lines.at(Verbosity.INFO)
            fcntl.flock(holder, fcntl.LOCK_UN)
    finally:
        os.close(holder)

    assert "finding garbage collector roots..." in lines.at(Verbosity.INFO)


async def test_a_locked_tmp_dir_survives(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    tmp = store.layout.real_store_dir / "tmp-1-1"
    tmp.mkdir()
    holder = os.open(tmp, os.O_RDONLY | os.O_DIRECTORY)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        paths, _ = await _collect(store, GCOptions(GCAction.DELETE_DEAD), _Lines())
    finally:
        os.close(holder)

    assert paths == set()
    assert tmp.exists()


async def test_the_walk_of_the_roots_says_what_it_skipped(tmp_path: Path) -> None:
    """`findRoots` (`gc.cc:223`) and `findTempRoots` (`gc.cc:190`) print what they skip and reap."""
    store = _Store(tmp_path)
    gone = "/nix/store/00000000000000000000000000000009-gone"
    store.root(gone, "dangling")
    temproots = store.layout.state_dir / "temproots"
    temproots.mkdir(parents=True)
    (temproots / "12345").write_bytes(f"{gone}\0".encode())

    lines = _Lines()
    await _collect(store, GCOptions(GCAction.DELETE_DEAD), lines)

    info = lines.at(Verbosity.INFO)
    link = store.layout.state_dir / "gcroots" / "dangling"
    assert f'skipping invalid root from "{link}" to "{gone}"' in info
    assert f'removing stale temporary roots file "{temproots / "12345"}"' in info
    assert info.index(f'skipping invalid root from "{link}" to "{gone}"') < info.index("deleting garbage...")
    assert not (temproots / "12345").exists()


@pytest.mark.skipif(os.geteuid() == 0, reason="root unlinks in a read-only directory")
async def test_an_unreadable_root_directory_is_named(tmp_path: Path) -> None:
    store = _Store(tmp_path)
    locked = store.layout.state_dir / "gcroots" / "locked"
    locked.mkdir()
    locked.chmod(0)
    lines = _Lines()
    try:
        await _collect(store, GCOptions(GCAction.RETURN_DEAD), lines)
    finally:
        locked.chmod(0o755)

    assert f'cannot read potential root "{locked}"' in lines.at(Verbosity.INFO)


@pytest.mark.skipif(os.geteuid() == 0, reason="root unlinks in a read-only directory")
async def test_a_failed_delete_carries_nix_s_trace(tmp_path: Path) -> None:
    """`deleteStorePath` adds `While deleting store path` to the `SysError` of `deletePath` (`local-store.cc:420`)."""
    store = _Store(tmp_path)
    store.add(A)
    store.layout.real_store_dir.chmod(0o555)
    try:
        with pytest.raises(PynixdError) as error:
            await _collect(store, GCOptions(GCAction.DELETE_DEAD), _Lines())
    finally:
        store.layout.real_store_dir.chmod(0o755)

    real = store.real(A)
    assert str(error.value) == f"cannot unlink {path_fmt(real)}: {magenta('Permission denied')}"
    assert error.value.traces == (f"While deleting store path {path_fmt(real)}",)
