"""Each root's storage, full and exclusive, attributed exactly.

Two roots share a library: both full sizes count it, neither exclusive
does. A derivation pulls its build input into its own full size through
the deriver branch, and a path no root reaches lands nowhere. The
counting runs through one integer counter per valid path -- pairs would
store an order of magnitude more -- so the fixture below stays small
while the production table does not.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

import pynixd.handlers.pynixd_roots_report as roots_handler
from pynixd.daemon_extensions.pynixd_roots_report import (
    PynixdRootsReportRequest,
    PynixdRootsReportResponse,
    RootsReportRow,
)
from pynixd.handlers.pynixd_roots_report import PynixdRootsReportHandler
from pynixd.local_store_db import LocalStoreDB
from pynixd.serde.auth import Role
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.store_layout import StoreLayout
from pynixd.wire import BytesReader, BytesWriter

if TYPE_CHECKING:
    from pathlib import Path

VERSION = 0x126

A = "/nix/store/00000000000000000000000000000001-a"
B = "/nix/store/00000000000000000000000000000002-b"
DEP = "/nix/store/00000000000000000000000000000003-dep"
DRV = "/nix/store/00000000000000000000000000000004-x.drv"
TOOL = "/nix/store/00000000000000000000000000000005-tool"
DEAD = "/nix/store/00000000000000000000000000000006-dead"


def _store_with_shared_dep(tmp_path: Path) -> Path:
    """Two roots share `dep`; `a` builds through `drv` which needs `tool`."""
    db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths ("
            "id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER, narSize INTEGER)",
        )
        conn.execute("CREATE TABLE Refs (referrer INTEGER, reference INTEGER)")
        conn.execute("INSERT INTO ValidPaths (id, path, deriver, narSize) VALUES (1, ?, NULL, 100)", (A,))
        conn.execute("INSERT INTO ValidPaths (id, path, deriver, narSize) VALUES (2, ?, NULL, 200)", (B,))
        conn.execute("INSERT INTO ValidPaths (id, path, deriver, narSize) VALUES (3, ?, NULL, 300)", (DEP,))
        conn.execute("INSERT INTO ValidPaths (id, path, deriver, narSize) VALUES (4, ?, NULL, 400)", (DRV,))
        conn.execute("INSERT INTO ValidPaths (id, path, deriver, narSize) VALUES (5, ?, NULL, 500)", (TOOL,))
        conn.execute("INSERT INTO ValidPaths (id, path, deriver, narSize) VALUES (6, ?, NULL, 600)", (DEAD,))
        conn.execute("UPDATE ValidPaths SET deriver = ? WHERE path = ?", (DRV, A))
        conn.execute("INSERT INTO Refs (referrer, reference) VALUES (1, 3)")
        conn.execute("INSERT INTO Refs (referrer, reference) VALUES (2, 3)")
        conn.execute("INSERT INTO Refs (referrer, reference) VALUES (4, 5)")
    return db_path


@pytest.mark.anyio
async def test_shared_paths_count_full_but_never_exclusive(tmp_path: Path) -> None:
    """`dep` feeds both full sizes and neither exclusive one; `dead` feeds none."""
    _store_with_shared_dep(tmp_path)
    async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
        report = await db.query_roots_report([("root-a", [A]), ("root-b", [B])])

        assert report is not None
        by_label = {row.label: row for row in report}
        assert (by_label["root-a"].full_paths, by_label["root-a"].full_bytes) == (4, 1300)
        assert (by_label["root-b"].full_paths, by_label["root-b"].full_bytes) == (2, 500)
        assert (by_label["root-a"].exclusive_paths, by_label["root-a"].exclusive_bytes) == (3, 1000)
        assert (by_label["root-b"].exclusive_paths, by_label["root-b"].exclusive_bytes) == (1, 200)


@pytest.mark.anyio
async def test_labels_sharing_seeds_count_separately(tmp_path: Path) -> None:
    """Two names for one closure: `dep` is shared, so exclusive stays empty."""
    _store_with_shared_dep(tmp_path)
    async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
        report = await db.query_roots_report([("one", [B]), ("two", [B])])

        assert report is not None
        assert [(row.label, row.full_paths, row.exclusive_paths) for row in report] == [
            ("one", 2, 0),
            ("two", 2, 0),
        ]


@pytest.mark.anyio
async def test_no_roots_reports_nothing(tmp_path: Path) -> None:
    _store_with_shared_dep(tmp_path)
    async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
        assert await db.query_roots_report([]) == []


class FakeStore:
    """A store with a database; the roots walk is patched per test."""

    def __init__(self, db: LocalStoreDB | None, layout: StoreLayout | None) -> None:
        self.db = db
        self.layout = layout


class FakeProxy:
    def __init__(self, body: bytes, local_store: FakeStore) -> None:
        self.r = BytesReader(body, identifier="test:roots")
        self.version = VERSION
        self.standard_features: frozenset[str] = frozenset()
        self.local_store = local_store
        self.ctx = SimpleNamespace(local_store=local_store)
        self.client: Any = None
        self.errors: list[str] = []

    async def send_error(self, message: str) -> None:
        self.errors.append(message)


@dataclass
class FakeContext:
    proxy: FakeProxy
    role: Role
    version: int = VERSION
    username: str = "test"


async def _handle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    db: LocalStoreDB | None,
    role: Role,
    labeled: list[tuple[str, set[str]]] | None,
) -> tuple[FakeProxy, object | None]:
    """The handler with the filesystem walk replaced by `labeled`."""
    if labeled is None:
        store: FakeStore = FakeStore(db, None)
    else:
        monkeypatch.setattr(roots_handler, "walk_labeled_roots", lambda *args: labeled)
        store = FakeStore(db, StoreLayout.chroot(tmp_path))
    writer = BytesWriter("test")
    await PynixdRootsReportRequest().to_writer(WriteContext(writer=writer, version=VERSION))
    proxy = FakeProxy(writer.get_bytes()[8:], store)
    resp = await PynixdRootsReportHandler().handle(FakeContext(proxy=proxy, role=role))  # type: ignore[arg-type]  # fakes
    return proxy, resp


@pytest.mark.anyio
async def test_the_handler_attributes_each_labeled_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The walk's labels reach the wire rows with full and exclusive numbers."""
    _store_with_shared_dep(tmp_path)
    async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
        proxy, resp = await _handle(monkeypatch, tmp_path, db, Role.ADMIN, [("root-a", {A}), ("root-b", {B})])

    assert proxy.errors == []
    assert isinstance(resp, PynixdRootsReportResponse)
    by_label = {row.label: row for row in resp.rows}
    assert (by_label["root-a"].full_bytes, by_label["root-a"].exclusive_bytes) == (1300, 1000)
    assert (by_label["root-b"].full_bytes, by_label["root-b"].exclusive_bytes) == (500, 200)


@pytest.mark.anyio
async def test_the_handler_refuses_the_untrusted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _store_with_shared_dep(tmp_path)
    async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
        proxy, resp = await _handle(monkeypatch, tmp_path, db, Role.USER, [("root-a", {A})])

    assert resp is None
    assert len(proxy.errors) == 1


@pytest.mark.anyio
async def test_the_handler_reports_nothing_without_a_layout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No layout, no walk, no database read: an empty report, not an error."""
    _store_with_shared_dep(tmp_path)
    async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
        proxy, resp = await _handle(monkeypatch, tmp_path, db, Role.ADMIN, None)

    assert proxy.errors == []
    assert isinstance(resp, PynixdRootsReportResponse)
    assert resp.rows == []


@pytest.mark.anyio
async def test_the_response_round_trips_on_the_wire() -> None:
    """Rows encode inline and decode back: label and all four numbers."""
    resp = PynixdRootsReportResponse(
        rows=[RootsReportRow(label="proc", full_paths=3, full_bytes=300, exclusive_paths=1, exclusive_bytes=100)],
    )
    writer = BytesWriter("test")
    await resp.to_writer(WriteContext(writer=writer, version=VERSION))
    back = await PynixdRootsReportResponse.from_reader(
        ReadContext(
            reader=BytesReader(writer.get_bytes(), identifier="test:roots"),
            version=VERSION,
            features=frozenset(),
        )
    )

    assert back.rows == resp.rows
