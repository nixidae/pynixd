"""The route of each operation of the local store: daemon, shadow or pynixd.

nixidae/nixidae#1155 moves operations from the daemon into pynixd one at a
time. The route says who answers, so an answer moves with a setting and
moves back without a revert. `pynixd/op_routes.py` says what each route does.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING, Any, ClassVar

import pytest
from prometheus_client import REGISTRY
from pydantic import ValidationError

from nix_daemon_protocol.ids import StoreId
from pynixd.config import LocalSocketStoreSpec
from pynixd.local_store_db import LocalStoreDB
from pynixd.op_routes import OpRoute, Owned
from pynixd.serde import IsValidPathRequest, IsValidPathResponse, QueryPathInfoRequest, QueryPathInfoResponse, StorePath
from pynixd.store.local_db import LocalDBStore
from pynixd.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path

    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd.connection import ClientConn

HELLO = "/nix/store/00000000000000000000000000000001-hello"
STORE = StoreId("local")


def _store_db(root: Path) -> None:
    """A store database that holds `HELLO`."""
    db_path = root / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(db_path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ValidPaths (id INTEGER PRIMARY KEY, path TEXT UNIQUE NOT NULL, "
            "deriver TEXT, registrationTime INTEGER)",
        )
        conn.execute("INSERT INTO ValidPaths (id, path, registrationTime) VALUES (1, ?, 0)", (HELLO,))


def _store(root: Path, **routes: OpRoute) -> tuple[LocalDBStore, list[Any]]:
    """A store that never starts a daemon: its wire says every path is invalid."""
    store = LocalDBStore(LocalSocketStoreSpec(store_id=STORE, store_path=root, op_routes=routes))
    asked: list[Any] = []

    async def call(
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        asked.append(request)
        if isinstance(request, IsValidPathRequest):
            return IsValidPathResponse(valid=False)
        return QueryPathInfoResponse(valid=False)

    store.call = call  # type: ignore[method-assign]  # the daemon of this test
    return store, asked


def _count(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, {"store_id": STORE, **labels}) or 0.0


class TestTheConfig:
    def test_an_unknown_operation_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="NoSuchOp"):
            LocalSocketStoreSpec(op_routes={"NoSuchOp": OpRoute.pynixd})

    def test_a_store_without_a_database_answers_through_the_daemon(self) -> None:
        with pytest.raises(ValidationError, match="use_db"):
            LocalSocketStoreSpec(use_db=False, op_routes={"IsValidPath": OpRoute.pynixd})

    def test_a_route_to_an_operation_pynixd_cannot_answer_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no answer to AddSignatures"):
            _store(tmp_path, AddSignatures=OpRoute.pynixd)

    def test_an_answer_that_writes_has_no_shadow(self, tmp_path: Path) -> None:
        class Writes(LocalDBStore):
            OWNED: ClassVar[dict[int, Owned]] = {37: Owned("own_add_signatures", writes=True)}

        with pytest.raises(ValueError, match="cannot run beside the daemon"):
            Writes(
                LocalSocketStoreSpec(store_id=STORE, store_path=tmp_path, op_routes={"AddSignatures": OpRoute.shadow})
            )

    def test_writes_are_off_unless_a_deployment_turns_them_on(self) -> None:
        assert LocalSocketStoreSpec().pynixd_writes is False

    def test_a_route_to_an_answer_that_writes_needs_the_opt_in(self, tmp_path: Path) -> None:
        class Writes(LocalDBStore):
            OWNED: ClassVar[dict[int, Owned]] = {37: Owned("own_add_signatures", default=OpRoute.pynixd, writes=True)}

        routes = {"AddSignatures": OpRoute.pynixd}
        with pytest.raises(ValueError, match="pynixd_writes is off"):
            Writes(LocalSocketStoreSpec(store_id=STORE, store_path=tmp_path, op_routes=routes))
        on = Writes(LocalSocketStoreSpec(store_id=STORE, store_path=tmp_path, op_routes=routes, pynixd_writes=True))
        assert on.routes == {37: OpRoute.pynixd}

    def test_an_answer_that_writes_defaults_to_the_daemon_even_with_the_opt_in(self, tmp_path: Path) -> None:
        """Its declared default is `pynixd`; only a route set by hand takes it."""

        class Writes(LocalDBStore):
            OWNED: ClassVar[dict[int, Owned]] = {37: Owned("own_add_signatures", default=OpRoute.pynixd, writes=True)}

        store = Writes(LocalSocketStoreSpec(store_id=STORE, store_path=tmp_path, pynixd_writes=True))
        assert store.routes == {37: OpRoute.daemon}

    def test_the_defaults_keep_the_routes_of_before(self, tmp_path: Path) -> None:
        """The SQLite answers pynixd gives, and nothing more."""
        store, _ = _store(tmp_path)
        pynixd = {op: OpRoute.pynixd for op in (1, 6, 14, 23, 26, 29, 31, 32, 33, 41, 43)}
        assert store.routes == {
            **pynixd,
            7: OpRoute.daemon,
            38: OpRoute.daemon,
            39: OpRoute.daemon,
            44: OpRoute.daemon,
        }


@pytest.mark.anyio
class TestTheRoutes:
    async def test_pynixd_answers_without_the_daemon(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        store, asked = _store(tmp_path)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as store.db:
            answer = await store.execute(IsValidPathRequest(path=StorePath(path=HELLO)), skip_probe=True)
        assert answer.valid is True
        assert asked == []

    async def test_daemon_asks_the_daemon_although_pynixd_could_answer(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        store, asked = _store(tmp_path, IsValidPath=OpRoute.daemon)
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as store.db:
            answer = await store.execute(IsValidPathRequest(path=StorePath(path=HELLO)), skip_probe=True)
        assert answer.valid is False
        assert len(asked) == 1

    async def test_shadow_gives_the_daemons_answer_and_counts_a_mismatch(self, tmp_path: Path) -> None:
        """The daemon of the test says invalid and the database says valid."""
        _store_db(tmp_path)
        store, asked = _store(tmp_path, IsValidPath=OpRoute.shadow)
        before = _count("pynixd_op_shadow_total", op="IsValidPath", outcome="mismatch")
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as store.db:
            answer = await store.execute(IsValidPathRequest(path=StorePath(path=HELLO)), skip_probe=True)
        assert answer.valid is False
        assert len(asked) == 1
        assert _count("pynixd_op_shadow_total", op="IsValidPath", outcome="mismatch") == before + 1

    async def test_shadow_counts_a_match(self, tmp_path: Path) -> None:
        _store_db(tmp_path)
        store, _ = _store(tmp_path, IsValidPath=OpRoute.shadow)
        gone = StorePath(path="/nix/store/00000000000000000000000000000003-gone")
        before = _count("pynixd_op_shadow_total", op="IsValidPath", outcome="match")
        async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as store.db:
            await store.execute(IsValidPathRequest(path=gone), skip_probe=True)
        assert _count("pynixd_op_shadow_total", op="IsValidPath", outcome="match") == before + 1

    async def test_an_inactive_database_falls_through_and_counts_it(self, tmp_path: Path) -> None:
        """A store whose database pynixd cannot read: an overlay store, or no database.

        `QueryPathInfo` raised "Database not active" here, where it should
        fall back: only `IsValidPath` checked the database before it read.
        """
        store, asked = _store(tmp_path)
        assert not store.db.active
        before = _count("pynixd_op_fallthrough_total", op="QueryPathInfo")
        answer = await store.execute(QueryPathInfoRequest(path=StorePath(path=HELLO)), skip_probe=True)
        assert answer.valid is False
        assert len(asked) == 1
        assert _count("pynixd_op_fallthrough_total", op="QueryPathInfo") == before + 1

    async def test_a_route_set_by_hand_needs_the_database(self, tmp_path: Path) -> None:
        store, _ = _store(tmp_path, QueryPathInfo=OpRoute.pynixd)
        with pytest.raises(RuntimeError, match="QueryPathInfo need the store database"):
            store._refuse_routes_without_a_database()
