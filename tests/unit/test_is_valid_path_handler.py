"""`IsValidPath` (op 1) answers through `DaemonProxy.is_valid_path`, as the generic `execute` would.

The handler skips `DaemonProxy.execute` and `LocalDBStore.execute`: 4.3 us of each 33.5 us answer, and an
evaluation of one NixOS system sends 49193 (nixidae/nixidae#1345). It must answer what they answer: the
store's database on the pynixd route, a builder store's output as valid, `execute` on any other route. It
still records the path for the collector's access table, as `execute` does.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.context import WriteContext
from nix_daemon_protocol.io import BytesWriter
from pynixd.op_routes import OpRoute
from pynixd.proxy import DaemonProxy
from pynixd.serde import IsValidPathRequest, IsValidPathResponse, StorePath
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout
from tests.unit.test_sync_reader import GONE, HELLO, _store, _store_db

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from nix_daemon_protocol.wire_ops import WireResponse
    from pynixd.store.local_db import LocalDBStore


class _Daemon:
    """The daemon half of the store: probed already, and never asked."""

    async def probe(self) -> None:
        return None

    async def execute(self, request: Any, **_: Any) -> Any:
        raise AssertionError(f"the daemon answered {type(request).__name__}")


async def _encode(response: WireResponse) -> bytes:
    writer = BytesWriter()
    await response.to_writer(WriteContext(writer=writer, version=IsValidPathRequest.min_protocol))
    return writer.bytes()


@pytest.fixture
async def store(tmp_path: Path) -> AsyncIterator[LocalDBStore]:
    _store_db(tmp_path)
    async with await LocalStoreDB.open(StoreLayout.chroot(tmp_path)) as db:
        local = _store(db, tmp_path)
        local.routes = {IsValidPathRequest.op: OpRoute.pynixd}
        local._daemon = cast("Any", _Daemon())
        try:
            yield local
        finally:
            for reader in _READERS:
                reader.close()
            _READERS.clear()


_READERS: list[Any] = []


def _proxy(store: LocalDBStore, built: frozenset[str] = frozenset()) -> tuple[DaemonProxy, list[Any]]:
    """A proxy over *store*, whose builder stores hold *built*; the list collects what reached `execute`."""
    proxy = object.__new__(DaemonProxy)
    executed: list[Any] = []
    reader = store.db.sync_reader()
    if reader is not None:
        _READERS.append(reader)
    proxy.ctx = cast(
        "Any", SimpleNamespace(local_store=store, store_for_output_path=lambda p: p if p in built else None)
    )
    proxy.client = cast("Any", SimpleNamespace(sync_reader=reader))

    async def execute(request: Any) -> Any:
        executed.append(request)
        return IsValidPathResponse.fast(valid=False)

    proxy.execute = execute  # type: ignore[method-assign]  # the generic route, observed
    return proxy, executed


def _valid(response: WireResponse) -> bool:
    return cast("IsValidPathResponse", response).valid


def _request(path: str) -> IsValidPathRequest:
    return IsValidPathRequest(path=StorePath(path=path))


@pytest.mark.anyio
class TestTheHandlerAnswers:
    async def test_a_path_the_store_holds(self, store: LocalDBStore) -> None:
        proxy, executed = _proxy(store)
        assert _valid(await proxy.is_valid_path(_request(HELLO))) is True
        assert executed == []

    async def test_a_path_the_store_does_not_hold(self, store: LocalDBStore) -> None:
        proxy, executed = _proxy(store)
        assert _valid(await proxy.is_valid_path(_request(GONE))) is False
        assert executed == []

    async def test_a_path_a_builder_store_holds(self, store: LocalDBStore) -> None:
        """`_mapped_output_response`: an output a builder holds is valid."""
        proxy, _ = _proxy(store, built=frozenset({GONE}))
        assert _valid(await proxy.is_valid_path(_request(GONE))) is True

    async def test_the_path_reaches_the_access_table(self, store: LocalDBStore) -> None:
        proxy, _ = _proxy(store)
        await proxy.is_valid_path(_request(HELLO))
        assert HELLO in store.db.pending_references

    @pytest.mark.parametrize("path", [HELLO, GONE])
    async def test_the_bytes_match_the_generic_route(self, store: LocalDBStore, path: str) -> None:
        proxy, _ = _proxy(store)
        generic = await store.execute(_request(path), client=proxy.client)
        assert await _encode(await proxy.is_valid_path(_request(path))) == await _encode(generic)


@pytest.mark.anyio
class TestExecuteAnswersOtherwise:
    @pytest.mark.parametrize("route", [OpRoute.daemon, OpRoute.shadow])
    async def test_a_route_other_than_pynixd(self, store: LocalDBStore, route: OpRoute) -> None:
        store.routes = {IsValidPathRequest.op: route}
        proxy, executed = _proxy(store)
        await proxy.is_valid_path(_request(HELLO))
        assert [r.path for r in executed] == [StorePath(path=HELLO)]

    async def test_an_inactive_database(self, store: LocalDBStore, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(type(store.db), "active", property(lambda _: False))
        proxy, executed = _proxy(store)
        await proxy.is_valid_path(_request(HELLO))
        assert len(executed) == 1
