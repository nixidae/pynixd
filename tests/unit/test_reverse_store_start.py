"""A reverse store keeps one health monitor, however often it starts, and its close ends the builder's connection.

A monitor per start left one that `close()` never stopped, and it sampled
the closed connection for ever. Issue nixidae/nixidae#1065.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import ReverseStoreSpec
from pynixd.store import reverse
from pynixd.store.daemon import DaemonStore
from pynixd.store.reverse import ReverseStore


class _Connection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _store(name: str, conn: _Connection) -> ReverseStore:
    return ReverseStore(ReverseStoreSpec(store_id=StoreId(name)), conn)  # type: ignore[arg-type]


class _CountedMonitor:
    instances: ClassVar[list[_CountedMonitor]] = []

    def __init__(self, *_args: Any) -> None:
        self.running = False
        _CountedMonitor.instances.append(self)

    def start(self, _background: object) -> None:
        self.running = True

    def stop(self) -> None:
        self.running = False


async def test_a_second_start_keeps_one_monitor_and_close_stops_it(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _started(self: DaemonStore, sync_paths: bool = True) -> None:
        del self, sync_paths

    _CountedMonitor.instances.clear()
    monkeypatch.setattr(DaemonStore, "start", _started)
    monkeypatch.setattr(reverse, "PeerHealthMonitor", _CountedMonitor)
    store = _store("start-twice", _Connection())

    await store.start()
    await store.start()
    assert len(_CountedMonitor.instances) == 1

    await store.close()
    assert not any(monitor.running for monitor in _CountedMonitor.instances)


async def test_a_closed_store_closes_the_builders_connection() -> None:
    """A store that a failed loop closed left its builder waiting on a
    connection nobody served, so the builder never registered again."""
    conn = _Connection()
    store = _store("closes-conn", conn)
    await store.close()
    assert conn.closed
