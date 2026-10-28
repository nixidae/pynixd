"""A reverse store keeps one health monitor, however often it starts.

Registration starts the store before its probe, and `Server.add_store`
starts it again. A monitor per start left one that `close()` never
stopped, and it sampled the closed connection for ever. Issue
nixidae/nixidae#1065.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import ReverseStoreSpec
from pynixd.store import reverse
from pynixd.store.daemon import DaemonStore
from pynixd.store.reverse import ReverseStore


class _CountedMonitor:
    instances: ClassVar[list[_CountedMonitor]] = []

    def __init__(self, *_args: Any) -> None:
        self.running = False
        _CountedMonitor.instances.append(self)

    def start(self) -> None:
        self.running = True

    async def stop(self) -> None:
        self.running = False


async def test_a_second_start_keeps_one_monitor_and_close_stops_it(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _started(self: DaemonStore, sync_paths: bool = True) -> None:
        del self, sync_paths

    async def _no_ssh(self: ReverseStore) -> None:
        del self

    _CountedMonitor.instances.clear()
    monkeypatch.setattr(DaemonStore, "start", _started)
    monkeypatch.setattr(ReverseStore, "close_ssh", _no_ssh)
    monkeypatch.setattr(reverse, "PeerHealthMonitor", _CountedMonitor)
    store = ReverseStore(ReverseStoreSpec(store_id=StoreId("start-twice")), None)  # type: ignore[arg-type]

    await store.start()
    await store.start()
    assert len(_CountedMonitor.instances) == 1

    await store.close()
    assert not any(monitor.running for monitor in _CountedMonitor.instances)
