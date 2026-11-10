"""A builder whose health samples stopped gets no new build until one arrives.

On 2026-11-06 the vllm build went to gx10-1759 while its health stream was
silent, and sat there 6.5 min until the connection dropped. A silent peer
ranked as one pynixd cannot monitor, and that rank skips
`min_schedule_score`, so it beat a monitored store that was only busy.
nixidae/nixidae#1063.
"""

from __future__ import annotations

import json
from typing import Any

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.allocator import SILENT, BuildAllocator, TelemetryStoreRanker
from pynixd.config import PressureSettings, StoreRankingSettings
from pynixd.daemon_extensions.pynixd_host_health import PynixdHostHealthRequest, PynixdHostHealthResponse
from pynixd.monitor import PeerHealthMonitor
from pynixd.psi import SystemHealth
from tests._conftest.placement import FakeBuild, FakeMonitor, FakeStore, cpu

SILENT_PEER = StoreId("gx10-1759")
BUSY_PEER = StoreId("gx10-1a5f")


def _rank(*, silent: bool) -> tuple[list[StoreId], dict[StoreId, str]]:
    local = FakeStore(LOCAL_STORE_ID)
    local.no_schedule = True
    busy = FakeStore(BUSY_PEER)
    busy.cpu_util = cpu(100.0)
    stores = {
        LOCAL_STORE_ID: local,
        SILENT_PEER: FakeStore(SILENT_PEER, monitor=FakeMonitor(silent=silent)),
        BUSY_PEER: busy,
    }
    ranker = TelemetryStoreRanker(StoreRankingSettings(min_schedule_score=-100.0))
    ranked = BuildAllocator(stores, local, ranker).rank_stores(FakeBuild(), {}, running_builds={BUSY_PEER: 3})
    return [c.store_id for c in ranked], {e.store_id: e.reason for e in ranked.excluded}


def test_a_silent_peer_is_no_candidate() -> None:
    ranked, excluded = _rank(silent=True)
    assert ranked == []
    assert excluded[SILENT_PEER] == SILENT


def test_the_same_peer_with_no_figures_but_answering_still_builds() -> None:
    ranked, _ = _rank(silent=False)
    assert ranked == [SILENT_PEER]


async def test_a_failed_sample_makes_the_peer_silent_and_the_next_one_clears_it() -> None:
    payload = json.dumps({"files": {"/proc/loadavg": "0.5 0.4 0.3 1/100 1\n"}, "cores": 4.0})
    silent_after: list[bool] = []

    class Peer:
        store_id = "gx10-1759"
        features = frozenset({PynixdHostHealthRequest.name})
        calls = 0

        async def execute(self, _request: Any) -> Any:
            Peer.calls += 1
            if Peer.calls == 1:
                raise ConnectionError("SSH connection closed")
            monitor.running = False
            return PynixdHostHealthResponse(payload=payload)

    def on_sample(_health: SystemHealth) -> None:
        silent_after.append(monitor.silent)

    monitor = PeerHealthMonitor(PressureSettings(interval=0.0), on_sample, Peer())
    monitor.running = True
    await monitor.run()

    assert silent_after == [True, False]
