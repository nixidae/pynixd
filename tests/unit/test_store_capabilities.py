"""Extension-op support is decided from handshake features. Issue #82.

The codebase's idiom is `"Name" in store.features`: extension op names
ride the handshake capability list, the peer's set is cached per store,
and a store that never handshook -- an HTTP cache -- advertises nothing.
"""

from __future__ import annotations

from pynixd.store import LocalSocketStore
from tests.conftest import make_test_spec


def test_daemon_store_without_handshake_advertises_nothing() -> None:
    # A store with no daemon offers features with no handshake, so `PYNIXD_NO_DAEMON` must not reach this one.
    store = LocalSocketStore(make_test_spec(store_id="cap", daemon=True))
    assert store.features == set()
    assert "PynixdState" not in store.features


def test_daemon_store_reflects_the_handshake_cache() -> None:
    store = LocalSocketStore(make_test_spec(store_id="cap"))
    store._features = {"PynixdState", "PynixdCollectGarbage"}
    assert "PynixdState" in store.features
    assert "PynixdCollectGarbage" in store.features
    assert "PynixdState2" not in store.features
