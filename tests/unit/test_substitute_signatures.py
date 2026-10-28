"""A substitute from an HTTP cache is checked against `trusted-public-keys`.

Nix copies a substitute with `CheckSigs` unless the store is marked trusted
(`substitution-goal.cc:236`). pynixd imported every substitute with
`dont_check_sigs=1`, so a cache could place an unsigned path in the store.
User's decision: a builder or peer store stays trusted, because its outputs
are unsigned. nixidae/nixidae#1008.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from nix_daemon_protocol.ids import StoreId
from pynixd.config import HTTPBinaryCacheSpec
from pynixd.store.http_binary_cache import HTTPBinaryCacheStore
from pynixd.substitution_queue import _trusted_source  # noqa: PLC2701 -- the decision is the unit


def _cache(*, trusted: bool) -> HTTPBinaryCacheStore:
    return HTTPBinaryCacheStore(
        HTTPBinaryCacheSpec(store_id=StoreId("cache"), url="https://cache.example", trusted=trusted)
    )


def test_an_http_cache_is_checked() -> None:
    assert not _trusted_source(_cache(trusted=False))


def test_an_http_cache_marked_trusted_is_not() -> None:
    assert _trusted_source(_cache(trusted=True))


def test_a_builder_or_peer_store_is_trusted() -> None:
    assert _trusted_source(cast("Any", SimpleNamespace(store_id=StoreId("sg1-n1"))))
