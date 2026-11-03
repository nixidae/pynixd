"""A substitute is checked against `trusted-public-keys` unless its store is trusted.

Nix copies a substitute with `CheckSigs` unless the store is marked trusted
(`substitution-goal.cc:236`), and a store is not trusted without `?trusted=1`
(`store-api.hh:275`). User's decision: a builder or peer store in the
configuration stays trusted, because its outputs are unsigned.
nixidae/nixidae#1008.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.config import ExternalUnixStoreSpec, HTTPBinaryCacheSpec, PynixdSettings
from pynixd.store_url import substituter_spec


def _trusted(url: str) -> bool:
    spec = substituter_spec(url, PynixdSettings())
    assert spec is not None
    return spec.to_store(str(spec.store_id)).trusted


@pytest.mark.parametrize(
    ("url", "trusted"),
    [
        ("https://cache.example", False),
        ("https://cache.example?trusted=1", True),
        ("unix:///run/peer/socket", False),
        ("unix:///run/peer/socket?trusted=true", True),
    ],
)
def test_a_substituter_url_is_trusted_only_when_it_says_so(url: str, trusted: bool) -> None:
    assert _trusted(url) is trusted


def test_a_cache_in_the_configuration_is_checked() -> None:
    assert not HTTPBinaryCacheSpec(store_id=StoreId("cache"), url="https://cache.example").trusted


def test_a_peer_in_the_configuration_is_trusted() -> None:
    assert ExternalUnixStoreSpec(store_id=StoreId("peer")).trusted


def test_a_unix_substituter_ranks_as_nix_does() -> None:
    """`priority` defaults to 0 (`store-api.hh:286`); only a cache takes another from `nix-cache-info`."""
    spec = substituter_spec("unix:///run/peer/socket", PynixdSettings())
    assert spec is not None
    assert spec.priority == 0
