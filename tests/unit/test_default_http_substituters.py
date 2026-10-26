"""pynixd's HTTP substituters are the ones nix.conf names, as Nix's are.

A local store without a `nix_config` defers to its daemon's nix.conf, which
`Server.start` reads. In a given `nix_config`, an unset `substituters` is
Nix's default, cache.nixos.org. A set one, even an empty one, is exactly
that list: a node without a network that lists only a local cache must not
wait on DNS for cache.nixos.org on every miss.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from pynixd.instance import _add_substituter_stores, _configured_substituters, _default_http_substituter_urls
from pynixd.store import is_http_binary_cache

LOCAL = "http://10.254.0.1:5000"


def _store(substituters: list[str] | None) -> Any:
    return SimpleNamespace(nix_config=SimpleNamespace(substituters=substituters))


def test_no_nix_config_defers_to_the_daemon() -> None:
    bare: Any = SimpleNamespace(nix_config=None)
    assert _configured_substituters(bare) is None


def test_unset_is_nix_default() -> None:
    assert _configured_substituters(_store(None)) == ["https://cache.nixos.org/"]


def test_set_is_exactly_that() -> None:
    assert _default_http_substituter_urls(_configured_substituters(_store([LOCAL])) or []) == [LOCAL]


def test_empty_is_none() -> None:
    assert _default_http_substituter_urls([]) == []


def test_non_http_and_duplicates_drop() -> None:
    urls = [LOCAL, f"{LOCAL}/", "ssh-ng://nix@pynixd", "https://cache.nixos.org/"]
    assert _default_http_substituter_urls(urls) == [LOCAL, "https://cache.nixos.org/"]


def test_the_daemons_list_adds_only_what_is_missing() -> None:
    stores: dict[Any, Any] = {}
    added = _add_substituter_stores(stores, [LOCAL])
    assert [store.url.rstrip("/") for store in added if is_http_binary_cache(store)] == [LOCAL]
    assert len(added) == 1
    assert _add_substituter_stores(stores, [f"{LOCAL}/"]) == []
