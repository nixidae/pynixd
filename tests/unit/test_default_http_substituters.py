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

from pynixd.config import PynixdSettings
from pynixd.instance import _add_substituter_stores, _configured_substituters
from pynixd.store import is_http_binary_cache

LOCAL = "http://10.254.0.1:5000"


def _store(substituters: list[str] | None) -> Any:
    return SimpleNamespace(nix_config=SimpleNamespace(substituters=substituters))


def test_no_nix_config_defers_to_the_daemon() -> None:
    bare: Any = SimpleNamespace(nix_config=None)
    assert _configured_substituters(bare) is None


def test_unset_is_nix_default() -> None:
    assert _configured_substituters(_store(None)) == ["https://cache.nixos.org/"]


def _urls(configured: list[str]) -> list[str]:
    added = _add_substituter_stores({}, configured, PynixdSettings())
    return [store.url.rstrip("/") for store in added if is_http_binary_cache(store)]


def test_set_is_exactly_that() -> None:
    assert _urls(list(_configured_substituters(_store([LOCAL])) or [])) == [LOCAL]


def test_empty_is_none() -> None:
    assert _urls([]) == []


def test_unopened_schemes_and_duplicates_drop() -> None:
    urls = [LOCAL, f"{LOCAL}/", "ssh://nix@pynixd", "https://cache.nixos.org/"]
    assert _urls(urls) == [LOCAL, "https://cache.nixos.org"]


def test_the_daemons_list_adds_only_what_is_missing() -> None:
    stores: dict[Any, Any] = {}
    added = _add_substituter_stores(stores, [LOCAL], PynixdSettings())
    assert [store.url.rstrip("/") for store in added if is_http_binary_cache(store)] == [LOCAL]
    assert len(added) == 1
    assert _add_substituter_stores(stores, [f"{LOCAL}/"], PynixdSettings()) == []


def test_its_own_cache_is_no_substituter() -> None:
    """A miss there waits on itself for `substitution_query_timeout`. nixidae/nixidae#1125."""
    settings = PynixdSettings(http_port=8080)
    stores: dict[Any, Any] = {}
    added = _add_substituter_stores(stores, ["http://127.0.0.1:8080", "http://[::1]:8080/", LOCAL], settings)
    assert [store.url.rstrip("/") for store in added if is_http_binary_cache(store)] == [LOCAL]
    assert len(added) == 1


def test_another_port_on_loopback_is_a_substituter() -> None:
    settings = PynixdSettings(http_port=8080)
    added = _add_substituter_stores({}, ["http://127.0.0.1:5000"], settings)
    assert [store.url.rstrip("/") for store in added if is_http_binary_cache(store)] == ["http://127.0.0.1:5000"]


def test_url_parameters_reach_the_backend() -> None:
    """Nix reads `?trusted=1` and `?priority=` off a substituter's URL; the cache's own URL has no query."""
    stores: dict[Any, Any] = {}
    added = _add_substituter_stores(stores, [f"{LOCAL}/?trusted=1&priority=10"], PynixdSettings())
    caches = [store for store in added if is_http_binary_cache(store)]
    assert [(store.url.rstrip("/"), store.trusted, store.priority) for store in caches] == [(LOCAL, True, 10.0)]
    assert len(added) == 1
    assert _add_substituter_stores(stores, [f"{LOCAL}?priority=10&trusted=1"], PynixdSettings()) == []


def test_a_parameter_the_registry_does_not_read_is_left_to_the_daemon() -> None:
    """pynixd never drops a setting the user wrote: `store_url`."""
    assert _add_substituter_stores({}, [f"{LOCAL}?compression=zstd"], PynixdSettings()) == []
