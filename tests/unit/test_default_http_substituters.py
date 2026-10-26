"""pynixd's HTTP substituters are the ones nix.conf names, as Nix's are.

An unset `substituters` is Nix's default, cache.nixos.org. A set one, even
an empty one, is exactly that list: a node without a network that lists
only a local cache must not wait on DNS for cache.nixos.org on every miss.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from pynixd.instance import _default_http_substituter_urls

LOCAL = "http://10.254.0.1:5000"


def _store(substituters: list[str] | None) -> Any:
    return SimpleNamespace(nix_config=SimpleNamespace(substituters=substituters))


def test_unset_is_nix_default() -> None:
    assert _default_http_substituter_urls(_store(None)) == ["https://cache.nixos.org/"]


def test_set_is_exactly_that() -> None:
    assert _default_http_substituter_urls(_store([LOCAL])) == [LOCAL]


def test_empty_is_none() -> None:
    assert _default_http_substituter_urls(_store([])) == []


def test_non_http_and_duplicates_drop() -> None:
    urls = [LOCAL, f"{LOCAL}/", "ssh-ng://nix@pynixd", "https://cache.nixos.org/"]
    assert _default_http_substituter_urls(_store(urls)) == [LOCAL, "https://cache.nixos.org/"]
