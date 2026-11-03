"""A `file://` substituter reads as Nix's `LocalBinaryCacheStore` reads it."""

from __future__ import annotations

import lzma
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynixd.config import LocalBinaryCacheSpec, PynixdSettings
from pynixd.exceptions import NarGone
from pynixd.instance import _add_substituter_stores
from pynixd.store.local_binary_cache import LocalBinaryCacheStore
from pynixd.substituters import file_cache
from pynixd.substitution_queue import SubstitutionQueue
from pynixd_store.store_path import StorePath

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.context import PynixdContext

PATH = StorePath("/nix/store/00000000000000000000000000000001-hello")
NAR = b"not a real NAR, but the bytes the cache holds" * 1000


def _narinfo(url: str, compression: str) -> str:
    return (
        f"StorePath: {PATH}\n"
        f"URL: {url}\n"
        f"Compression: {compression}\n"
        "NarHash: sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s\n"
        f"NarSize: {len(NAR)}\n"
        "References: \n"
    )


def _cache(root: Path, compression: str = "xz") -> Path:
    root.mkdir()
    (root / "nar").mkdir()
    (root / "nix-cache-info").write_text("StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 30\n")
    body = lzma.compress(NAR) if compression == "xz" else NAR
    suffix = ".xz" if compression == "xz" else ""
    (root / f"nar/abc.nar{suffix}").write_bytes(body)
    (root / f"{PATH.hash_part()}.narinfo").write_text(_narinfo(f"nar/abc.nar{suffix}", compression))
    return root


async def _store(root: Path) -> LocalBinaryCacheStore:
    store = LocalBinaryCacheSpec(path=root).to_store("file-cache")
    await store.start()
    return store


@pytest.mark.parametrize("compression", ["xz", "none"])
async def test_the_nar_streams_decompressed(tmp_path: Path, compression: str) -> None:
    store = await _store(_cache(tmp_path / "cache", compression))
    narinfo = await store.get_narinfo(PATH)
    assert narinfo is not None
    assert b"".join([piece async for piece in store.stream_nar(narinfo)]) == NAR


async def test_nix_cache_info_is_read(tmp_path: Path) -> None:
    store = await _store(_cache(tmp_path / "cache"))
    assert store.cache_info["WantMassQuery"] == "1"
    assert store.cache_info["Priority"] == "30"


async def test_a_cache_without_nix_cache_info_takes_nix_defaults(tmp_path: Path) -> None:
    root = _cache(tmp_path / "cache")
    (root / "nix-cache-info").unlink()
    store = await _store(root)
    assert store.cache_info["WantMassQuery"] == "0"


async def test_a_missing_narinfo_is_a_miss(tmp_path: Path) -> None:
    store = await _store(_cache(tmp_path / "cache"))
    assert await store.get_narinfo(StorePath("/nix/store/00000000000000000000000000000002-other")) is None


async def test_a_missing_nar_is_gone(tmp_path: Path) -> None:
    """Nix treats that as no substitute (`substitution-goal.cc:270`), not as a fault."""
    root = _cache(tmp_path / "cache")
    (root / "nar/abc.nar.xz").unlink()
    store = await _store(root)
    narinfo = await store.get_narinfo(PATH)
    assert narinfo is not None
    with pytest.raises(NarGone):
        _ = [piece async for piece in store.stream_nar(narinfo)]


def test_a_file_url_names_its_directory_and_parameters() -> None:
    cache = file_cache("file:///srv/cache?trusted=1&priority=10")
    assert cache is not None
    assert str(cache.path) == "/srv/cache"
    assert cache.trusted
    assert cache.priority == 10
    assert file_cache("https://cache.example/") is None
    assert file_cache("file://relative") is None


def test_a_readable_directory_gets_a_backend(tmp_path: Path) -> None:
    root = _cache(tmp_path / "cache")
    stores: dict[Any, Any] = {}
    added = _add_substituter_stores(stores, [f"file://{root}?trusted=1"], PynixdSettings())
    caches = [store for store in added if isinstance(store, LocalBinaryCacheStore)]
    assert [store.path for store in caches] == [root]
    assert caches[0].trusted
    assert _add_substituter_stores(stores, [f"file://{root}"], PynixdSettings()) == []


def test_an_unreadable_directory_is_left_to_the_daemon(tmp_path: Path) -> None:
    """pynixd may not run as the daemon's user. Its paths must not read as missing."""
    root = _cache(tmp_path / "cache")
    root.chmod(0)
    try:
        stores: dict[Any, Any] = {}
        assert _add_substituter_stores(stores, [f"file://{root}"], PynixdSettings()) == []
        queue = SubstitutionQueue(cast("PynixdContext", SimpleNamespace(settings=PynixdSettings(), stores=stores)))
        queue.note_daemon_substituters([f"file://{root}"])
        assert queue.uncovered == (f"file://{root}",)
    finally:
        root.chmod(0o755)


def test_a_directory_with_a_backend_is_covered(tmp_path: Path) -> None:
    root = _cache(tmp_path / "cache")
    stores: dict[Any, Any] = {}
    _add_substituter_stores(stores, [f"file://{root}"], PynixdSettings())
    queue = SubstitutionQueue(cast("PynixdContext", SimpleNamespace(settings=PynixdSettings(), stores=stores)))
    queue.note_daemon_substituters([f"file://{root}", "file:///elsewhere"])
    assert queue.uncovered == ("file:///elsewhere",)
