"""A `file://` substituter reads as Nix's `LocalBinaryCacheStore` reads it."""

from __future__ import annotations

import hashlib
import json
import lzma
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.store_dir import store_dir
from pynixd.config import LocalBinaryCacheSpec, PynixdSettings
from pynixd.exceptions import CacheFault, NarGone, StoreDirMismatch
from pynixd.instance import _add_substituter_stores
from pynixd.serde import DrvOutput
from pynixd.store.local_binary_cache import LocalBinaryCacheStore
from pynixd.store_url import spec_for
from pynixd.substitution_queue import SubstitutionQueue
from pynixd_store.content_address import ContentAddress, Method, content_addressed_path
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
    spec = spec_for("file:///srv/cache?trusted=1&priority=10")
    assert isinstance(spec, LocalBinaryCacheSpec)
    assert str(spec.path) == "/srv/cache"
    assert spec.trusted
    assert spec.priority == 10
    assert spec_for("file://relative") is None


def test_a_store_parameter_names_the_caches_store_directory() -> None:
    """`?store=` is `StoreDirConfig::storeDir`, and another one is another store (`StoreConfig::getReference`)."""
    spec = spec_for("file:///srv/cache?store=/fnord/store")
    plain = spec_for("file:///srv/cache")
    assert isinstance(spec, LocalBinaryCacheSpec)
    assert plain is not None
    assert spec.store_dir == "/fnord/store"
    assert spec.reference_key() != plain.reference_key()
    same = spec_for("file:///srv/cache?store=/nix/store/")
    assert same is not None
    assert same.reference_key() == plain.reference_key()
    assert spec_for("file:///srv/cache?store=fnord") is None


def _fnord_cache(root: Path) -> Path:
    _cache(root)
    (root / "nix-cache-info").write_text("StoreDir: /fnord/store\n")
    (root / f"{PATH.hash_part()}.narinfo").write_text(
        _narinfo("nar/abc.nar.xz", "xz").replace(str(PATH), f"/fnord/store/{PATH.name}")
    )
    return root


async def test_a_cache_of_another_store_directory_reads_its_narinfo(tmp_path: Path) -> None:
    """`NarInfo` parses `StorePath:` against the cache's own store directory (`nar-info.cc:50`)."""
    root = _fnord_cache(tmp_path / "cache")
    store = spec_for(f"file://{root}?store=/fnord/store")
    assert store is not None
    cache = store.to_store("fnord")
    await cache.start()
    assert isinstance(cache, LocalBinaryCacheStore)
    narinfo = await cache.get_narinfo(PATH)
    assert narinfo is not None
    assert narinfo.path == PATH
    assert narinfo.valid_path_info.path == PATH


async def test_a_cache_of_another_store_directory_does_not_open_as_ours(tmp_path: Path) -> None:
    """`BinaryCacheStore::init` (`binary-cache-store.cc:58`) refuses a `StoreDir` that is not the store's."""
    root = _fnord_cache(tmp_path / "cache")
    with pytest.raises(StoreDirMismatch, match="is for Nix stores with prefix '/fnord/store', not '/nix/store'"):
        await _store(root)


def test_a_readable_directory_gets_a_backend(tmp_path: Path) -> None:
    root = _cache(tmp_path / "cache")
    stores: dict[Any, Any] = {}
    added = _add_substituter_stores(stores, [f"file://{root}?trusted=1"], PynixdSettings())
    caches = [store for store in added if isinstance(store, LocalBinaryCacheStore)]
    assert [store.path for store in caches] == [root]
    assert caches[0].trusted
    assert _add_substituter_stores(stores, [f"file://{root}/?trusted=1"], PynixdSettings()) == []


def test_another_trust_is_another_store(tmp_path: Path) -> None:
    """Nix opens one store per URL and its parameters (`StoreConfig::getReference`)."""
    root = _cache(tmp_path / "cache")
    stores: dict[Any, Any] = {}
    _add_substituter_stores(stores, [f"file://{root}?trusted=1"], PynixdSettings())
    (added,) = _add_substituter_stores(stores, [f"file://{root}"], PynixdSettings())
    assert isinstance(added, LocalBinaryCacheStore)
    assert not added.trusted


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


FILE_CA = ContentAddress(Method.FLAT, "sha256", hashlib.sha256(b"a fixed output").digest())
HERE = StorePath(content_addressed_path(FILE_CA, "file", (), store_dir()))
THERE = StorePath.in_store(content_addressed_path(FILE_CA, "file", (), "/fnord/store"), "/fnord/store")


async def _fnord_queue(tmp_path: Path, references: str = "") -> tuple[SubstitutionQueue, LocalBinaryCacheStore]:
    """A queue over a `/fnord/store` cache that holds `FILE_CA` under the name it has there."""
    root = _fnord_cache(tmp_path / "cache")
    (root / f"{THERE.hash_part()}.narinfo").write_text(
        f"StorePath: /fnord/store/{THERE.name}\nURL: nar/abc.nar.xz\nCompression: xz\n"
        "NarHash: sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s\n"
        f"NarSize: {len(NAR)}\nReferences: {references}\nCA: {FILE_CA.render()}\n"
    )
    spec = spec_for(f"file://{root}?store=/fnord/store")
    assert spec is not None
    cache = spec.to_store("fnord")
    assert isinstance(cache, LocalBinaryCacheStore)
    await cache.start()
    queue = SubstitutionQueue(cast("PynixdContext", SimpleNamespace(settings=PynixdSettings(), stores={})))
    return queue, cache


async def test_a_cache_of_another_store_directory_holds_a_content_address_under_its_own_name(tmp_path: Path) -> None:
    """`store-api.cc:461`: the path is made again for the substituter's store directory."""
    queue, cache = await _fnord_queue(tmp_path)
    assert HERE != THERE
    candidate = await queue.get_substituter(HERE, [cache], ca=FILE_CA)
    assert candidate is not None
    assert candidate.path_info.path == HERE
    assert candidate.store_path == THERE


async def test_a_cache_of_another_store_directory_has_no_path_without_its_address(tmp_path: Path) -> None:
    """`store-api.cc:471`: a path with no content address is not looked for there."""
    queue, cache = await _fnord_queue(tmp_path)
    assert await queue.get_substituter(HERE, [cache]) is None


async def test_a_cache_of_another_store_directory_has_no_path_with_references(tmp_path: Path) -> None:
    """`store-api.cc:481`: across store directories only a content-addressed path with no references."""
    queue, cache = await _fnord_queue(tmp_path, references=PATH.name)
    assert await queue.get_substituter(HERE, [cache], ca=FILE_CA) is None


async def test_a_daemon_cache_ranks_by_its_nix_cache_info_unless_a_priority_is_given(tmp_path: Path) -> None:
    """`binary-cache-store.cc:67`: `Priority:` sets the default; `?priority=` or the spec wins."""
    plain, named = _cache(tmp_path / "plain"), _cache(tmp_path / "named")
    added = _add_substituter_stores({}, [f"file://{plain}", f"file://{named}?priority=10"], PynixdSettings())
    configured = LocalBinaryCacheSpec(path=_cache(tmp_path / "configured"), priority=5).to_store("configured")
    for store in (*added, configured):
        await store.start()
    assert [store.priority for store in (*added, configured)] == [30.0, 10.0, 5.0]


DRV = StorePath("/nix/store/00000000000000000000000000000003-thing.drv")
DRV_OUTPUT = DrvOutput(f"sha256:{'ab' * 32}!out")


@pytest.mark.parametrize(
    ("by_path", "name", "body"),
    [
        # Nix 2.34 writes the keyed form (`binary-cache-store.cc:554`); 2.35 the unkeyed one (`:711`).
        (False, f"realisations/{DRV_OUTPUT}.doi", {"id": str(DRV_OUTPUT), "dependentRealisations": {}}),
        (True, f"build-trace-v2/{DRV.name}/out.doi", {}),
    ],
)
async def test_a_realisation_reads_from_the_layout_of_the_local_nix(
    tmp_path: Path, by_path: bool, name: str, body: dict[str, Any]
) -> None:
    root = _cache(tmp_path / "cache")
    doi = root / name
    doi.parent.mkdir(parents=True, exist_ok=True)
    doi.write_text(json.dumps({"outPath": PATH.name, "signatures": ["k:b", "k:a"], **body}))
    store = await _store(root)
    found = await store.find_realisation(DRV_OUTPUT, DRV, by_path=by_path)
    assert found is not None
    assert (found.id, str(found.out_path), found.signatures) == (DRV_OUTPUT, str(PATH), ["k:a", "k:b"])
    assert await store.find_realisation(DRV_OUTPUT, DRV, by_path=not by_path) is None


async def test_a_realisation_the_cache_cannot_parse_is_a_fault(tmp_path: Path) -> None:
    """`binary-cache-store.cc:538` throws, and the goal counts the substituter as failed."""
    root = _cache(tmp_path / "cache")
    (root / "realisations").mkdir()
    (root / f"realisations/{DRV_OUTPUT}.doi").write_text("{not json")
    store = await _store(root)
    with pytest.raises(CacheFault):
        await store.find_realisation(DRV_OUTPUT, DRV, by_path=False)
