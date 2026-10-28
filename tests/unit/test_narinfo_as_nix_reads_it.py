"""A narinfo and a copied path read as Nix 2.35.3 reads them. nixidae/nixidae#1008."""

from __future__ import annotations

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd.config import HTTPBinaryCacheSpec
from pynixd.store.http_binary_cache import (  # noqa: PLC2701 -- the parser is the unit
    HTTPBinaryCacheStore,
    _parse_narinfo,
)
from pynixd.store.transfer import as_copied

NARINFO = """StorePath: /nix/store/00000000000000000000000000000001-hello
URL: nar/abc.nar.bz2
FileSize: 100
NarHash: sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s
NarSize: 200
References:
Deriver: unknown-deriver
"""


def test_no_compression_line_means_bzip2() -> None:
    """`nar-info.cc:122`: an absent `Compression:` is bzip2, not none."""
    info = _parse_narinfo(NARINFO, expected_path=None)

    assert info is not None
    assert info.compression == "bzip2"


def test_unknown_deriver_names_none() -> None:
    """`nar-info.cc:75`."""
    assert ValidPathInfo.from_narinfo(NARINFO).info.deriver is None


def test_a_narinfo_without_nar_hash_is_corrupt() -> None:
    """`nar-info.cc:97-107`: no NarHash, or NarSize 0, names no NAR to check."""
    assert (
        _parse_narinfo(
            NARINFO.replace("NarHash: sha256:1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s\n", ""), None
        )
        is None
    )
    assert _parse_narinfo(NARINFO.replace("NarSize: 200", "NarSize: 0"), None) is None


def test_a_copy_is_never_ultimate() -> None:
    """`daemon.cc:516` registers every copied path with `ultimate = false`."""
    info = ValidPathInfo.from_narinfo(NARINFO)
    built_here = info.model_copy(update={"info": info.info.model_copy(update={"ultimate": True})})

    assert as_copied(built_here).info.ultimate is False


def test_the_nar_url_stays_under_the_cache() -> None:
    """Nix fetches `<cache>/<url>`; `urljoin` followed a `//host` URL out of the cache."""
    store = HTTPBinaryCacheStore(HTTPBinaryCacheSpec(store_id=StoreId("cache"), url="https://cache.example/sub"))
    info = _parse_narinfo(NARINFO.replace("URL: nar/abc.nar.bz2", "URL: //evil.example/x.nar"), None)

    assert info is not None
    assert store.nar_url(info) == "https://cache.example/sub/evil.example/x.nar"
