"""pynixd checks a path's signatures as `ValidPathInfo::checkSignatures` (`path-info.cc:125`) does.

The fixture is Nix's own: `nix key generate-secret --key-name test-1`, `nix
store add-file` of "hello\\n", `nix store sign`, then `nix path-info --json`
(Nix 2.35).
"""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.wire_time import Time
from pynixd.serde import ValidPathInfo
from pynixd.signing import MAX_SIGS, PublicKey, check_signatures, decode_base64, key_settings, public_keys
from pynixd.store_path import StorePath

if TYPE_CHECKING:
    from pathlib import Path

_SECRET = "test-1:/AtszAyNmI6F9kiY5RsCqJNdftINkIsUpy+iYUQgOqLJtwly27I9k2Gy2s6CLkNM5UmGxn/MXak60vHpXqXYjw=="
_PUBLIC = "test-1:ybcJctuyPZNhstrOgi5DTOVJhsZ/zF2pOtLx6V6l2I8="
_OTHER = "test-1:" + base64.b64encode(b"\x01" * 32).decode()
_PATH = "/nix/store/1p9q0bz6f22dyxh4lw8xs08p4201vyq4-f"
_CA = "fixed:sha256:00xyyr3fi8l6hb839bv3f7yb86yjv7xi1cgh1xnhipym4asvb4aq"
_NAR_HASH = base64.b64decode("HDfQGvQL4ugGkd48w99EN3ppmvuxfGjwgJZLL9Bx/BM=").hex()
_SIG = "test-1:Ju4wsgOW95Qiz8tPfYAPKGP0qh/uQ3tAePaS/b8WqomQD+a28i4GYraHqdwFs4zQkHLO/sqTpmQkaHyQXJCeCA=="


def _info(*, ca: str = "", sigs: tuple[str, ...] = (_SIG,), nar_size: int = 120) -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path=_PATH),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash=_NAR_HASH),
            references=set(),
            registration_time=Time(ts=0),
            nar_size=nar_size,
            ultimate=False,
            sigs={Signature(sig) for sig in sigs},
            ca=ContentAddress(value=ca),
        ),
    )


def _keys(*texts: str) -> dict[str, PublicKey]:
    return public_keys(texts, ())


def test_a_signature_by_a_trusted_key_counts() -> None:
    assert check_signatures(_info(), _keys(_PUBLIC), "/nix/store") == 1


def test_a_signature_by_another_key_of_that_name_does_not() -> None:
    assert check_signatures(_info(), _keys(_OTHER), "/nix/store") == 0


def test_a_signature_by_an_unknown_key_does_not() -> None:
    assert check_signatures(_info(), _keys(), "/nix/store") == 0


def test_a_signature_over_another_fingerprint_does_not() -> None:
    assert check_signatures(_info(nar_size=121), _keys(_PUBLIC), "/nix/store") == 0


def test_a_content_addressed_path_needs_no_signature() -> None:
    assert check_signatures(_info(ca=_CA, sigs=()), _keys(), "/nix/store") == MAX_SIGS


def test_a_false_content_address_warns_and_counts_the_signatures() -> None:
    warnings: list[str] = []
    false_ca = "fixed:sha256:" + "0" * 52
    assert check_signatures(_info(ca=false_ca), _keys(_PUBLIC), "/nix/store", warnings.append) == 1
    assert warnings == [f"warning: path '{_PATH}' claims to be content-addressed but isn't"]


def test_an_unknown_size_has_no_fingerprint() -> None:
    with pytest.raises(ValueError, match="because its size is not known"):
        check_signatures(_info(nar_size=0), _keys(_PUBLIC), "/nix/store")


def test_a_signature_of_the_wrong_length_is_refused() -> None:
    with pytest.raises(ValueError, match="signature is not valid"):
        check_signatures(_info(sigs=("test-1:AAAA",)), _keys(_PUBLIC), "/nix/store")


def test_the_first_key_of_a_name_wins() -> None:
    """`getDefaultPublicKeys` fills a `std::map` with `emplace`, which keeps the first."""
    assert check_signatures(_info(), _keys(_OTHER, _PUBLIC), "/nix/store") == 0
    assert check_signatures(_info(), _keys(_PUBLIC, _OTHER), "/nix/store") == 1


def test_a_secret_key_file_gives_its_public_half(tmp_path: Path) -> None:
    secret = tmp_path / "sk"
    secret.write_text(_SECRET + "\n")
    keys = public_keys((), (str(secret), str(tmp_path / "unreadable")))
    assert keys == _keys(_PUBLIC)


def test_a_bad_public_key_is_refused() -> None:
    with pytest.raises(ValueError, match="public key is not valid"):
        PublicKey.parse("test-1:AAAA")
    with pytest.raises(ValueError, match="key is corrupt"):
        PublicKey.parse("no-colon")


def test_base64_is_nix_s() -> None:
    """`base64::decode` stops at `=` and skips a newline; `b64decode` would drop the `*` too."""
    assert decode_base64("aGk=garbage") == b"hi"
    assert decode_base64("aG\nk") == b"hi"
    with pytest.raises(ValueError, match="invalid character in Base64 string: '\\*'"):
        decode_base64("aG*k")


def test_a_client_extends_or_replaces_the_keys() -> None:
    """`ClientSettings::apply` walks the overrides in name order: `extra-` first, then a plain name."""
    assert key_settings(["a"], [], {"extra-trusted-public-keys": "b c"}) == (["a", "b", "c"], [])
    assert key_settings(["a"], [], {"binary-cache-public-keys": "b"}) == (["b"], [])
    assert key_settings(["a"], [], {"extra-trusted-public-keys": "b", "trusted-public-keys": "c"}) == (["c"], [])
    assert key_settings([], ["f"], {"extra-secret-key-files": "g"}) == ([], ["f", "g"])
    assert key_settings(["a"], ["f"], {"substituters": "x"}) == (["a"], ["f"])
