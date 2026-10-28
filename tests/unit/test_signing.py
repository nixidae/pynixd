"""Unit tests for pynixd.signing — Ed25519 path signing.

Tests SecretKey parsing, fingerprint construction, and signing roundtrips.
Uses deterministic test keys — no I/O or env dependencies.
"""

from __future__ import annotations

import base64

import nacl.bindings
import pytest

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd.serde import ValidPathInfo
from pynixd.signing import SecretKey, fingerprint, get_default_signing_key, sign_path_info
from pynixd.store_path import StorePath

_SEED_32 = b"\x00" * 32
_SEED_32_B64 = base64.b64encode(_SEED_32).decode()
_verify_key = nacl.bindings.crypto_sign_seed_keypair(_SEED_32)[0]
_SEED_64 = _SEED_32 + _verify_key
_SEED_64_B64 = base64.b64encode(_SEED_64).decode()


class TestSecretKeyParse:
    def test_parse_32_byte_seed(self) -> None:
        key = SecretKey._parse(f"test:{_SEED_32_B64}")
        assert key.name == "test"

    def test_parse_64_byte_libsodium(self) -> None:
        key = SecretKey._parse(f"test:{_SEED_64_B64}")
        assert key.name == "test"

    def test_parse_from_string(self) -> None:
        key = SecretKey.from_string(f"mykey:{_SEED_32_B64}")
        assert key.name == "mykey"

    def test_parse_invalid_length(self) -> None:
        with pytest.raises(ValueError, match="Secret key must be 32 or 64 bytes"):
            SecretKey._parse("bad:" + base64.b64encode(b"\x01" * 16).decode())

    def test_parse_empty_key(self) -> None:
        with pytest.raises(ValueError):  # noqa: PT011
            SecretKey._parse("empty:")

    def test_public_key_derivation(self) -> None:
        key = SecretKey._parse(f"test:{_SEED_32_B64}")
        pub = key.public_key_bytes
        expected_pub = nacl.bindings.crypto_sign_seed_keypair(_SEED_32)[0]
        assert pub == expected_pub

    def test_public_key_string_format(self) -> None:
        key = SecretKey._parse(f"test:{_SEED_32_B64}")
        pks = key.public_key_string()
        assert pks.startswith("test:")
        name, b64 = pks.split(":", 1)
        assert name == "test"
        decoded = base64.b64decode(b64)
        assert len(decoded) == 32


class TestSecretKeySign:
    def test_sign_deterministic(self) -> None:
        key = SecretKey._parse(f"test:{_SEED_32_B64}")
        data = b"hello world"
        sig1 = key.sign(data)
        sig2 = key.sign(data)
        assert sig1 == sig2
        assert len(sig1) == 64

    def test_sign_fingerprint_format(self) -> None:
        key = SecretKey._parse(f"test:{_SEED_32_B64}")
        fp = "1;/nix/store/abc123-foo;sha256:xyz;42;"
        sig_str = key.sign_fingerprint(fp)
        assert sig_str.startswith("test:")
        name, b64 = sig_str.split(":", 1)
        assert name == "test"
        decoded = base64.b64decode(b64)
        assert len(decoded) == 64

    def test_verify_with_public_key(self) -> None:
        key = SecretKey._parse(f"test:{_SEED_32_B64}")
        data = b"verify me"
        sig = key.sign(data)
        pub = key.public_key_bytes
        signed_msg = sig + data
        nacl.bindings.crypto_sign_open(signed_msg, pub)


# One NAR hash in the two forms. `nix-daemon` sends the first, and
# `ValidPathInfo::fingerprint` at `path-info.cc:48` of Nix writes the second.
# Nix itself made the pair:
#   nix hash convert --hash-algo sha256 --to nix32 c6f868...e105
_DIGEST_16 = "c6f868b00a75e555f44b2554f3fb57c69836460fc0f02e515455723b1f46e105"
_DIGEST_32 = "01g18qgknwjmai8jxw601x33d666azxz6m159gs5brbm1aq6iy66"


class TestFingerprint:
    """The string that a signature covers, and the form of the NAR hash in it.

    **A fingerprint carries the base-32 digest, and the wire carries base 16.**
    `path-info.cc:48` writes `narHash.to_string(HashFormat::Nix32, true)`, and
    `worker-protocol.cc:356` writes `Base16` with no name of an algorithm. So
    the value that reaches pynixd has to be converted, and pynixd converted
    nothing: it signed a fingerprint over the base-16 digest, which no
    verifier of Nix accepts.

    The two callers also disagreed with each other. `sign_path_info` put
    `sha256:` in front of the base-16 digest, and `SignPathInfo` of
    `DaemonStore` passed the digest with no name at all, so one path signed
    two ways gave two signatures and neither one was right.
    """

    def test_the_fingerprint_carries_the_base_32_digest(self) -> None:
        path = StorePath("/nix/store/abc123-foo")

        fp = fingerprint(path, NARHash(f"sha256:{_DIGEST_16}"), 42, set())

        assert fp == f"1;/nix/store/abc123-foo;sha256:{_DIGEST_32};42;"

    def test_a_digest_with_no_name_of_an_algorithm_gets_one(self) -> None:
        """`nix-daemon` sends the digest alone, and that is what pynixd holds.

        `NARHash` is what makes the two agree: it takes off a name that a
        source put in front, so the fingerprint sees one form only. The
        stripping itself is asserted in
        `nix-daemon-protocol/tests/test_wire_scalars.py`.
        """
        path = StorePath("/nix/store/abc123-foo")

        assert fingerprint(path, NARHash(_DIGEST_16), 42, set()) == fingerprint(
            path,
            NARHash(f"sha256:{_DIGEST_16}"),
            42,
            set(),
        )

    def test_a_digest_that_is_base_32_already_passes_through(self) -> None:
        path = StorePath("/nix/store/abc123-foo")

        fp = fingerprint(path, NARHash(f"sha256:{_DIGEST_32}"), 42, set())

        assert fp == f"1;/nix/store/abc123-foo;sha256:{_DIGEST_32};42;"

    def test_with_references_sorted(self) -> None:
        path = StorePath("/nix/store/abc123-foo")
        refs = {
            StorePath("/nix/store/zzz-bar"),
            StorePath("/nix/store/aaa-baz"),
        }

        fp = fingerprint(path, NARHash(f"sha256:{_DIGEST_16}"), 42, refs)

        assert fp == (f"1;/nix/store/abc123-foo;sha256:{_DIGEST_32};42;/nix/store/aaa-baz,/nix/store/zzz-bar")

    def test_empty_hash(self) -> None:
        path = StorePath("/nix/store/abc123-foo")
        fp = fingerprint(path, NARHash(""), 0, set())
        assert fp == "1;/nix/store/abc123-foo;;0;"

    def test_large_nar_size(self) -> None:
        path = StorePath("/nix/store/abc123-foo")
        fp = fingerprint(path, NARHash(f"sha256:{_DIGEST_16}"), 999999999999, set())
        assert "999999999999" in fp


class TestSignPathInfo:
    def test_sign_path_info_roundtrip(self) -> None:

        key = SecretKey._parse(f"test:{_SEED_32_B64}")
        path = StorePath(path="/nix/store/abc123-foo")
        references = {StorePath(path="/nix/store/ref1-dep")}  # pyright: ignore[reportUnhashable]
        info = ValidPathInfo(
            path=path,
            info=UnkeyedValidPathInfo(
                deriver=None,
                nar_hash=NARHash(hash=_DIGEST_16),
                references=references,
                registration_time=Time(ts=0),
                nar_size=42,
                ultimate=False,
                sigs=set(),
                ca=ContentAddress(value=""),
            ),
        )

        sig = sign_path_info(key, info)
        assert sig.startswith("test:")

        fp = fingerprint(info.path, info.info.nar_hash, info.info.nar_size, info.info.references)
        sig_bytes = sig.split(":", 1)[1]
        signed_msg = base64.b64decode(sig_bytes) + fp.encode("utf-8")
        nacl.bindings.crypto_sign_open(signed_msg, key.public_key_bytes)


class TestGetDefaultSigningKey:
    def test_no_env_var(self, monkeypatch) -> None:
        monkeypatch.delenv("PYNIXD_SIGNING_KEY", raising=False)

        assert get_default_signing_key() is None

    def test_with_env_var(self, monkeypatch) -> None:
        key_str = f"mykey:{_SEED_32_B64}"
        monkeypatch.setenv("PYNIXD_SIGNING_KEY", key_str)

        key = get_default_signing_key()
        assert key is not None
        assert key.name == "mykey"
