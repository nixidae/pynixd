"""Unit tests for serde ValidPathInfo narinfo serialization.

Tests from_narinfo parsing and to_narinfo serialization with various
combinations of fields. All tests are pure — no I/O.
"""

from __future__ import annotations

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.wire_time import Time
from pynixd.serde import StorePath, ValidPathInfo


def path(value: str) -> StorePath:
    return StorePath(path=value)


def info(
    *,
    store_path: str,
    nar_hash: str,
    nar_size: int,
    references: set[str] | None = None,
    deriver: str | None = None,
    sigs: set[str] | None = None,
    ca: str = "",
) -> ValidPathInfo:
    return ValidPathInfo(
        path=path(store_path),
        info=UnkeyedValidPathInfo(
            deriver=path(deriver) if deriver else None,
            nar_hash=NARHash(hash=nar_hash.removeprefix("sha256:")),
            references={path(ref) for ref in references or set()},
            registration_time=Time(ts=0),
            nar_size=nar_size,
            ultimate=False,
            sigs={Signature(**Signature.from_str(sig)) for sig in sigs or set()},
            ca=ContentAddress(value=ca),
        ),
    )


class TestNarinfoRoundtrip:
    def test_minimal(self) -> None:
        vpi = info(
            store_path="/nix/store/abc123-foo",
            nar_hash="sha256:abc123",
            nar_size=42,
        )
        narinfo = vpi.to_narinfo()
        parsed = ValidPathInfo.from_narinfo(narinfo)
        assert parsed.path == vpi.path
        assert parsed.info.nar_hash == vpi.info.nar_hash
        assert parsed.info.nar_size == vpi.info.nar_size
        assert parsed.info.references == set()
        assert parsed.info.deriver is None
        assert parsed.info.sigs == set()
        assert parsed.info.ca == ""

    def test_with_references(self) -> None:
        vpi = info(
            store_path="/nix/store/abc123-foo",
            nar_hash="sha256:abc123",
            nar_size=100,
            references={
                "/nix/store/ref1-bar",
                "/nix/store/ref2-baz",
            },
        )
        narinfo = vpi.to_narinfo()
        parsed = ValidPathInfo.from_narinfo(narinfo)
        assert parsed.path == vpi.path
        assert parsed.info.references == vpi.info.references

    def test_with_deriver(self) -> None:
        vpi = info(
            store_path="/nix/store/abc123-foo",
            nar_hash="sha256:abc123",
            nar_size=50,
            deriver="/nix/store/drv1-bar.drv",
        )
        narinfo = vpi.to_narinfo()
        parsed = ValidPathInfo.from_narinfo(narinfo)
        assert parsed.info.deriver == vpi.info.deriver

    def test_with_signatures(self) -> None:
        vpi = info(
            store_path="/nix/store/abc123-foo",
            nar_hash="sha256:abc123",
            nar_size=50,
            sigs={"key1:sig1", "key2:sig2"},
        )
        narinfo = vpi.to_narinfo()
        parsed = ValidPathInfo.from_narinfo(narinfo)
        assert parsed.info.sigs == vpi.info.sigs

    def test_with_ca(self) -> None:
        vpi = info(
            store_path="/nix/store/abc123-foo",
            nar_hash="sha256:abc123",
            nar_size=50,
            ca="text:sha256:xyz",
        )
        narinfo = vpi.to_narinfo()
        parsed = ValidPathInfo.from_narinfo(narinfo)
        assert parsed.info.ca == vpi.info.ca

    def test_full_roundtrip(self) -> None:
        vpi = info(
            store_path="/nix/store/abc123-foo",
            deriver="/nix/store/drv1-bar.drv",
            nar_hash="sha256:abc123def456",
            references={
                "/nix/store/ref1-bar",
                "/nix/store/ref2-baz",
            },
            nar_size=999,
            sigs={"key1:sig1", "key2:sig2"},
            ca="text:sha256:xyz",
        )
        narinfo = vpi.to_narinfo()
        parsed = ValidPathInfo.from_narinfo(narinfo)
        assert parsed.path == vpi.path
        assert parsed.info.deriver == vpi.info.deriver
        assert parsed.info.nar_hash == vpi.info.nar_hash
        assert parsed.info.references == vpi.info.references
        assert parsed.info.nar_size == vpi.info.nar_size
        assert parsed.info.sigs == vpi.info.sigs
        assert parsed.info.ca == vpi.info.ca

    def test_nar_hash_without_prefix(self) -> None:
        """to_narinfo should add sha256: prefix if missing."""
        vpi = info(
            store_path="/nix/store/abc123-foo",
            nar_hash="abc123",
            nar_size=50,
        )
        narinfo = vpi.to_narinfo()
        # The roundtrip should add sha256: prefix
        assert "sha256:abc123" in narinfo


class TestNarinfoParsingEdgeCases:
    def test_empty_content(self) -> None:
        parsed = ValidPathInfo.from_narinfo("")
        assert parsed.path == path("")

    def test_comment_lines(self) -> None:
        content = """# This is a comment
StorePath: /nix/store/abc-foo
NarHash: sha256:xyz
NarSize: 10
"""
        parsed = ValidPathInfo.from_narinfo(content)
        assert parsed.path == path("/nix/store/abc-foo")

    def test_empty_lines(self) -> None:
        content = """StorePath: /nix/store/abc-foo

NarHash: sha256:xyz

NarSize: 10
"""
        parsed = ValidPathInfo.from_narinfo(content)
        assert parsed.path == path("/nix/store/abc-foo")

    def test_references_without_prefix(self) -> None:
        """References without /nix/store/ prefix should be fixed up."""
        content = """StorePath: /nix/store/abc-foo
NarHash: sha256:xyz
NarSize: 10
References: ref1-bar ref2-baz
"""
        parsed = ValidPathInfo.from_narinfo(content)
        assert path("/nix/store/ref1-bar") in parsed.info.references

    def test_deriver_without_prefix(self) -> None:
        content = """StorePath: /nix/store/abc-foo
NarHash: sha256:xyz
NarSize: 10
Deriver: drv1-bar.drv
"""
        parsed = ValidPathInfo.from_narinfo(content)
        assert parsed.info.deriver == path("/nix/store/drv1-bar.drv")

    def test_empty_deriver(self) -> None:
        content = """StorePath: /nix/store/abc-foo
NarHash: sha256:xyz
NarSize: 10
Deriver:
"""
        parsed = ValidPathInfo.from_narinfo(content)
        assert parsed.info.deriver is None

    def test_unknown_keys_ignored(self) -> None:
        content = """StorePath: /nix/store/abc-foo
NarHash: sha256:xyz
NarSize: 10
URL: nar/xyz.nar
Compression: none
"""
        parsed = ValidPathInfo.from_narinfo(content)
        assert parsed.path == path("/nix/store/abc-foo")
        assert str(parsed.info.nar_hash) == "xyz"
