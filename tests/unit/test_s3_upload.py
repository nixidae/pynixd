"""What an upload to an S3 binary cache writes, short of the bucket. nixidae/nixidae#1417."""

from __future__ import annotations

import pytest

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.wire_time import Time
from pynixd.serde import ValidPathInfo
from pynixd.store.binary_cache import parse_narinfo
from pynixd.store.s3_upload import (
    MAX_PARTS,
    MIN_PART,
    Parts,
    complete_body,
    error_of,
    nar_object,
    narinfo_text,
    part_size,
    upload_id,
)

LEAF = "/nix/store/00000000000000000000000000000000-leaf"
NAR_HASH = "1b8m03r63zqhnjf7l5wnldhh7c134ap5vpj0850ymkq1iyzicy5s"


def _info(references: set[StorePath], sigs: set[Signature]) -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(LEAF),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash=NAR_HASH),
            references=references,
            registration_time=Time(ts=0),
            nar_size=120,
            ultimate=False,
            sigs=sigs,
            ca=ContentAddress(value=""),
        ),
    )


def test_parts_are_one_size_and_the_rest_is_held() -> None:
    parts = Parts(4)
    assert parts.feed(b"abc") == []
    assert parts.feed(b"defghij") == [b"abcd", b"efgh"]
    assert parts.rest() == b"ij"
    assert parts.rest() == b""


def test_data_on_a_part_boundary_leaves_no_rest() -> None:
    parts = Parts(2)
    assert parts.feed(b"abcd") == [b"ab", b"cd"]
    assert parts.rest() == b""


@pytest.mark.parametrize("nar_size", [0, 1, 200 * 1024**3])
def test_a_part_size_fits_every_nar_in_the_part_limit(nar_size: int) -> None:
    size = part_size(nar_size, MIN_PART)
    assert size >= MIN_PART
    assert size * MAX_PARTS >= nar_size + nar_size // 100


def test_the_chunk_size_is_a_floor() -> None:
    assert part_size(1, 16 * 1024 * 1024) == 16 * 1024 * 1024


def test_the_nar_object_is_named_by_the_nar_hash() -> None:
    assert nar_object(_info(set(), set()), "zstd") == f"nar/{NAR_HASH}.nar.zst"
    assert nar_object(_info(set(), set()), "none") == f"nar/{NAR_HASH}.nar"


def test_a_narinfo_with_no_references_still_names_them() -> None:
    """`NarInfo::to_string` writes `References:` when the set is empty (`nar-info.cc:121`)."""
    text = narinfo_text(_info(set(), set()), url="nar/x.nar.zst", compression="zstd", file_hash="f" * 52, file_size=9)
    assert "References: \n" in text


def test_a_narinfo_reads_back_with_every_signature() -> None:
    sigs = {Signature("a:AAAA"), Signature("b:BBBB")}
    info = _info({StorePath("/nix/store/11111111111111111111111111111111-dep")}, sigs)
    text = narinfo_text(info, url="nar/x.nar.zst", compression="zstd", file_hash="f" * 52, file_size=9)
    assert [line.partition(":")[0] for line in text.splitlines()] == [
        "StorePath",
        "URL",
        "Compression",
        "FileHash",
        "FileSize",
        "NarHash",
        "NarSize",
        "References",
        "Sig",
        "Sig",
    ]
    parsed = parse_narinfo(text, expected_path=StorePath(LEAF))
    assert parsed is not None
    assert parsed.url == "nar/x.nar.zst"
    assert parsed.file_size == 9
    assert parsed.valid_path_info.info.sigs == sigs
    assert parsed.valid_path_info.info.references == info.info.references


def test_the_complete_request_numbers_each_etag() -> None:
    body = complete_body(['"e1"', '"e2"'])
    assert b'<PartNumber>1</PartNumber><ETag>"e1"</ETag>' in body
    assert b'<PartNumber>2</PartNumber><ETag>"e2"</ETag>' in body


def test_an_upload_id_is_read_from_the_create_answer() -> None:
    body = (
        b'<?xml version="1.0"?><InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        b"<Bucket>cache</Bucket><Key>nar/x</Key><UploadId>abc+/=</UploadId></InitiateMultipartUploadResult>"
    )
    assert upload_id(body) == "abc+/="
    with pytest.raises(ValueError, match="no UploadId"):
        upload_id(b"<InitiateMultipartUploadResult/>")


def test_a_200_with_an_error_body_is_an_error() -> None:
    """`CompleteMultipartUpload` may answer 200 and fail in the body."""
    body = b"<Error><Code>InternalError</Code><Message>try again</Message></Error>"
    assert error_of(body) == "InternalError: try again"
    assert error_of(b"<CompleteMultipartUploadResult/>") is None
    assert error_of(b"") is None
    assert error_of(b"not xml") is None
