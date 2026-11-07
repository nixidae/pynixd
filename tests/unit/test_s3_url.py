"""`s3://` URLs to the HTTP URL each reads from: the cases of Nix's `src/libstore-tests/s3-url.cc:207`.

Nix's cases name one object; pynixd's base URL is a prefix, so each expected URL gains a trailing slash.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pynixd.config import S3BinaryCacheSpec
from pynixd.s3_url import https_url
from pynixd.store_url import spec_for

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("bucket", "key", "options", "expected"),
    [
        ("my-bucket", "my-key.txt", {}, "https://my-bucket.s3.us-east-1.amazonaws.com/my-key.txt/"),
        (
            "prod-cache",
            "nix/store/abc123.nar.xz",
            {"region": "eu-west-1"},
            "https://prod-cache.s3.eu-west-1.amazonaws.com/nix/store/abc123.nar.xz/",
        ),
        ("bucket", "key", {"scheme": "http", "endpoint": "custom.s3.com"}, "http://custom.s3.com/bucket/key/"),
        ("bucket", "key", {"endpoint": "http://server:9000"}, "http://server:9000/bucket/key/"),
        (
            "bucket",
            "path/to/file.txt",
            {"region": "ap-southeast-2", "scheme": "https"},
            "https://bucket.s3.ap-southeast-2.amazonaws.com/path/to/file.txt/",
        ),
        (
            "my-bucket",
            "my-key.txt",
            {"region": "us-west-2", "style": "path"},
            "https://s3.us-west-2.amazonaws.com/my-bucket/my-key.txt/",
        ),
        (
            "bucket",
            "key",
            {"scheme": "http", "style": "virtual", "endpoint": "custom.s3.com"},
            "http://bucket.custom.s3.com/key/",
        ),
        ("bucket", "key", {"style": "virtual", "endpoint": "http://server:9000"}, "http://bucket.server:9000/key/"),
        ("my.bucket", "key", {"style": "path"}, "https://s3.us-east-1.amazonaws.com/my.bucket/key/"),
        ("my.bucket.name", "key", {}, "https://s3.us-east-1.amazonaws.com/my.bucket.name/key/"),
        ("my.bucket", "key", {"endpoint": "minio.local"}, "https://minio.local/my.bucket/key/"),
        ("cache", "", {"endpoint": "http://127.0.0.1:9000"}, "http://127.0.0.1:9000/cache/"),
    ],
)
def test_an_s3_url_reads_where_nix_reads(bucket: str, key: str, options: dict[str, Any], expected: str) -> None:
    assert https_url(bucket, key, **options) == expected


@pytest.mark.parametrize(
    ("bucket", "options"),
    [
        ("my.bucket", {"style": "virtual"}),
        ("bucket", {"style": "virtual", "endpoint": "file:///some/path"}),
    ],
)
def test_an_address_nix_refuses_is_refused(bucket: str, options: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="virtual-hosted-style"):
        https_url(bucket, "key", **options)


def test_the_parameters_nix_reads_reach_the_spec() -> None:
    spec = spec_for(
        "s3://bucket/some/prefix?region=eu-west-1&profile=prod&endpoint=http://minio:9000"
        "&addressing-style=path&scheme=http&priority=7&trusted=1"
    )
    assert isinstance(spec, S3BinaryCacheSpec)
    assert (spec.bucket, spec.key, spec.region, spec.profile) == ("bucket", "some/prefix", "eu-west-1", "prod")
    assert (spec.endpoint, spec.addressing_style, spec.scheme) == ("http://minio:9000", "path", "http")
    assert spec.priority == 7
    assert spec.trusted
    assert spec.url() == "http://minio:9000/bucket/some/prefix/"


@pytest.mark.parametrize(
    "url",
    [
        "s3://bucket/key?versionId=abc123xyz",
        "s3://bucket?addressing-style=bogus",
        "s3://bucket?storage-class=GLACIER",
        "s3://my.bucket?addressing-style=virtual",
        "s3:///key",
    ],
)
def test_what_the_registry_cannot_read_is_left_to_the_daemon(url: str) -> None:
    assert spec_for(url) is None


async def test_a_request_is_signed_with_the_credentials_of_the_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`CURLOPT_AWS_SIGV4` with `aws:amz:<region>:s3` (`filetransfer.cc:1302`): the region signs the request."""
    for name, value in {
        "AWS_ACCESS_KEY_ID": "AKIDEXAMPLE",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_CONFIG_FILE": str(tmp_path / "config"),
        "AWS_SHARED_CREDENTIALS_FILE": str(tmp_path / "credentials"),
    }.items():
        monkeypatch.setenv(name, value)
    spec = spec_for("s3://bucket?region=eu-north-1&endpoint=http://127.0.0.1:1")
    assert isinstance(spec, S3BinaryCacheSpec)
    store = spec.to_store("s3-test")
    credentials = store._resolve_credentials()
    assert credentials is not None
    store._credentials = credentials
    headers = await store._headers("http://127.0.0.1:1/bucket/nix-cache-info")
    assert headers["Authorization"].startswith("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/")
    assert "/eu-north-1/s3/aws4_request" in headers["Authorization"]
    assert "x-amz-content-sha256" in {key.lower() for key in headers}


async def test_no_credentials_send_the_request_unsigned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`filetransfer.cc:1309`: Nix warns and goes on without a signature, so a public bucket still answers."""
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    spec = spec_for("s3://bucket?endpoint=http://127.0.0.1:1")
    assert isinstance(spec, S3BinaryCacheSpec)
    store = spec.to_store("s3-test")
    assert store._resolve_credentials() is None
    assert await store._headers("http://127.0.0.1:1/bucket/nix-cache-info") == {}
