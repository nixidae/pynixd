"""Uploads to an `s3://` binary cache, read back by stock Nix. nixidae/nixidae#1417.

Nix is the oracle: `nix copy --from` the bucket into an empty store checks every NAR hash and the
signature, and each narinfo is compared with the one `nix copy --to` writes for the same path.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

import botocore.session
import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_path import StorePath
from pynixd import Server
from pynixd.config import S3BinaryCacheSpec
from pynixd.exceptions import CacheFault
from pynixd.store import LocalDBStore, S3BinaryCacheStore, Store
from pynixd.store.s3_upload import MIN_PART, Uploaded
from tests._conftest.nix_config import for_test_store
from tests._conftest.s3_bucket import S3Server, s3_bucket
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

    from botocore.client import BaseClient

_PARENT = (
    'with import <nixpkgs> {}; runCommand "pynixd-s3-upload-parent" {} '
    '"echo ${builtins.toFile "pynixd-s3-upload-child" "the child of the upload test"} > $out"'
)

_DIFFERS = frozenset({"URL", "FileHash", "FileSize"})
"""What names the NAR object: pynixd names it by the NAR hash, Nix by the file hash."""


@dataclass
class _Lab:
    tmp: Path
    source: Path
    server: S3Server
    key: Path
    public: str
    parent: str
    child: str
    big: str
    """A path whose NAR takes three parts."""

    def url(self, bucket: str, **params: str) -> str:
        query = "&".join(
            f"{k}={v}" for k, v in {"endpoint": self.server.endpoint, "region": "us-east-1", **params}.items()
        )
        return f"s3://{bucket}?{query}"


async def _nix(*args: str, stdin: bytes | None = None) -> str:
    _rc, stdout, _stderr, _both = await run_subproc([str(NIX_BIN), *args], verbose=False, stdin=stdin)
    return stdout.strip()


@pytest.fixture
async def lab(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Lab]:
    source = tmp_path / "source"
    source.mkdir()
    parent = await _nix("build", "--impure", "--no-link", "--print-out-paths", "--expr", _PARENT)
    await _nix("copy", "--no-check-sigs", "--to", str(source), parent)
    child = next(
        p for p in (await _nix("path-info", "--store", str(source), "--recursive", parent)).split() if p != parent
    )
    blob = tmp_path / "big"
    blob.mkdir()
    (blob / "random").write_bytes(os.urandom(2 * MIN_PART + MIN_PART // 2))  # noqa: ASYNC240 -- test setup
    big = await _nix("store", "add-path", "--store", str(source), str(blob))
    key = tmp_path / "cold.sec"
    key.write_text(await _nix("key", "generate-secret", "--key-name", "cold-test"))  # noqa: ASYNC240 -- test setup
    public = await _nix("key", "convert-secret-to-public", stdin=key.read_bytes())
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache", buckets=("cache", "reference")) as server:
        yield _Lab(tmp_path, source, server, key, public, parent, child, big)


def _s3(lab: _Lab) -> BaseClient:
    return botocore.session.get_session().create_client("s3", endpoint_url=lab.server.endpoint, region_name="us-east-1")


@asynccontextmanager
async def _stores(lab: _Lab) -> AsyncIterator[tuple[Store, S3BinaryCacheStore]]:
    """The source store and a cold tier on bucket `cache`, started by a server."""
    local = LocalDBStore(
        make_test_spec(
            store_id="local", store_path=lab.source, no_probe=True, nix_config=for_test_store(substituters=())
        )
    )
    spec = S3BinaryCacheSpec(
        bucket="cache",
        endpoint=lab.server.endpoint,
        region="us-east-1",
        secret_key=lab.key,
        multipart_chunk_size=MIN_PART,
    )
    cold = spec.to_store("cold")
    stores: dict[StoreId, Store] = {StoreId("local"): local, StoreId("cold"): cold}
    async with Server(stores=stores, unix_path=lab.tmp / "pynixd.sock", ssh_port=None, http_port=None) as server:
        # The listeners do not wait for the stores; the upload does.
        scheduler = server.ctx.scheduler
        assert scheduler is not None
        assert scheduler.stores_opened is not None
        await scheduler.stores_opened.wait()
        yield local, cold


def _fields(text: str) -> dict[str, list[str]]:
    fields: dict[str, list[str]] = {}
    for line in text.splitlines():
        name, _, value = line.partition(": ")
        fields.setdefault(name, []).append(value)
    return fields


async def test_nix_copies_what_pynixd_uploaded_and_checks_the_signature(lab: _Lab) -> None:
    async with _stores(lab) as (local, cold):
        for path in (lab.child, lab.parent, lab.big):
            assert await cold.upload(local, StorePath(path)) is Uploaded.UPLOADED
        assert await cold.upload(local, StorePath(lab.parent)) is Uploaded.PRESENT
    s3 = _s3(lab)
    assert s3.get_object(Bucket="cache", Key="nix-cache-info")["Body"].read() == b"StoreDir: /nix/store\n"
    big = _fields(
        s3.get_object(Bucket="cache", Key=f"{StorePath(lab.big).hash_part()}.narinfo")["Body"].read().decode()
    )
    # A multipart object's ETag ends in its part count.
    assert s3.head_object(Bucket="cache", Key=big["URL"][0])["ETag"].strip('"').endswith("-3")

    target = lab.tmp / "target"
    # The harness sets `require-sigs = false` for every command.
    trusted = ["--option", "require-sigs", "true", "--option", "trusted-public-keys", lab.public]
    await _nix("copy", *trusted, "--from", lab.url("cache"), "--to", str(target), lab.parent, lab.big)
    copied = set((await _nix("path-info", "--store", str(target), "--all")).split())
    assert copied == {lab.parent, lab.child, lab.big}


async def test_nix_refuses_the_upload_under_another_key(lab: _Lab) -> None:
    """The control of the test above: the copy checks the signature, so another key fails it.

    `parent` is the one input-addressed path: Nix takes a content-addressed path with no signature.
    """
    async with _stores(lab) as (local, cold):
        for path in (lab.child, lab.parent):
            assert await cold.upload(local, StorePath(path)) is Uploaded.UPLOADED
    other = lab.tmp / "other.sec"
    other.write_text(await _nix("key", "generate-secret", "--key-name", "cold-test"))  # noqa: ASYNC240 -- test setup
    public = await _nix("key", "convert-secret-to-public", stdin=other.read_bytes())
    rc, _out, stderr, _both = await run_subproc(
        [
            str(NIX_BIN),
            "copy",
            "--option",
            "require-sigs",
            "true",
            "--option",
            "trusted-public-keys",
            public,
            "--from",
            lab.url("cache"),
            "--to",
            str(lab.tmp / "target"),
            lab.parent,
        ],
        verbose=False,
        expected_retcode=None,
    )
    assert rc != 0
    assert "lacks a signature by a trusted key" in stderr


async def test_each_narinfo_matches_what_nix_writes(lab: _Lab) -> None:
    """Every field but the NAR object's name, signature included: both sign with one key."""
    paths = (lab.child, lab.parent, lab.big)
    async with _stores(lab) as (local, cold):
        for path in paths:
            await cold.upload(local, StorePath(path))
    reference = lab.url("reference", **{"secret-key": str(lab.key), "compression": "zstd"})
    await _nix("copy", "--no-check-sigs", "--from", str(lab.source), "--to", reference, *paths)
    s3 = _s3(lab)
    for path in paths:
        name = f"{StorePath(path).hash_part()}.narinfo"
        ours = _fields(s3.get_object(Bucket="cache", Key=name)["Body"].read().decode())
        nixs = _fields(s3.get_object(Bucket="reference", Key=name)["Body"].read().decode())
        assert list(ours) == list(nixs)
        assert {k: v for k, v in ours.items() if k not in _DIFFERS} == {
            k: v for k, v in nixs.items() if k not in _DIFFERS
        }


async def test_a_wrong_secret_uploads_nothing(lab: _Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    s3 = _s3(lab)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "not-the-secret")
    async with _stores(lab) as (local, cold):
        with pytest.raises(CacheFault, match="403"):
            await cold.upload(local, StorePath(lab.child))
    assert s3.list_objects_v2(Bucket="cache")["KeyCount"] == 0


async def test_a_refused_put_reports_no_upload(lab: _Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    """A user that may read and not write: the presence check passes, the first PUT fails."""
    s3 = _s3(lab)
    s3.put_object(Bucket="cache", Key="nix-cache-info", Body=b"StoreDir: /nix/store\n")
    access, secret = lab.server.read_only
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", access)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret)
    async with _stores(lab) as (local, cold):
        with pytest.raises(CacheFault, match="403"):
            await cold.upload(local, StorePath(lab.big))
    assert [o["Key"] for o in s3.list_objects_v2(Bucket="cache")["Contents"]] == ["nix-cache-info"]
    assert s3.list_multipart_uploads(Bucket="cache").get("Uploads", []) == []
