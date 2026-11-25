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

import anyio
import botocore.session
import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_path_info import QueryPathInfoRequest
from nix_daemon_protocol.store_path import StorePath
from pynixd import Server
from pynixd.cold_tier import ColdTier
from pynixd.config import LocalBinaryCacheSpec, S3BinaryCacheSpec, S3Compression
from pynixd.exceptions import CacheFault
from pynixd.store import LocalDBStore, S3BinaryCacheStore, Store
from pynixd.store.s3_upload import MIN_PART, Uploaded
from pynixd_store.db_migrations import PATH_ACCESS_TABLE
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


@dataclass(frozen=True)
class S3Shape:
    """How the cold tier addresses and writes the bucket."""

    compression: S3Compression = "zstd"
    prefix: str = ""
    region: str = "us-east-1"


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
async def _stores(
    lab: _Lab, shape: S3Shape | None = None, **extra: Store
) -> AsyncIterator[tuple[LocalDBStore, S3BinaryCacheStore]]:
    """The source store and a cold tier on bucket `cache` in *shape*, started by a server with *extra*."""
    shape = shape or S3Shape()
    local = LocalDBStore(
        make_test_spec(
            store_id="local", store_path=lab.source, no_probe=True, nix_config=for_test_store(substituters=())
        )
    )
    spec = S3BinaryCacheSpec(
        bucket="cache",
        key=shape.prefix,
        endpoint=lab.server.endpoint,
        region=shape.region,
        addressing_style="path",
        secret_key=lab.key,
        compression=shape.compression,
        multipart_chunk_size=MIN_PART,
        gc_cold=True,
    )
    cold = spec.to_store("cold")
    stores: dict[StoreId, Store] = {StoreId("local"): local, StoreId("cold"): cold}
    stores.update({StoreId(name): store for name, store in extra.items()})
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


def _narinfos(lab: _Lab) -> set[str]:
    listed = _s3(lab).list_objects_v2(Bucket="cache").get("Contents", [])
    return {o["Key"] for o in listed if o["Key"].endswith(".narinfo")}


def _narinfo(path: str) -> str:
    return f"{StorePath(path).hash_part()}.narinfo"


async def test_a_closure_goes_up_whole_and_is_recorded(lab: _Lab) -> None:
    async with _stores(lab) as (local, cold):
        tier = ColdTier(cold, [], local)
        first = await tier.upload_closure({StorePath(lab.parent)})
        assert (first.closure, first.recorded, first.uploaded) == (2, 0, 2)
        assert await tier.covered({lab.parent, lab.child, lab.big}) == {lab.parent, lab.child}
        again = await tier.upload_closure({StorePath(lab.parent)})
        assert (again.recorded, again.uploaded, again.present) == (2, 0, 0)
    assert _narinfos(lab) == {_narinfo(lab.parent), _narinfo(lab.child)}


async def test_a_stable_store_keeps_its_paths_out_of_the_bucket(lab: _Lab) -> None:
    """The control is the test above: with no stable store, `child` goes up."""
    stable_dir = lab.tmp / "stable"
    await _nix("copy", "--no-check-sigs", "--from", str(lab.source), "--to", f"file://{stable_dir}", lab.child)
    stable = LocalBinaryCacheSpec(path=stable_dir, gc_stable=True).to_store("stable")
    async with _stores(lab, stable=stable) as (local, cold):
        tier = ColdTier(cold, [stable], local)
        report = await tier.upload_closure({StorePath(lab.parent)})
        assert (report.stable, report.uploaded) == (1, 1)
        assert await tier.covered({lab.parent, lab.child}) == {lab.parent, lab.child}
    assert _narinfos(lab) == {_narinfo(lab.parent)}


async def _access(local: LocalDBStore, path: str) -> object:
    await local.db.flush_references()
    row = await local.db.fetchone(f"SELECT lastReferencedAt FROM {PATH_ACCESS_TABLE} WHERE path = ?", (path,))
    return None if row is None else row[0]


async def test_an_upload_leaves_the_access_date_alone(lab: _Lab) -> None:
    """The collector ages a path by its access date, so the upload of a candidate must not move it."""
    async with _stores(lab) as (local, cold):
        await ColdTier(cold, [], local).upload_closure({StorePath(lab.parent)})
        assert await _access(local, lab.parent) is None
        # The control: a client's read of the same path dates it.
        await local.execute(QueryPathInfoRequest(path=StorePath(lab.parent)))
        assert await _access(local, lab.parent) is not None


async def test_a_refusal_stops_before_the_referrer(lab: _Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    """With the bucket read-only nothing is recorded, and the referrer is never tried."""
    s3 = _s3(lab)
    s3.put_object(Bucket="cache", Key="nix-cache-info", Body=b"StoreDir: /nix/store\n")
    access, secret = lab.server.read_only
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", access)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret)
    async with _stores(lab) as (local, cold):
        tier = ColdTier(cold, [], local)
        with pytest.raises(CacheFault, match="403"):
            await tier.upload_closure({StorePath(lab.parent)})
        assert await tier.covered({lab.parent, lab.child}) == set()


def _left_behind(lab: _Lab) -> tuple[set[str], list[object]]:
    """The objects but `nix-cache-info`, and the multipart uploads still open."""
    s3 = _s3(lab)
    objects = {o["Key"] for o in s3.list_objects_v2(Bucket="cache").get("Contents", [])} - {"nix-cache-info"}
    return objects, s3.list_multipart_uploads(Bucket="cache").get("Uploads", [])


async def test_a_refused_part_aborts_the_upload(lab: _Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    """Part 2 of 3 fails: the open upload is aborted, so the bucket keeps no parts, no NAR and no narinfo.

    Parts go up at once, so part 3 may be in flight when part 2 fails; the abort removes it too.
    """
    async with _stores(lab) as (local, cold):
        put_part = cold._put_part
        sent: list[int] = []

        async def refuse_second(name: str, multipart: str, number: int, part: bytes) -> str:
            sent.append(number)
            if number == 2:
                raise CacheFault(f"part {number} of {name}: refused by the test", status=403)
            return await put_part(name, multipart, number, part)

        monkeypatch.setattr(cold, "_put_part", refuse_second)
        with pytest.raises(CacheFault, match="refused by the test"):
            await ColdTier(cold, [], local).upload_closure({StorePath(lab.big)})
        assert 2 in sent
        assert await ColdTier(cold, [], local).covered({lab.big}) == set()
    assert _left_behind(lab) == (set(), [])


async def test_parts_go_up_at_once(lab: _Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    """Part 1 waits until part 2 has started: a store that sent parts one by one would hang here."""
    async with _stores(lab) as (local, cold):
        put_part = cold._put_part
        second = anyio.Event()

        async def hold_first(name: str, multipart: str, number: int, part: bytes) -> str:
            if number == 1:
                await second.wait()
            else:
                second.set()
            return await put_part(name, multipart, number, part)

        monkeypatch.setattr(cold, "_put_part", hold_first)
        with anyio.fail_after(30):
            assert await cold.upload(local, StorePath(lab.big)) is Uploaded.UPLOADED
    target = lab.tmp / "target"
    await _nix("copy", "--no-check-sigs", "--from", lab.url("cache"), "--to", str(target), lab.big)


async def test_a_cancelled_upload_aborts_too(lab: _Lab, monkeypatch: pytest.MonkeyPatch) -> None:
    """A shutdown cancels the upload after part 1: the abort still runs, shielded."""
    async with _stores(lab) as (local, cold):
        put_part = cold._put_part
        first = anyio.Event()

        async def stall_second(name: str, multipart: str, number: int, part: bytes) -> str:
            if number == 2:
                first.set()
                await anyio.sleep_forever()
            return await put_part(name, multipart, number, part)

        monkeypatch.setattr(cold, "_put_part", stall_second)
        async with anyio.create_task_group() as tg:
            tg.start_soon(cold.upload, local, StorePath(lab.big))
            await first.wait()
            # The control: part 1 is in the bucket, in an open upload.
            assert len(_left_behind(lab)[1]) == 1
            tg.cancel_scope.cancel()
    assert _left_behind(lab) == (set(), [])


async def test_a_refused_narinfo_leaves_the_path_unseen_and_a_retry_completes_it(
    lab: _Lab, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The narinfo is the commit: with its PUT refused, no client sees the path and nothing is recorded.

    The NAR object stays, named by its NAR hash, and the next upload writes it again and commits.
    """
    async with _stores(lab) as (local, cold):
        expect = cold._expect

        async def refuse_narinfo(method: str, name: str, body: bytes = b"", **kwargs: str) -> bytes:
            if method == "PUT" and name.endswith(".narinfo"):
                raise CacheFault(f"PUT {name}: refused by the test")
            return await expect(method, name, body, **kwargs)

        tier = ColdTier(cold, [], local)
        with monkeypatch.context() as patch:
            patch.setattr(cold, "_expect", refuse_narinfo)
            with pytest.raises(CacheFault, match="refused by the test"):
                await tier.upload_closure({StorePath(lab.child)})
        assert _narinfos(lab) == set()
        assert await tier.covered({lab.child}) == set()
        report = await tier.upload_closure({StorePath(lab.child)})
        assert report.uploaded == 1
    assert _narinfos(lab) == {_narinfo(lab.child)}
    target = lab.tmp / "target"
    await _nix("copy", "--no-check-sigs", "--from", lab.url("cache"), "--to", str(target), lab.child)


@pytest.mark.parametrize(
    "shape",
    [S3Shape(compression="xz"), S3Shape(compression="none"), S3Shape(prefix="cold/tier"), S3Shape(region="auto")],
    ids=["xz", "none", "prefix", "region-auto"],
)
async def test_nix_reads_every_shape_back(lab: _Lab, shape: S3Shape) -> None:
    """R2 takes region `auto` and path-style addresses; a prefix keeps the cache in part of a bucket."""
    async with _stores(lab, shape) as (local, cold):
        for path in (lab.child, lab.parent, lab.big):
            assert await cold.upload(local, StorePath(path)) is Uploaded.UPLOADED
    s3 = _s3(lab)
    key = f"{shape.prefix}/" if shape.prefix else ""
    info = _fields(s3.get_object(Bucket="cache", Key=f"{key}{_narinfo(lab.big)}")["Body"].read().decode())
    assert info["Compression"] == [shape.compression]
    query = f"endpoint={lab.server.endpoint}&region={shape.region}"
    source = f"s3://cache/{shape.prefix}?{query}" if shape.prefix else f"s3://cache?{query}"
    trusted = ["--option", "require-sigs", "true", "--option", "trusted-public-keys", lab.public]
    target = lab.tmp / "target"
    await _nix("copy", *trusted, "--from", source, "--to", str(target), lab.parent, lab.big)
    assert set((await _nix("path-info", "--store", str(target), "--all")).split()) == {lab.parent, lab.child, lab.big}
