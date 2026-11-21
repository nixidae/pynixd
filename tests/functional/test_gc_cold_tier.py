"""The age rule with a cold tier: a path leaves once its closure is in the bucket. nixidae/nixidae#1417.

The lab is `test_gc_max_age`'s, whose dry-run names `stale`, `parent` and `child` with no cold tier:
that is the control of every plan here. Stock `nix copy --from` the bucket is the oracle that a
deleted path comes back.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import anyio
import botocore.session
import pytest

from nix_daemon_protocol.ensure_path import EnsurePathRequest
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_path import StorePath
from pynixd import Server, metrics
from pynixd.cold_tier import ColdTier
from pynixd.config import S3BinaryCacheSpec
from pynixd.connection import Connection
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.exceptions import BackendError, CacheFault
from pynixd.gc import Collector
from pynixd.store import LocalDBStore, Store
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from pynixd_store.signing import SecretKey
from tests._conftest.client_options import set_options
from tests._conftest.nix_config import for_test_store
from tests._conftest.s3_bucket import s3_bucket
from tests.conftest import NIX_BIN, make_test_spec, run_subproc
from tests.functional.test_gc_max_age import MAX_AGE, STALE_AGE, _backdate, _freeze_tracker, _Lab, _lab, _valid

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from pynixd.nix_config import NixConfig


async def _nix(*args: str, stdin: bytes | None = None) -> str:
    _rc, stdout, _stderr, _both = await run_subproc([str(NIX_BIN), *args], verbose=False, stdin=stdin)
    return stdout.strip()


async def _key(tmp_path: Path) -> str:
    """The cold tier's signing key at `cold.sec`; answers its public half."""
    key = tmp_path / "cold.sec"
    if not key.exists():  # noqa: ASYNC240 -- test setup
        key.write_text(await _nix("key", "generate-secret", "--key-name", "cold-test"))  # noqa: ASYNC240 -- test setup
    return await _nix("key", "convert-secret-to-public", stdin=key.read_bytes())  # noqa: ASYNC240 -- test setup


@asynccontextmanager
async def _pynixd(
    lab: _Lab,
    tmp_path: Path,
    endpoint: str,
    target: float | None = None,
    nix_config: NixConfig | None = None,
    cold_interval: float = 900.0,
    verify: bool = True,
) -> AsyncIterator[tuple[Server, ColdTier]]:
    key = tmp_path / "cold.sec"
    await _key(tmp_path)
    spec = make_test_spec(
        store_id="local",
        store_path=lab.store_path,
        nix_config=nix_config,
        no_probe=True,
        postgres=None,
        gc_max_age=MAX_AGE,
        gc_target_usage=target,
        gc_allow_execute=True,
    )
    cold = S3BinaryCacheSpec(
        bucket="cache", endpoint=endpoint, region="us-east-1", secret_key=key, gc_cold=True, gc_cold_verify=verify
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec), StoreId("cold"): cold.to_store("cold")}
    async with Server(
        stores=stores, unix_path=_socket(tmp_path), ssh_port=None, http_port=None, gc_cold_interval=cold_interval
    ) as server:
        _freeze_tracker(server)
        scheduler = server.ctx.scheduler
        assert scheduler is not None
        assert scheduler.stores_opened is not None
        await scheduler.stores_opened.wait()
        tier = ColdTier.of(server.ctx)
        assert tier is not None
        await _backdate(lab.store_path, {lab.stale, lab.parent, lab.child, lab.rooted}, STALE_AGE)
        await _backdate(lab.store_path, {lab.fresh}, 0)
        yield server, tier


def _socket(tmp_path: Path) -> Path:
    return tmp_path / "pynixd.sock"


@asynccontextmanager
async def _client(tmp_path: Path) -> AsyncIterator[Connection]:
    link = await open_unix_connection(_socket(tmp_path))
    conn = Connection(UnixNixReader(link, identifier="t"), UnixNixWriter(link, identifier="t"), "t")
    await conn.connect()
    try:
        yield conn
    finally:
        await conn.close()


async def _plan(server: Server) -> set[str]:
    return {str(path) for path in (await Collector(server.ctx).run(PynixdGCAction.DRY_RUN)).store_paths}


async def test_a_path_goes_only_with_its_closure_in_the_bucket(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lab = await _lab(tmp_path)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint) as (server, tier):
            assert await _plan(server) == set()
            # `child` alone is up, but `parent` names it and stays, so `child` stays too.
            await tier.upload_closure({StorePath(lab.child)})
            assert await _plan(server) == set()
            await tier.upload_closure({StorePath(lab.parent)})
            assert await _plan(server) == {lab.parent, lab.child}
            await tier.upload_closure({StorePath(lab.stale)})
            deleted = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
        assert {str(path) for path in deleted.store_paths} == {lab.stale, lab.parent, lab.child}
        assert await _valid(lab.store_path) == {lab.fresh, lab.rooted}
        public = await _nix("key", "convert-secret-to-public", stdin=(tmp_path / "cold.sec").read_bytes())
        restored = tmp_path / "restored"
        source = f"s3://cache?endpoint={bucket.endpoint}&region=us-east-1"
        trusted = ["--option", "require-sigs", "true", "--option", "trusted-public-keys", public]
        await _nix("copy", *trusted, "--from", source, "--to", str(restored), lab.parent)
        assert await _valid(restored) == {lab.parent, lab.child}


async def test_over_the_target_it_stops_short_of_what_the_bucket_lacks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A target no disk meets: the covered `stale` goes, and every dead path the bucket lacks stays.

    Pressure lifts the age (`plan`), so `fresh` is dead enough to go, and stays only for being uncovered.
    """
    lab = await _lab(tmp_path)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9) as (server, tier):
            await tier.upload_closure({StorePath(lab.stale)})
            before = metrics.GC_STOPPED_SHORT._value.get()
            deleted = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
            counted = metrics.GC_STOPPED_SHORT._value.get() - before
    assert {str(path) for path in deleted.store_paths} == {lab.stale}
    assert counted == 1
    assert await _valid(lab.store_path) == {lab.parent, lab.child, lab.fresh, lab.rooted}


async def test_upload_ahead_covers_what_the_pressed_pass_takes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No budget uploads nothing; a budget over the plan covers it, and the pressed pass then takes all of it."""
    lab = await _lab(tmp_path)
    dead = {lab.stale, lab.parent, lab.child, lab.fresh}
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9) as (server, tier):
            assert await Collector(server.ctx).upload_ahead(tier, 0, 1.0) == 0
            assert await tier.covered(dead) == set()
            assert await Collector(server.ctx).upload_ahead(tier, 2**62, 1.0) == len(dead)
            assert await tier.covered(dead | {lab.rooted}) == dead
            deleted = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
    assert {str(path) for path in deleted.store_paths} == dead


@pytest.mark.parametrize("signer", ["trusted-key", "own-key", "foreign-key"])
async def test_an_evicted_path_comes_back_through_pynixd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, signer: str
) -> None:
    """A client asks pynixd for a path its collector moved to the bucket: pynixd fetches the closure back.

    The bucket is the one substituter, and the store requires signatures. The cold key admits its own paths
    whether `trusted-public-keys` names it or not. The control: a bucket signed by a key the store no longer
    holds, and no client trusts, gives nothing back, so the path came from the bucket, signature checked.
    """
    lab = await _lab(tmp_path)
    keys = (await _key(tmp_path),) if signer == "trusted-key" else ()
    config = for_test_store(substituters=(), trusted_public_keys=keys, require_sigs=True)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, nix_config=config) as (server, tier):
            await tier.upload_closure({StorePath(lab.parent)})
            deleted = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
            assert {lab.parent, lab.child} <= {str(path) for path in deleted.store_paths}
            assert not {lab.parent, lab.child} & await _valid(lab.store_path)
            if signer == "foreign-key":
                rotated = SecretKey.from_string(await _nix("key", "generate-secret", "--key-name", "cold-test"))
                monkeypatch.setattr(tier.cold, "_secret_key", rotated)
            async with _client(tmp_path) as conn:
                request = EnsurePathRequest(path=StorePath(lab.parent))
                if signer == "foreign-key":
                    with pytest.raises(BackendError, match="there is no substituter that can build it"):
                        await conn.call(request, raise_on_error=True)
                    return
                await conn.call(request, raise_on_error=True)
    assert {lab.parent, lab.child} <= await _valid(lab.store_path)


@pytest.mark.parametrize("names", ["substituters", "trusted-public-keys"])
async def test_a_client_with_its_own_list_still_gets_an_evicted_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, names: str
) -> None:
    """The bucket holds what the collector took from the local store, so it serves every client.

    A client's `substituters` replaces the daemon's list, and `extra-substituters` sends one too; its
    `trusted-public-keys` replaces the daemon's keys. Neither names the bucket or its key.
    """
    lab = await _lab(tmp_path)
    public = await _key(tmp_path)
    config = for_test_store(substituters=(), trusted_public_keys=(public,), require_sigs=True)
    empty = tmp_path / "empty-cache"
    empty.mkdir()  # noqa: ASYNC240 -- test setup
    secret = await _nix("key", "generate-secret", "--key-name", "other")
    other = await _nix("key", "convert-secret-to-public", stdin=secret.encode())
    overrides = {"substituters": f"file://{empty}", "trusted-public-keys": other}
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, nix_config=config) as (server, tier):
            await tier.upload_closure({StorePath(lab.parent)})
            await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
            assert not {lab.parent, lab.child} & await _valid(lab.store_path)
            async with _client(tmp_path) as conn:
                await conn.call(set_options(overrides={names: overrides[names]}), raise_on_error=True)
                await conn.call(EnsurePathRequest(path=StorePath(lab.parent)), raise_on_error=True)
    assert {lab.parent, lab.child} <= await _valid(lab.store_path)


async def test_a_path_gone_before_its_upload_skips_only_itself(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The plan closes before the uploads start, so another delete can take a planned path first.

    The round goes on with the rest: one gone path must not end the cold loop, nor the daemon with it.
    """
    lab = await _lab(tmp_path)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9) as (server, tier):
            upload = tier.upload_closure

            async def racing(paths: set[StorePath]) -> object:
                if StorePath(lab.stale) in paths:
                    await _nix("store", "delete", "--store", str(lab.store_path), lab.stale)
                return await upload(paths)

            monkeypatch.setattr(tier, "upload_closure", racing)
            await Collector(server.ctx).upload_ahead(tier, 2**62, 1.0)
            assert await tier.covered({lab.stale, lab.parent, lab.child, lab.fresh}) == {
                lab.parent,
                lab.child,
                lab.fresh,
            }


async def test_the_cold_loop_outlives_a_failed_round(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The loop the server starts: a round the bucket refuses is skipped, and the next one uploads."""
    lab = await _lab(tmp_path)
    rounds: list[str] = []
    second = anyio.Event()
    upload_ahead = Collector.upload_ahead

    async def flaky(collector: Collector, tier: ColdTier, budget: int, pressure: float) -> int:
        if not rounds:
            rounds.append("refused")
            raise CacheFault("refused by the test")
        uploaded = await upload_ahead(collector, tier, budget, pressure)
        rounds.append("uploaded")
        second.set()
        return uploaded

    monkeypatch.setattr(Collector, "upload_ahead", flaky)
    dead = {lab.stale, lab.parent, lab.child}
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, cold_interval=0.5) as (_server, tier):
            with anyio.fail_after(30):
                await second.wait()
            assert rounds == ["refused", "uploaded"]
            assert dead <= await tier.covered(dead | {lab.rooted})


@pytest.mark.parametrize("verify", [True, False], ids=["verify", "trust-the-record"])
async def test_a_path_the_bucket_lost_stays(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, verify: bool) -> None:
    """Someone removes `parent`'s narinfo from the bucket after the upload recorded it.

    With `gc_cold_verify` the pass asks the bucket, deletes nothing and forgets the row; the next pass keeps
    `parent` and the `child` it names. The control: trusting the record deletes both, and `parent` is lost.
    """
    lab = await _lab(tmp_path)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, verify=verify) as (server, tier):
            await tier.upload_closure({StorePath(lab.parent), StorePath(lab.stale)})
            s3 = botocore.session.get_session().create_client(
                "s3", endpoint_url=bucket.endpoint, region_name="us-east-1"
            )
            s3.delete_object(Bucket="cache", Key=f"{StorePath(lab.parent).hash_part()}.narinfo")
            first = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
            if verify:
                assert first.store_paths == set()
                assert await tier.covered({lab.parent, lab.child, lab.stale}) == {lab.child, lab.stale}
                second = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
                assert {str(path) for path in second.store_paths} == {lab.stale}
                assert {lab.parent, lab.child} <= await _valid(lab.store_path)
            else:
                assert {str(path) for path in first.store_paths} == {lab.stale, lab.parent, lab.child}
                assert lab.parent not in await _valid(lab.store_path)
