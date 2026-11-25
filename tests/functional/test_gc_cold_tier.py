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
from pynixd import Server, cold_tier, metrics
from pynixd.cold_tier import ColdTier, ColdUploader
from pynixd.config import S3BinaryCacheSpec
from pynixd.connection import Connection
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.exceptions import BackendError, CacheFault
from pynixd.gc import Collector
from pynixd.store import LocalDBStore, S3BinaryCacheStore, Store
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from pynixd_store.gc_sql import Bound, SqlPlan
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
    verify: bool = True,
    workers: int = 0,
    watermark: float | None = None,
    poll: float = 60.0,
) -> AsyncIterator[tuple[Server, ColdTier]]:
    """A pynixd over the lab and a bucket. With no *workers* nothing uploads unless the test does."""
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
        stores=stores,
        unix_path=_socket(tmp_path),
        ssh_port=None,
        http_port=None,
        gc_cold_workers=workers,
        gc_high_watermark=watermark,
        gc_poll_interval=poll,
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
async def _uploading(tier: ColdTier, workers: int = 4) -> AsyncIterator[ColdUploader]:
    """An uploader over *tier*, run by the test rather than the server."""
    uploader = ColdUploader(tier, workers=workers, poll=3600)
    async with anyio.create_task_group() as tg:
        tg.start_soon(uploader.run)
        yield uploader
        tg.cancel_scope.cancel()


async def _cover(uploader: ColdUploader, paths: set[str]) -> None:
    """Urge *paths* and wait until none is left to cover."""
    pending = await uploader.urge(paths)
    with anyio.fail_after(60):
        while uploader.queue.pending(pending) and not uploader.stopped:
            await uploader.progress()


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


async def test_a_drive_waits_for_the_uploader_and_then_deletes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Over the watermark the server's loop drives at once. Its first pass finds nothing covered and frees nothing;
    the drive then puts the window first in the upload queue, waits, and its next pass deletes every dead path.

    Uploads wait for that first pass, so the drive must wait for them: no cooldown, no upload round.
    """
    lab = await _lab(tmp_path)
    dead = {lab.stale, lab.parent, lab.child, lab.fresh}
    deleted: set[str] = set()
    nothing_freed = anyio.Event()
    done = anyio.Event()
    run = Collector.run
    stage = S3BinaryCacheStore.stage

    async def spy(self: Collector, action: PynixdGCAction, *args: object, **kwargs: object) -> object:
        resp = await run(self, action, *args, **kwargs)  # type: ignore[arg-type]  # the spy passes them through
        if action == PynixdGCAction.EXECUTE:
            if not resp.store_paths:
                nothing_freed.set()
            deleted.update(str(path) for path in resp.store_paths)
            if dead <= deleted:
                done.set()
        return resp

    async def after_the_first_pass(self: S3BinaryCacheStore, local: Store, path: StorePath) -> object:
        await nothing_freed.wait()
        return await stage(self, local, path)

    monkeypatch.setattr(Collector, "run", spy)
    monkeypatch.setattr(S3BinaryCacheStore, "stage", after_the_first_pass)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9, workers=4, watermark=1e-9, poll=0.2):
            with anyio.fail_after(60):
                await done.wait()
    assert deleted == dead
    assert await _valid(lab.store_path) == {lab.rooted}


async def test_the_cold_window_names_what_the_pressed_pass_takes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No budget names nothing; a budget over the plan names all of it, and once it is up the pass takes it."""
    lab = await _lab(tmp_path)
    dead = {lab.stale, lab.parent, lab.child, lab.fresh}
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9) as (server, tier):
            assert await Collector(server.ctx).cold_window(tier, 0, 1.0) == []
            window = await Collector(server.ctx).cold_window(tier, 2**62, 1.0)
            assert set(window) == dead
            async with _uploading(tier) as uploader:
                await _cover(uploader, set(window))
            assert dead <= await tier.covered(dead | {lab.rooted})
            deleted = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
    assert {str(path) for path in deleted.store_paths} == dead


async def test_the_cold_window_takes_the_referrers_a_covered_window_waits_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The budget reaches only `child`, already up; `parent` names it, so the window names `parent`.

    Without it a pass keeps both, and every later round finds its window covered and uploads nothing:
    sg1, 2026-11-25, 688 covered paths filled the budget while 101,940 planned paths stayed.
    """
    lab = await _lab(tmp_path)
    weighed = SqlPlan.weighed

    async def child_first(
        self: SqlPlan, *, pressure: float, now: int, bound: Bound | None, limit: int | None
    ) -> list[tuple[str, int, int]]:
        rows = await weighed(self, pressure=pressure, now=now, bound=bound, limit=limit)
        return sorted(rows, key=lambda row: row[0] != lab.child)

    monkeypatch.setattr(SqlPlan, "weighed", child_first)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9) as (server, tier):
            await tier.upload_closure({StorePath(lab.child)})
            assert await Collector(server.ctx).cold_window(tier, 1, 1.0) == [lab.parent]


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
    """Another delete takes a queued path before its upload: the uploader goes on with the rest."""
    lab = await _lab(tmp_path)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9) as (_server, tier):
            stage = tier.cold.stage

            async def racing(local: Store, path: StorePath) -> object:
                if str(path) == lab.stale:
                    await _nix("store", "delete", "--store", str(lab.store_path), lab.stale)
                return await stage(local, path)

            monkeypatch.setattr(tier.cold, "stage", racing)
            async with _uploading(tier) as uploader:
                await _cover(uploader, {lab.stale, lab.parent, lab.child, lab.fresh})
                assert lab.stale not in uploader.queue
            assert await tier.covered({lab.stale, lab.parent, lab.child, lab.fresh}) == {
                lab.parent,
                lab.child,
                lab.fresh,
            }


async def test_the_uploader_retries_a_transient_fault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The uploader the server starts: a NAR the bucket fails goes up on the next try."""
    lab = await _lab(tmp_path)
    faults: list[str] = []
    stage = S3BinaryCacheStore.stage

    async def flaky(self: S3BinaryCacheStore, local: Store, path: StorePath) -> object:
        if not faults:
            faults.append(str(path))
            raise CacheFault("failed by the test", status=503)
        return await stage(self, local, path)

    monkeypatch.setattr(S3BinaryCacheStore, "stage", flaky)
    dead = {lab.stale, lab.parent, lab.child}
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, workers=2) as (server, tier):
            uploader = server.cold_uploader
            assert uploader is not None
            await _cover(uploader, dead)
            assert len(faults) == 1
            assert dead <= await tier.covered(dead | {lab.rooted})


async def test_a_narinfo_waits_for_its_references(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`child`'s NAR is held back while `parent`'s goes up: `parent` gets no narinfo and no record until then.

    Otherwise a client could fetch `parent` from the bucket and find no `child` to go with it.
    """
    lab = await _lab(tmp_path)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint) as (_server, tier):
            stage = tier.cold.stage
            release = anyio.Event()

            async def hold_child(local: Store, path: StorePath) -> object:
                if str(path) == lab.child:
                    await release.wait()
                return await stage(local, path)

            monkeypatch.setattr(tier.cold, "stage", hold_child)
            async with _uploading(tier) as uploader:
                gated = anyio.Event()
                staged = uploader.queue.staged

                def spy(path: str, uncovered: set[str]) -> list[str]:
                    parked = staged(path, uncovered)
                    if path == lab.parent:
                        gated.set()
                    return parked

                monkeypatch.setattr(uploader.queue, "staged", spy)
                await uploader.urge({lab.parent})
                with anyio.fail_after(60):
                    await gated.wait()
                # Gated, not publishable: a worker would otherwise put its narinfo before `child`'s NAR is up.
                assert uploader.queue.depth()["gated"] == 1
                assert uploader.queue.depth()["publishable"] == 0
                s3 = botocore.session.get_session().create_client(
                    "s3", endpoint_url=bucket.endpoint, region_name="us-east-1"
                )
                narinfos = {
                    o["Key"] for o in s3.list_objects_v2(Bucket="cache").get("Contents", []) if ".narinfo" in o["Key"]
                }
                assert f"{StorePath(lab.parent).hash_part()}.narinfo" not in narinfos
                assert await tier.covered({lab.parent, lab.child}) == set()
                release.set()
                await _cover(uploader, {lab.parent})
            assert await tier.covered({lab.parent, lab.child}) == {lab.parent, lab.child}


async def test_a_restart_rebuilds_the_queue_from_the_record(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A new uploader queues every valid path the record does not cover, and only those."""
    lab = await _lab(tmp_path)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint) as (_server, tier):
            await tier.upload_closure({StorePath(lab.child)})
            uploader = ColdUploader(tier, workers=1, poll=3600)
            assert await uploader.refill() > 0
            assert lab.child not in uploader.queue
            assert {lab.parent, lab.stale, lab.fresh, lab.rooted} == uploader.queue.pending(
                {lab.parent, lab.stale, lab.fresh, lab.rooted, lab.child}
            )
            assert await uploader.refill() == 0


async def test_refusals_across_paths_stop_the_tier(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A bucket that refuses every write: the uploader parks each path, then stops, and covers nothing."""
    lab = await _lab(tmp_path)
    monkeypatch.setattr(cold_tier, "STOP_AFTER_REFUSALS", 2)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        s3 = botocore.session.get_session().create_client("s3", endpoint_url=bucket.endpoint, region_name="us-east-1")
        s3.put_object(Bucket="cache", Key="nix-cache-info", Body=b"StoreDir: /nix/store\n")
        access, secret = bucket.read_only
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", access)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret)
        async with _pynixd(lab, tmp_path, bucket.endpoint) as (_server, tier):
            async with _uploading(tier, workers=1) as uploader:
                with anyio.fail_after(60):
                    while not uploader.stopped:
                        await uploader.progress()
                assert uploader.queue.depth()["parked"] >= 2
            assert metrics.COLD_STOPPED._value.get() == 1
            assert await tier.covered({lab.parent, lab.child, lab.stale}) == set()


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
