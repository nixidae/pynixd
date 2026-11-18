"""The age rule with a cold tier: a path leaves once its closure is in the bucket. nixidae/nixidae#1417.

The lab is `test_gc_max_age`'s, whose dry-run names `stale`, `parent` and `child` with no cold tier:
that is the control of every plan here. Stock `nix copy --from` the bucket is the oracle that a
deleted path comes back.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_path import StorePath
from pynixd import Server, metrics
from pynixd.cold_tier import ColdTier
from pynixd.config import S3BinaryCacheSpec
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.gc import Collector
from pynixd.store import LocalDBStore, Store
from tests._conftest.s3_bucket import s3_bucket
from tests.conftest import NIX_BIN, make_test_spec, run_subproc
from tests.functional.test_gc_max_age import MAX_AGE, STALE_AGE, _backdate, _freeze_tracker, _Lab, _lab, _valid

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


async def _nix(*args: str, stdin: bytes | None = None) -> str:
    _rc, stdout, _stderr, _both = await run_subproc([str(NIX_BIN), *args], verbose=False, stdin=stdin)
    return stdout.strip()


@asynccontextmanager
async def _pynixd(
    lab: _Lab, tmp_path: Path, endpoint: str, target: float | None = None
) -> AsyncIterator[tuple[Server, ColdTier]]:
    key = tmp_path / "cold.sec"
    key.write_text(await _nix("key", "generate-secret", "--key-name", "cold-test"))  # noqa: ASYNC240 -- test setup
    spec = make_test_spec(
        store_id="local",
        store_path=lab.store_path,
        no_probe=True,
        postgres=None,
        gc_max_age=MAX_AGE,
        gc_target_usage=target,
        gc_allow_execute=True,
    )
    cold = S3BinaryCacheSpec(bucket="cache", endpoint=endpoint, region="us-east-1", secret_key=key, gc_cold=True)
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec), StoreId("cold"): cold.to_store("cold")}
    async with Server(stores=stores, unix_path=tmp_path / "pynixd.sock", ssh_port=None, http_port=None) as server:
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


async def test_over_the_target_the_rest_goes_and_is_counted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A target no disk meets: the covered `stale` goes first and counts zero, then every other dead path.

    Pressure lifts the age (`plan`), so `fresh` is dead enough to go too.
    """
    lab = await _lab(tmp_path)
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9) as (server, tier):
            await tier.upload_closure({StorePath(lab.stale)})
            before = metrics.GC_DELETED_UNCOVERED._value.get()
            deleted = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
            counted = metrics.GC_DELETED_UNCOVERED._value.get() - before
    assert {str(path) for path in deleted.store_paths} == {lab.stale, lab.parent, lab.child, lab.fresh}
    assert counted == 3
    assert await _valid(lab.store_path) == {lab.rooted}


async def test_upload_ahead_covers_what_the_pressed_pass_takes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No budget uploads nothing; a budget over the plan covers it, and the pressed pass then counts none."""
    lab = await _lab(tmp_path)
    dead = {lab.stale, lab.parent, lab.child, lab.fresh}
    with s3_bucket(monkeypatch, tmp_path / "xdg-cache") as bucket:
        async with _pynixd(lab, tmp_path, bucket.endpoint, target=1e-9) as (server, tier):
            assert await Collector(server.ctx).upload_ahead(tier, 0, 1.0) == 0
            assert await tier.covered(dead) == set()
            assert await Collector(server.ctx).upload_ahead(tier, 2**62, 1.0) == len(dead)
            assert await tier.covered(dead | {lab.rooted}) == dead
            before = metrics.GC_DELETED_UNCOVERED._value.get()
            deleted = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
            counted = metrics.GC_DELETED_UNCOVERED._value.get() - before
    assert {str(path) for path in deleted.store_paths} == dead
    assert counted == 0
