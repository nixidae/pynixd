"""The collector's cold tier: an `s3://` store the local store's paths go to before they leave. nixidae/nixidae#1417.

One invariant: a narinfo in the bucket means its whole closure is in the bucket or in a `gc_stable`
store. `upload_closure` keeps it by uploading references first and stopping at the first refusal, so
a referrer never goes up over a missing reference.

`PynixdHeldBy` records each answer: the bucket's `UPLOADED` or `PRESENT`, and a stable store's hit.
The collector reads that record, not the network, for every planned path.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Final

import anyio
import structlog

from nix_daemon_protocol import QueryValidPathsRequest, StorePath
from nix_daemon_protocol.query_path_infos import QueryPathInfosRequest
from pynixd_store.gc import GarbagePlanner
from pynixd_store.gc_sql import referrer_first
from pynixd_store.local_store_db import DatabaseError

from . import metrics
from .cold_queue import Lane, Stage, UploadQueue
from .daemon_extensions import QueryClosureRequest
from .exceptions import CacheFault, PathGone
from .store.local_db import LocalDBStore
from .store.s3_binary_cache import S3BinaryCacheStore
from .store.s3_upload import Uploaded

if TYPE_CHECKING:
    from collections.abc import Collection

    from nix_daemon_protocol.query_path_infos import QueryPathInfosResponse
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd_store.local_store_db import LocalStoreDB

    from .cold_queue import Work
    from .context import PynixdContext
    from .daemon_extensions.query_closure import QueryClosureResponse
    from .store.base import Store
    from .store.local_daemon import LocalStore
    from .store.s3_binary_cache import StagedNar

log = structlog.get_logger(__name__)

VERIFY_CONCURRENCY: Final = 16
"""HEAD requests in flight while a pass asks the bucket for its recorded paths."""


@dataclass(frozen=True, slots=True)
class ClosureUpload:
    """What one `upload_closure` found and did, in paths."""

    closure: int
    recorded: int
    """Held already, by the record."""
    stable: int
    """Held by a stable store, recorded now."""
    uploaded: int
    present: int
    """The bucket had it and the record did not."""


class ColdTier:
    def __init__(self, cold: S3BinaryCacheStore, stable: list[Store], local: LocalStore | LocalDBStore) -> None:
        self.cold = cold
        self.stable = stable
        self.local = local

    @classmethod
    def of(cls, ctx: PynixdContext) -> ColdTier | None:
        """The configured cold tier, or `None`; `ValueError` for more than one."""
        cold = [store for store in ctx.stores.values() if isinstance(store, S3BinaryCacheStore) and store.gc_cold]
        if not cold:
            return None
        if len(cold) > 1:
            raise ValueError(f"one gc_cold store at most, and {', '.join(sorted(s.store_id for s in cold))} set it")
        stable = [store for store in ctx.stores.values() if store.gc_stable and store is not cold[0]]
        return cls(cold[0], stable, ctx.local_store)

    @property
    def keys(self) -> set[str]:
        """The `PynixdHeldBy` stores whose rows cover a path."""
        return {self.cold.held_key, *(store.held_key for store in self.stable)}

    def _db(self) -> LocalStoreDB:
        db = GarbagePlanner(self.local.layout, self.local.db).plan_database()
        if db is None:
            raise RuntimeError(f"the cold tier needs pynixd's tables in {self.local.store_id}'s database")
        return db

    async def covered(self, paths: Collection[str]) -> set[str]:
        """The paths of *paths* the record shows held by the cold tier or a stable store."""
        return await self._db().held_by.held(self.keys, paths)

    async def missing(self, paths: Collection[str]) -> set[str]:
        """The paths of *paths* that only the bucket's record covers and the bucket no longer holds.

        Their rows go, so the next plan keeps them. With `gc_cold_verify` off the record stands unasked.
        Raises `CacheFault` when the bucket cannot answer.
        """
        if not self.cold.gc_cold_verify:
            return set()
        db = self._db()
        stable = await db.held_by.held({store.held_key for store in self.stable}, paths)
        ask = sorted(await db.held_by.held({self.cold.held_key}, paths) - stable)
        gone: set[str] = set()
        limiter = anyio.CapacityLimiter(VERIFY_CONCURRENCY)

        async def one(path: str) -> None:
            async with limiter:
                if not await self.cold.holds(StorePath(path)):
                    gone.add(path)

        async with anyio.create_task_group() as tg:
            for path in ask:
                tg.start_soon(one, path)
        await db.held_by.forget(self.cold.held_key, gone)
        log.info("cold_verified", store_id=self.cold.store_id, asked=len(ask), gone=len(gone))
        return gone

    async def _unmarked(self, request: WireRequest) -> Any:  # noqa: ANN401 -- `Store.execute` answers the request's own response type
        # Reading a candidate must not freshen it: the access date is what ages it out.
        if isinstance(self.local, LocalDBStore):
            return await self.local.execute(request, mark=False)
        return await self.local.execute(request)

    async def upload_closure(self, paths: Collection[StorePath]) -> ClosureUpload:
        """Put the closure of *paths* in the bucket, minus what a stable store holds, references first.

        Raises `CacheFault` at the first refusal, and `PathGone` when a path left the local store; each path
        uploaded before it stays recorded.
        """
        db = self._db()
        answer: QueryClosureResponse = await self._unmarked(QueryClosureRequest(paths=set(paths)))
        closure = {str(path) for path in answer.paths}
        if not {str(path) for path in paths} <= closure:
            raise PathGone(f"{self.local.store_id} answered a closure without its seeds")
        recorded = await db.held_by.held(self.keys, closure)
        todo = closure - recorded
        now = int(time.time())
        stable = 0
        for store in self.stable:
            if not todo:
                break
            held = {str(path) for path in await _held_by(store, todo)}
            await db.held_by.record(store.held_key, held, now)
            stable += len(held)
            todo -= held
        infos: QueryPathInfosResponse = await self._unmarked(QueryPathInfosRequest(paths={StorePath(p) for p in todo}))
        references = {str(info.path): {str(ref) for ref in info.info.references} for info in infos.infos}
        if references.keys() != todo:
            raise PathGone(f"{self.local.store_id} answered no path info for {sorted(todo - references.keys())[:3]}")
        counts = {Uploaded.UPLOADED: 0, Uploaded.PRESENT: 0}
        for path in reversed(referrer_first(sorted(todo), references)):
            outcome = await self.cold.upload(self.local, StorePath(path))
            await db.held_by.record(self.cold.held_key, (path,), int(time.time()))
            counts[outcome] += 1
        report = ClosureUpload(
            closure=len(closure),
            recorded=len(recorded),
            stable=stable,
            uploaded=counts[Uploaded.UPLOADED],
            present=counts[Uploaded.PRESENT],
        )
        log.info("cold_closure_uploaded", store_id=self.cold.store_id, seeds=len(paths), **asdict(report))
        return report


STOP_AFTER_REFUSALS: Final = 10
"""Distinct paths refused for good with no upload between them: then the bucket or its credentials are wrong."""

_BACKOFF_FIRST_S: Final = 1.0
_BACKOFF_MAX_S: Final = 300.0


class ColdUploader:
    """The cold tier's upload queue and its workers: every uncovered local path goes up. nixidae/nixidae#1600.

    The queue is the record's complement: each valid path that `PynixdHeldBy` shows in no covering store.
    `run` fills it from the database, then reads each new row. `urge` puts what the collector waits on first.
    A narinfo goes up only once the record covers each reference, and the record takes a path only once its
    narinfo is in. Nothing asks the bucket before an upload: a second upload writes the same objects.
    """

    def __init__(self, tier: ColdTier, *, workers: int, poll: float) -> None:
        self.tier = tier
        self.queue = UploadQueue()
        self.stopped = False
        self._workers = workers
        self._poll = poll
        self._last_row = 0
        self._staged: dict[str, StagedNar] = {}
        self._wake = anyio.Event()
        self._backoff = 0.0
        self._resume_at = 0.0
        self._refused: set[str] = set()
        self._scope: anyio.CancelScope | None = None

    async def run(self) -> None:
        log.info("cold_uploader_started", store_id=self.tier.cold.store_id, workers=self._workers)
        async with anyio.create_task_group() as tg:
            self._scope = tg.cancel_scope
            await self.refill()
            tg.start_soon(self._follow)
            for _ in range(self._workers):
                tg.start_soon(self._work)

    async def refill(self) -> int:
        """Queue each uncovered path registered since the last fill; answers how many are new."""
        rows = await self.tier._db().held_by.uncovered(self.tier.keys, self._last_row)
        if rows:
            self._last_row = rows[-1][0]
        added = self.queue.add(path for _, path in rows)
        if added:
            log.info("cold_queue_filled", added=added, **self.queue.depth())
        self._changed()
        return added

    async def urge(self, paths: Collection[str]) -> set[str]:
        """Queue the uncovered closure of *paths* first, references before referrers.

        Answers the paths of *paths* still to cover: `queue.pending` of it shrinks as `progress` wakes.
        """
        if not paths:
            return set()
        answer: QueryClosureResponse = await self.tier._unmarked(
            QueryClosureRequest(paths={StorePath(path) for path in paths})
        )
        closure = {str(path) for path in answer.paths}
        self.queue.add(await self.tier._db().held_by.uncovered_of(self.tier.keys, closure), Lane.URGENT)
        self._changed()
        return self.queue.pending(paths)

    async def progress(self) -> None:
        """Wait until the queue changes: a path covered, dropped or parked, or the tier stopped."""
        if not self.stopped:
            await self._wake.wait()

    async def _follow(self) -> None:
        while True:
            # Polled: a nix-daemon, or the other process of a hand-over, registers paths this process never
            # hears of. The query reads only the rows above the last one it saw.
            await anyio.sleep(self._poll)
            await self.refill()

    def _changed(self) -> None:
        for state, count in self.queue.depth().items():
            metrics.COLD_QUEUE_PATHS.labels(state=state).set(count)
        self._wake.set()
        self._wake = anyio.Event()

    def _covers(self, path: str) -> None:
        self.queue.covered(path)
        self._backoff = 0.0
        self._refused.clear()

    async def _work(self) -> None:
        while not self.stopped:
            if (delay := self._resume_at - time.monotonic()) > 0:
                # A bucket that fails answers no sooner for being asked again.
                await anyio.sleep(delay)
            work = self.queue.take()
            if work is None:
                await self._wake.wait()
                continue
            try:
                if work.stage is Stage.NAR:
                    await self._nar(work.path)
                else:
                    await self._publish(work.path)
            except PathGone:
                log.info("cold_path_gone", path=work.path)
                self._parked(self.queue.drop(work.path))
            except CacheFault as e:
                if e.transient:
                    self._fault(work, e)
                else:
                    self._refuse(work.path, e)
            except DatabaseError as e:
                # A busy or locked database: the record could not be read or written, so the work stands.
                self._fault(work, e)
            finally:
                self._changed()

    async def _nar(self, path: str) -> None:
        db = self.tier._db()
        for store in self.tier.stable:
            if await _held_by(store, {path}):
                await db.held_by.record(store.held_key, (path,), int(time.time()))
                metrics.COLD_UPLOADED.labels(by="stable").inc()
                self._covers(path)
                return
        staged = await self.tier.cold.stage(self.tier.local, StorePath(path))
        metrics.COLD_UPLOADED_BYTES.inc(staged.digest.size)
        references = {str(ref) for ref in staged.info.info.references} - {path}
        uncovered = references - await db.held_by.held(self.tier.keys, references)
        self._staged[path] = staged
        self._parked(self.queue.staged(path, uncovered))

    async def _publish(self, path: str) -> None:
        await self.tier.cold.publish(self._staged[path])
        await self.tier._db().held_by.record(self.tier.cold.held_key, (path,), int(time.time()))
        del self._staged[path]
        metrics.COLD_UPLOADED.labels(by="bucket").inc()
        self._covers(path)

    def _parked(self, paths: list[str]) -> None:
        for path in paths:
            self._staged.pop(path, None)
        if paths:
            log.warning("cold_paths_parked", paths=len(paths), first=paths[:3])

    def _fault(self, work: Work, fault: CacheFault | DatabaseError) -> None:
        metrics.COLD_UPLOAD_FAULTS.labels(kind="transient").inc()
        self.queue.retry(work)
        self._backoff = min(max(self._backoff * 2, _BACKOFF_FIRST_S), _BACKOFF_MAX_S)
        self._resume_at = max(self._resume_at, time.monotonic() + self._backoff)
        log.warning("cold_upload_fault", path=work.path, backoff=self._backoff, error=str(fault))

    def _refuse(self, path: str, fault: CacheFault) -> None:
        metrics.COLD_UPLOAD_FAULTS.labels(kind="permanent").inc()
        log.error("cold_upload_refused", path=path, status=fault.status, error=str(fault))
        self._parked(self.queue.park(path))
        self._refused.add(path)
        if len(self._refused) >= STOP_AFTER_REFUSALS:
            self.stopped = True
            metrics.COLD_STOPPED.set(1)
            log.error("cold_tier_stopped", store_id=self.tier.cold.store_id, refused=len(self._refused))
            if self._scope is not None:
                self._scope.cancel()


async def _held_by(store: Store, paths: set[str]) -> set[StorePath]:
    """What *store* answers it holds; a store that cannot answer holds nothing."""
    try:
        valid: set[StorePath] = (
            await store.execute(QueryValidPathsRequest(paths={StorePath(path) for path in paths}))
        ).paths
    except Exception:
        log.warning("cold_stable_query_failed", store_id=str(store.store_id), exc_info=True)
        return set()
    return valid
