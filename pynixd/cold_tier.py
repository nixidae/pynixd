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
from typing import TYPE_CHECKING, Any

import structlog

from nix_daemon_protocol import QueryValidPathsRequest, StorePath
from nix_daemon_protocol.query_path_infos import QueryPathInfosRequest
from pynixd_store.gc import GarbagePlanner
from pynixd_store.gc_sql import referrer_first

from .daemon_extensions import QueryClosureRequest
from .store.local_db import LocalDBStore
from .store.s3_binary_cache import S3BinaryCacheStore
from .store.s3_upload import Uploaded

if TYPE_CHECKING:
    from collections.abc import Collection

    from nix_daemon_protocol.query_path_infos import QueryPathInfosResponse
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd_store.local_store_db import LocalStoreDB

    from .context import PynixdContext
    from .daemon_extensions.query_closure import QueryClosureResponse
    from .store.base import Store
    from .store.local_daemon import LocalStore

log = structlog.get_logger(__name__)


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

    async def _unmarked(self, request: WireRequest) -> Any:  # noqa: ANN401 -- `Store.execute` answers the request's own response type
        # Reading a candidate must not freshen it: the access date is what ages it out.
        if isinstance(self.local, LocalDBStore):
            return await self.local.execute(request, mark=False)
        return await self.local.execute(request)

    async def upload_closure(self, paths: Collection[StorePath]) -> ClosureUpload:
        """Put the closure of *paths* in the bucket, minus what a stable store holds, references first.

        Raises `CacheFault` at the first refusal; each path uploaded before it stays recorded.
        """
        db = self._db()
        answer: QueryClosureResponse = await self._unmarked(QueryClosureRequest(paths=set(paths)))
        closure = {str(path) for path in answer.paths}
        if not {str(path) for path in paths} <= closure:
            raise ValueError(f"{self.local.store_id} answered a closure without its seeds")
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
            raise ValueError(f"{self.local.store_id} answered no path info for {sorted(todo - references.keys())[:3]}")
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
