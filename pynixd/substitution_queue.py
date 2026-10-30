"""Scheduler-owned substitution queue and availability cache."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import anyio
import structlog
from cachetools import TTLCache

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from nix_daemon_protocol.valid_path_info import ValidPathInfo

from . import metrics
from .exceptions import NarGone, OpNotImplementedError, leaves
from .serde import AddToStoreNarRequest, NarFromPathRequest, QueryPathInfoRequest, StorePath
from .serde.context import ReadContext, WriteContext
from .store import DaemonStore, is_http_binary_cache

if TYPE_CHECKING:
    from collections.abc import Iterable

    from nix_daemon_protocol.ids import StoreId

    from .config import PynixdSettings
    from .context import PynixdContext
    from .store import Store
    from .store.http_binary_cache import HTTPBinaryCacheStore, HTTPNarInfo
    from .store_path import StorePath

log = structlog.get_logger(__name__)
_NAR_CHUNK_SIZE = 1024 * 256


@dataclass(frozen=True)
class SubstitutionQueryResult:
    """Cached path-info query result for one path on one substituter."""

    store_id: StoreId
    path_info: ValidPathInfo | None
    query_succeeded: bool
    download_size: int = 0
    """The narinfo's `FileSize`; 0 for a store that serves no narinfo, as `misc.cc:139` reads it."""

    @property
    def found(self) -> bool:
        return self.path_info is not None


@dataclass(frozen=True)
class SubstitutionAvailability:
    """Fast availability answer for planning and graph traversal."""

    available: bool
    nar_size: int | None = None
    download_size: int | None = None
    references: frozenset[str] = frozenset()
    """What the path names, so a plan counts its closure (`misc.cc:318`)."""

    @classmethod
    def unavailable(cls) -> SubstitutionAvailability:
        return cls(available=False)

    @classmethod
    def from_query_result(cls, result: SubstitutionQueryResult) -> SubstitutionAvailability:
        if result.path_info is None:
            return cls.unavailable()
        info = result.path_info.info
        return cls(
            available=True,
            nar_size=info.nar_size,
            download_size=result.download_size,
            references=frozenset(str(reference) for reference in info.references),
        )


def _trusted_source(store: Store) -> bool:
    """Whether a substitute from *store* skips the signature check.

    NIX-DEVIATION (#27): `substitution-goal.cc:116` skips an unsigned
    substitute before the download; pynixd learns it from the daemon's
    refusal after the download. The cost is that download; reverse it once
    pynixd reads `trusted-public-keys` itself. A builder or peer store is
    trusted, by User's decision: its outputs carry no signature.
    nixidae/nixidae#1008.
    """
    return not is_http_binary_cache(store) or store.trusted


@dataclass(frozen=True)
class SubstitutionCandidate:
    """Selected substituter source for one path."""

    store: Store
    path_info: ValidPathInfo
    http_narinfo: HTTPNarInfo | None = None


@dataclass(frozen=True)
class SubstitutionImportResult:
    """Result of importing one path into the local store."""

    substituted: bool
    path: StorePath
    candidate: SubstitutionCandidate | None = None
    error: str = ""
    gone: bool = False
    """The substituter no longer has the NAR; Nix treats that as no substitute (`substitution-goal.cc:270`)."""


@dataclass(frozen=True)
class UpstreamNarinfo:
    """One upstream's `.narinfo` answer, raw text and winner attached.

    The HTTP cache serves the winner's text with only the URL line
    rewritten, so the race keeps the bytes the parse would throw away.
    Issue #85.
    """

    raw: str
    store: Store
    info: HTTPNarInfo


@dataclass
class _UpstreamNarinfoRace:
    """Settles once: first positive wins, stragglers keep recording.

    A waiter returns on the first answer without cancelling the rest, so
    a later NAR redirect still ranks every store that answered. Issue #85.
    """

    remaining: int
    event: anyio.Event = field(default_factory=anyio.Event)
    winner: UpstreamNarinfo | None = None
    lock: anyio.Lock = field(default_factory=anyio.Lock)


class SubstitutionHealthLog:
    """Fixed-size per-store substitution query health log."""

    def __init__(self, settings: PynixdSettings) -> None:
        self._entries: deque[bool] = deque(maxlen=settings.substitution_health_window)
        self._min_entries = max(
            1,
            int(settings.substitution_health_window * settings.substitution_health_min_fill_ratio),
        )
        self._min_success_ratio = settings.substitution_health_min_success_ratio

    def record(self, *, query_succeeded: bool) -> None:
        self._entries.append(query_succeeded)

    @property
    def is_healthy(self) -> bool:
        if len(self._entries) < self._min_entries:
            return True
        successes = sum(1 for entry in self._entries if entry)
        return successes / len(self._entries) > self._min_success_ratio


class SubstitutionQueue:
    """Owns substitution availability state for the scheduler singleton."""

    def __init__(self, ctx: PynixdContext) -> None:
        self.ctx = ctx
        settings = ctx.settings
        self.positive = cast(
            "TTLCache[StorePath, dict[StoreId, SubstitutionQueryResult], float]",
            TTLCache(maxsize=settings.substitution_cache_maxsize, ttl=settings.substitution_positive_ttl),
        )
        self.negative = cast(
            "TTLCache[StorePath, dict[StoreId, SubstitutionQueryResult], float]",
            TTLCache(maxsize=settings.substitution_cache_maxsize, ttl=settings.substitution_negative_ttl),
        )
        self.health: dict[StoreId, SubstitutionHealthLog] = {}
        self._probe_tasks: set[asyncio.Task[None]] = set()
        self._active_imports: dict[StorePath, asyncio.Task[SubstitutionImportResult]] = {}
        # Created on first use: a limiter needs the running loop.
        self._import_limit: anyio.CapacityLimiter | None = None
        self._import_slots = 16
        self._http_races: dict[str, _UpstreamNarinfoRace] = {}
        self._http_hash_paths = cast(
            "TTLCache[str, StorePath, float]",
            TTLCache(maxsize=settings.substitution_cache_maxsize, ttl=settings.substitution_positive_ttl),
        )
        self._http_hash_negative = cast(
            "TTLCache[str, None, float]",
            TTLCache(maxsize=settings.substitution_cache_maxsize, ttl=settings.http_upstream_negative_ttl),
        )
        self._negative_hash_paths = cast(
            "TTLCache[str, StorePath, float]",
            TTLCache(maxsize=settings.substitution_cache_maxsize, ttl=settings.substitution_negative_ttl),
        )
        """The path behind each hash `negative` holds, so a `.narinfo` request can find it."""

    async def close(self) -> None:
        for task in self._probe_tasks:
            task.cancel()
        for task in list(self._probe_tasks):
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await task

    async def can_substitute(self, path: StorePath) -> SubstitutionAvailability:
        cached = self._cached_positive(path)
        if cached is not None:
            return SubstitutionAvailability.from_query_result(cached)

        stores = list(self.substituter_stores())
        if not stores:
            return SubstitutionAvailability.unavailable()

        to_probe = [store for store in stores if not self._has_cached_result(path, store.store_id)]
        if not to_probe:
            return SubstitutionAvailability.unavailable()

        done = anyio.Event()
        lock = anyio.Lock()
        first_positive: SubstitutionQueryResult | None = None
        healthy_pending = {store.store_id for store in to_probe if self.should_wait_for(store.store_id)}

        async def probe_store(store: Store) -> None:
            nonlocal first_positive
            result = await self._query_store(path, store)
            self.record_query_result(path, result)
            async with lock:
                if result.found and first_positive is None:
                    first_positive = result
                    done.set()
                healthy_pending.discard(result.store_id)
                if not healthy_pending:
                    done.set()

        for store in to_probe:
            self._track_probe_task(asyncio.create_task(probe_store(store)))

        if not healthy_pending:
            return SubstitutionAvailability.unavailable()

        await done.wait()
        if first_positive is not None:
            return SubstitutionAvailability.from_query_result(first_positive)
        return SubstitutionAvailability.unavailable()

    async def get_substituter(self, path: StorePath) -> SubstitutionCandidate | None:
        stores = list(self.substituter_stores())
        if not stores:
            return None

        stale_stores = [store for store in stores if not self._has_cached_result(path, store.store_id)]
        if stale_stores:
            await self._query_stores_for_selection(path, stale_stores)

        return self._best_candidate_from_cache(path, stores)

    async def substitute(self, path: StorePath) -> SubstitutionImportResult:
        existing = self._active_imports.get(path)
        if existing is not None:
            return await existing

        task = asyncio.create_task(self._substitute_uncached(path))
        self._active_imports[path] = task
        try:
            return await task
        finally:
            self._active_imports.pop(path, None)

    def record_query_result(self, path: StorePath, result: SubstitutionQueryResult) -> None:
        cache = self.positive if result.found else self.negative
        if not result.found:
            self._negative_hash_paths[path.hash_part()] = path
        per_store = cache.setdefault(path, {})
        per_store[result.store_id] = result
        self.health_for(result.store_id).record(query_succeeded=result.query_succeeded)

    def health_for(self, store_id: StoreId) -> SubstitutionHealthLog:
        log = self.health.get(store_id)
        if log is None:
            log = SubstitutionHealthLog(self.ctx.settings)
            self.health[store_id] = log
        return log

    def should_wait_for(self, store_id: StoreId) -> bool:
        healthy = self.health_for(store_id).is_healthy
        metrics.SUBSTITUTER_WAITED_FOR.labels(store_id=store_id).set(1 if healthy else 0)
        return healthy

    def substituter_stores(self) -> Iterable[Store]:
        return (
            store
            for store_id, store in sorted(self.ctx.stores.items(), key=lambda item: str(item[0]))
            if store_id != LOCAL_STORE_ID and store.no_schedule
        )

    def http_substituter_stores(self) -> list[Store]:
        """HTTP substituters, the only ones a narinfo race can ask.

        A daemon substituter answers the wire protocol, not `.narinfo`
        over HTTP, so the race leaves it out. Issue #85.
        """
        return [store for store in self.substituter_stores() if is_http_binary_cache(store)]

    async def race_http_narinfo(self, hash_part: str) -> UpstreamNarinfo | None:
        """Answer one `.narinfo` hash from the fastest HTTP substituter.

        Every HTTP substituter is asked at once. The first store with the
        path wins the answer; the rest keep running into the caches, so a
        later NAR redirect still ranks the whole field by priority. A hash
        no store has lands in a brief negative cache. Issue #85.
        """
        if hash_part in self._http_hash_negative:
            return None

        stores = self.http_substituter_stores()
        if not stores:
            return None
        # The scheduler asks every substituter before it builds, and the local
        # nix-daemon then asks this cache for the same output. Raced again,
        # the miss cost a trivial local build 2 s, the query timeout, while a
        # remote build asks nothing (nixidae/nixidae#1125).
        if self._every_store_missed(hash_part, stores):
            return None

        cached = await self._cached_http_narinfo(hash_part, stores)
        if cached is not None:
            return cached

        race = self._http_races.get(hash_part)
        if race is None:
            race = _UpstreamNarinfoRace(remaining=len(stores))
            self._http_races[hash_part] = race
            for store in stores:
                self._track_probe_task(asyncio.create_task(self._probe_http_narinfo(hash_part, store, race)))

        await race.event.wait()
        self._http_races.pop(hash_part, None)
        if race.winner is None:
            self._http_hash_negative[hash_part] = None
        return race.winner

    async def upstream_nar_source(self, hash_part: str) -> tuple[HTTPBinaryCacheStore, HTTPNarInfo] | None:
        """Highest-priority upstream holding one NAR hash, with its narinfo.

        The hash is a store-path hash from a `.narinfo` this cache served.
        Stale stores are queried first so the choice ranks answers and not
        whoever answered fastest. Any upstream's file decompresses to the
        one NAR the served narinfo hashes, so the choice may differ from the
        store that answered the narinfo. Issue #85, nixidae/nixidae#1137.
        """
        path = self._http_hash_paths.get(hash_part)
        if path is None:
            winner = await self.race_http_narinfo(hash_part)
            if winner is None:
                return None
            path = winner.info.path

        stores = self.http_substituter_stores()
        stale = [
            store
            for store in stores
            if not self._has_cached_result(path, store.store_id) and self.should_wait_for(store.store_id)
        ]
        if stale:
            await self._query_stores_for_selection(path, stale)

        positive = self.positive.get(path, {})
        ranked = sorted(
            (store for store in stores if positive.get(store.store_id) is not None),
            key=lambda store: store.priority,
        )
        for store in ranked:
            if not is_http_binary_cache(store):
                continue
            narinfo = await store.get_narinfo(path)
            if narinfo is not None:
                return store, narinfo
        return None

    async def _cached_http_narinfo(self, hash_part: str, stores: list[Store]) -> UpstreamNarinfo | None:
        path = self._http_hash_paths.get(hash_part)
        if path is None:
            return None
        positive = self.positive.get(path, {})
        ranked = sorted(
            (store for store in stores if positive.get(store.store_id) is not None),
            key=lambda store: store.priority,
        )
        for store in ranked:
            if not is_http_binary_cache(store):
                continue
            try:
                fetched = await store.fetch_narinfo(hash_part)
            except Exception:
                log.debug("http_cache_narinfo_refetch_failed", store_id=store.store_id, exc_info=True)
                continue
            if fetched is None:
                continue
            raw, info = fetched
            return UpstreamNarinfo(raw=raw, store=store, info=info)
        return None

    async def _probe_http_narinfo(self, hash_part: str, store: Store, race: _UpstreamNarinfoRace) -> None:
        if not is_http_binary_cache(store):
            return
        started = time.monotonic()
        try:
            with anyio.fail_after(self.ctx.settings.substitution_query_timeout):
                fetched = await store.fetch_narinfo(hash_part)
        except TimeoutError:
            metrics.SUBSTITUTER_QUERIES.labels(store_id=store.store_id, result="timeout").inc()
            log.warning("substitution_query_timeout", store_id=store.store_id, path=hash_part)
            self.health_for(store.store_id).record(query_succeeded=False)
        except Exception:
            metrics.SUBSTITUTER_QUERIES.labels(store_id=store.store_id, result="error").inc()
            log.warning("substitution_query_failed", store_id=store.store_id, path=hash_part, exc_info=True)
            self.health_for(store.store_id).record(query_succeeded=False)
        else:
            metrics.SUBSTITUTER_QUERY_DURATION.labels(store_id=store.store_id).observe(time.monotonic() - started)
            if fetched is None:
                metrics.SUBSTITUTER_QUERIES.labels(store_id=store.store_id, result="miss").inc()
            else:
                raw, info = fetched
                metrics.SUBSTITUTER_QUERIES.labels(store_id=store.store_id, result="hit").inc()
                self._http_hash_paths[hash_part] = info.path
                path_info = ValidPathInfo(path=StorePath(path=str(info.path)), info=info.valid_path_info.info)
                self.record_query_result(
                    info.path,
                    SubstitutionQueryResult(store_id=store.store_id, path_info=path_info, query_succeeded=True),
                )
                async with race.lock:
                    if race.winner is None:
                        race.winner = UpstreamNarinfo(raw=raw, store=store, info=info)
                        race.event.set()
        finally:
            async with race.lock:
                race.remaining -= 1
                if race.remaining <= 0:
                    race.event.set()

    async def _query_store(self, path: StorePath, store: Store) -> SubstitutionQueryResult:
        # `store_id` is a label, and it comes from the configuration, so the
        # series count is the number of substituters. The path does not appear.
        def counted(result: str) -> None:
            metrics.SUBSTITUTER_QUERIES.labels(store_id=store.store_id, result=result).inc()

        started = time.monotonic()
        try:
            with anyio.fail_after(self.ctx.settings.substitution_query_timeout):
                response = await store.execute(QueryPathInfoRequest(path=StorePath(path=str(path))))
        except OpNotImplementedError:
            counted("unsupported")
            return SubstitutionQueryResult(store_id=store.store_id, path_info=None, query_succeeded=False)
        except TimeoutError:
            # Timed out and refused are both "no" to the caller and a
            # different fault to an operator. A store that times out on every
            # query costs each selection the whole timeout.
            counted("timeout")
            log.warning("substitution_query_timeout", store_id=store.store_id, path=str(path))
            return SubstitutionQueryResult(store_id=store.store_id, path_info=None, query_succeeded=False)
        except Exception:
            counted("error")
            log.warning("substitution_query_failed", store_id=store.store_id, path=str(path), exc_info=True)
            return SubstitutionQueryResult(store_id=store.store_id, path_info=None, query_succeeded=False)
        finally:
            metrics.SUBSTITUTER_QUERY_DURATION.labels(store_id=store.store_id).observe(time.monotonic() - started)

        if not response.valid or response.info is None:
            counted("miss")
            return SubstitutionQueryResult(store_id=store.store_id, path_info=None, query_succeeded=True)

        counted("hit")

        path_info = ValidPathInfo(path=StorePath(path=str(path)), info=response.info)
        download_size = 0
        if is_http_binary_cache(store):
            narinfo = await store.get_narinfo(StorePath(path=str(path)))
            download_size = narinfo.file_size if narinfo is not None else 0
        return SubstitutionQueryResult(
            store_id=store.store_id, path_info=path_info, query_succeeded=True, download_size=download_size
        )

    async def _query_stores_for_selection(self, path: StorePath, stores: list[Store]) -> None:
        healthy_stores = [store for store in stores if self.should_wait_for(store.store_id)]
        background_stores = [store for store in stores if store not in healthy_stores]

        async def query_and_record(store: Store) -> None:
            self.record_query_result(path, await self._query_store(path, store))

        async with anyio.create_task_group() as tg:
            for store in healthy_stores:
                tg.start_soon(query_and_record, store)

        for store in background_stores:
            self._track_probe_task(asyncio.create_task(query_and_record(store)))

    @property
    def import_limit(self) -> anyio.CapacityLimiter:
        """The NAR imports that may stream at once, `max-substitution-jobs` of them."""
        if self._import_limit is None:
            self._import_limit = anyio.CapacityLimiter(self._import_slots)
        return self._import_limit

    def set_import_slots(self, jobs: int) -> None:
        """`max-substitution-jobs`; Nix runs one substitution even at 0 (`substitution-goal.cc:200`)."""
        self._import_slots = max(1, jobs)
        if self._import_limit is not None:
            self._import_limit.total_tokens = self._import_slots

    async def _substitute_uncached(self, path: StorePath) -> SubstitutionImportResult:
        started = time.monotonic()
        candidate = await self.get_substituter(path)
        if candidate is None:
            metrics.SUBSTITUTIONS.labels(result="no_candidate").inc()
            metrics.SUBSTITUTION_DURATION.observe(time.monotonic() - started)
            return SubstitutionImportResult(substituted=False, path=path, error=f"no substituter has path: {path}")

        try:
            # Every import at once took a 512 MB cache guest's harmonia out
            # with an OOM kill: ten megabytes per stream, and no limit.
            async with self.import_limit:
                await self._import_nar(path, candidate)
        except Exception as exc:
            metrics.SUBSTITUTIONS.labels(result="error").inc()
            log.warning("substitution_import_failed", store_id=candidate.store.store_id, path=str(path), exc_info=True)
            gone = any(isinstance(leaf, NarGone) for leaf in leaves(exc))
            return SubstitutionImportResult(
                substituted=False, path=path, candidate=candidate, error=str(exc), gone=gone
            )
        else:
            metrics.SUBSTITUTIONS.labels(result="ok").inc()
            metrics.SUBSTITUTED_BYTES.inc(candidate.path_info.info.nar_size)
            metrics.record_store_transfer(candidate.store.store_id, "in", candidate.path_info.info.nar_size)
            return SubstitutionImportResult(substituted=True, path=path, candidate=candidate)
        finally:
            # The whole attempt, the query for a candidate included. That
            # query is where a slow substituter costs a build its time, and
            # timing the import alone would hide it.
            metrics.SUBSTITUTION_DURATION.observe(time.monotonic() - started)

    async def _import_nar(self, path: StorePath, candidate: SubstitutionCandidate) -> None:
        async with self.ctx.local_store.transfer_conn() as conn:
            request = AddToStoreNarRequest(
                info=candidate.path_info,
                repair=0,
                dont_check_sigs=int(_trusted_source(candidate.store)),
            )
            await request.to_writer(WriteContext.from_conn(conn))
            await conn.w.drain()

            framed = conn.w.framed()
            if is_http_binary_cache(candidate.store):
                http_narinfo = candidate.http_narinfo
                if http_narinfo is None:
                    http_narinfo = await candidate.store.get_narinfo(path)
                if http_narinfo is None:
                    raise RuntimeError(f"substituter lost path while streaming: {path}")
                async for chunk in candidate.store.stream_nar(http_narinfo):
                    framed.write(chunk)
                    await conn.w.drain()
            elif isinstance(candidate.store, DaemonStore):
                await self._stream_from_daemon(path, candidate, framed, conn)
            else:
                raise TypeError(f"store {candidate.store.store_id} cannot stream NARs")

            await framed.finalize()
            await request.response_type.from_reader(ReadContext.from_conn(conn))

    async def _stream_from_daemon(
        self, path: StorePath, candidate: SubstitutionCandidate, framed: Any, destination_conn: Any
    ) -> None:
        if not isinstance(candidate.store, DaemonStore):
            raise TypeError(f"store {candidate.store.store_id} cannot stream NARs")
        async with candidate.store.transfer_conn() as source_conn:
            await NarFromPathRequest(path=StorePath(path=str(path))).to_writer(WriteContext.from_conn(source_conn))
            await source_conn.w.drain()
            await source_conn.r.drain_stderr()

            remaining = candidate.path_info.info.nar_size
            while remaining > 0:
                chunk = await source_conn.r.readexactly(min(remaining, _NAR_CHUNK_SIZE))
                framed.write(chunk)
                await destination_conn.w.drain()
                remaining -= len(chunk)

    def _best_candidate_from_cache(self, path: StorePath, stores: list[Store]) -> SubstitutionCandidate | None:
        positive = self.positive.get(path, {})
        candidates = [
            SubstitutionCandidate(store=store, path_info=result.path_info)
            for store in stores
            if (result := positive.get(store.store_id)) is not None and result.path_info is not None
        ]
        if not candidates:
            return None
        return min(candidates, key=lambda candidate: candidate.store.priority)

    def _cached_positive(self, path: StorePath) -> SubstitutionQueryResult | None:
        for result in self.positive.get(path, {}).values():
            if result.found:
                return result
        return None

    def _every_store_missed(self, hash_part: str, stores: list[Store]) -> bool:
        path = self._negative_hash_paths.get(hash_part)
        if path is None or path in self.positive:
            return False
        missed = self.negative.get(path, {})
        return all(store.store_id in missed for store in stores)

    def _has_cached_result(self, path: StorePath, store_id: StoreId) -> bool:
        return store_id in self.positive.get(path, {}) or store_id in self.negative.get(path, {})

    def _track_probe_task(self, task: asyncio.Task[None]) -> None:
        self._probe_tasks.add(task)

        def done_callback(done_task: asyncio.Task[None]) -> None:
            self._probe_tasks.discard(done_task)
            if done_task.cancelled():
                return
            try:
                done_task.result()
            except Exception:
                log.warning("substitution_probe_task_failed", exc_info=True)

        task.add_done_callback(done_callback)
