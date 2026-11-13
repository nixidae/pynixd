"""The one place that chooses who writes the store: pynixd's engine or the Nix daemon.

A write states what is known before a byte of its body arrives (`Write`):
a NAR or a dump has no replay, so the choice comes first. The op's route
(`op_routes.py`) decides whether the engine may take it at all. `REASONS`
then gives, in order, what still sends it to the daemon. The test stores, a
deployment and an experiment differ only in the routes they load and the
table the router holds. `docs/design/store-engine.md`.
"""

from __future__ import annotations

import contextlib
import weakref
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from functools import partial
from typing import TYPE_CHECKING

import anyio
import structlog
from anyio.lowlevel import checkpoint

from nix_daemon_protocol.add_build_log import AddBuildLogResponse
from nix_daemon_protocol.add_indirect_root import AddIndirectRootResponse
from nix_daemon_protocol.add_perm_root import AddPermRootResponse
from nix_daemon_protocol.add_signatures import AddSignaturesResponse
from nix_daemon_protocol.collect_garbage import CollectGarbageResponse
from nix_daemon_protocol.logs import LogNext
from nix_daemon_protocol.operations import STANDARD_OPERATIONS
from nix_daemon_protocol.optimise_store import OptimiseStoreResponse
from nix_daemon_protocol.protocol import GCAction, Verbosity
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.verify_store import VerifyStoreResponse
from pynixd_store.add import AddError
from pynixd_store.autogc import AutoGCSettings
from pynixd_store.collect import GCOptions
from pynixd_store.content_address import Method, parse_method_algo, text_needs_sha256
from pynixd_store.errors import PynixdError
from pynixd_store.source import Metered

from . import metrics
from .op_routes import OpRoute
from .store.copied import as_copied
from .store.daemon_writes import DaemonWrites

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterable, Mapping, Sequence

    from nix_daemon_protocol.collect_garbage import CollectGarbageRequest
    from nix_daemon_protocol.signature import Signature
    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from pynixd_store.add import ImportSettings
    from pynixd_store.autogc import AutoGC
    from pynixd_store.engine import StoreEngine
    from pynixd_store.source import Source

    from .connection import ClientConn, Connection
    from .serde import SetOptionsRequest
    from .store.scheduled import ScheduledStore

log = structlog.get_logger(__name__)

_OP_CODES = {op.name: op.code for op in STANDARD_OPERATIONS}


class Signatures(Enum):
    """What the store's settings and a client's keys make of a path's signatures."""

    TRUSTED = auto()
    UNTRUSTED = auto()
    UNCHECKED = auto()
    """No engine reads the store's settings, so the import checks."""


@dataclass(frozen=True)
class Dump:
    """The header of an AddToStore dump."""

    name: str
    method_algo: str
    references: frozenset[str]
    provenance: str = ""


@dataclass(frozen=True)
class Write:
    """One write as the router sees it. *op* names the route it follows.

    An op of several paths asks once with no *info*, and once per path with it.
    """

    op: str
    repair: bool = False
    info: ValidPathInfo | None = None
    dump: Dump | None = None


@dataclass(frozen=True)
class Facts:
    """What the reasons read of the store."""

    writable: bool
    settings: ImportSettings
    read_only: bool = False
    """A read-only store: the engine takes each write, and SQLite refuses the first change, as in Nix."""


def _database(write: Write, facts: Facts) -> str | None:
    return None if facts.writable or facts.read_only else "the store database is not writable"


def parse_dump_header(dump: Dump, features: frozenset[str]) -> tuple[Method, str]:
    """The method and algorithm of *dump*, or `ValueError` with Nix's refusal of its header.

    Nix refuses these before it reads the body (`daemon.cc:420`), so pynixd needs no daemon for them. A name
    or an address Nix cannot make a path of fails later, in `content_addressed_path`, after the hash.
    """
    method, algo = parse_method_algo(dump.method_algo, features)
    if method is Method.TEXT and algo != "sha256":
        raise ValueError(text_needs_sha256(dump.name, algo))
    return method, algo


def nix_refusal(write: Write, features: frozenset[str]) -> str | None:
    """What Nix refuses of *write* before the body: a dump's header, or the method and algorithm of a NAR's address.

    `ContentAddress::parseOpt` reads a NAR's address off the wire (`daemon.cc:924`, `worker-protocol.cc:380` for
    AddMultipleToStore), so a feature it lacks fails there, as a dump's header does.
    """
    try:
        if write.dump is not None:
            parse_dump_header(write.dump, features)
        elif write.info is not None and write.info.info.ca is not None and str(write.info.info.ca):
            parse_method_algo(str(write.info.info.ca), features)
    except ValueError as error:
        return str(error)
    return None


def pynixd_import(info: ValidPathInfo, *, repair: bool = False) -> Write:
    """A NAR pynixd itself fetched or received: a substitution, or the HTTP cache's upload.

    It follows AddToStoreNar's route, so a deployment that keeps that op on
    the daemon keeps these imports there too.
    """
    return Write(op="AddToStoreNar", repair=repair, info=info)


REASONS: tuple[Callable[[Write, Facts], str | None], ...] = (_database,)
"""What sends a write the engine may take to the daemon, first match wins."""


def first_reason(
    write: Write, facts: Facts, reasons: Sequence[Callable[[Write, Facts], str | None]] = REASONS
) -> str | None:
    for reason in reasons:
        why = reason(write, facts)
        if why is not None:
            return why
    return None


async def _client_log(conn: ClientConn | None, level: Verbosity, text: str) -> None:
    """A line Nix prints in the daemon, sent to the client.

    `TunnelLogger::log` (`daemon.cc:89`) drops a line above the client's
    verbosity. Before SetOptions the daemon has its own, `lvlInfo`
    (`logging.cc:161`).
    """
    if conn is None:
        return
    verbosity = conn.options.verbosity if conn.options is not None else Verbosity.INFO
    if level <= verbosity:
        await conn.send(LogNext(text=f"{text}\n"))


def _override(client: WriteClient, name: str) -> bool | None:
    """The session's *name*: `ClientSettings::apply` (`daemon.cc:233`) sets it for the worker's adds too."""
    value = client.options.overrides.get(name) if client.options is not None else None
    return {"true": True, "false": False}.get(value) if value is not None else None


async def _no_root(path: str) -> None:
    return None


@dataclass(frozen=True)
class WriteClient:
    """What a write needs of the client session it serves."""

    options: SetOptionsRequest | None = None
    #: The session's connection: signature warnings and the daemon's log lines of op 44 go there.
    conn: ClientConn | None = None
    #: Holds a path for the session (`local-store.cc:1052`). nixidae/nixidae#1007.
    root: Callable[[str], Awaitable[None]] = _no_root
    metered: bool = False


def _add_meter(client: WriteClient) -> metrics.TransferMeter:
    return metrics.TransferMeter(
        enabled=client.metered,
        byte_counter=metrics.NAR_ADD_BYTES,
        path_counter=metrics.NAR_ADD_PATHS,
        duration=metrics.NAR_ADD_DURATION,
    )


async def _forward_metered(
    paths: AsyncIterator[tuple[ValidPathInfo, Source]], metered: bool
) -> AsyncGenerator[tuple[ValidPathInfo, Source]]:
    meter = metrics.TransferMeter(
        enabled=metered,
        byte_counter=metrics.NAR_FORWARD_BYTES,
        path_counter=metrics.NAR_FORWARD_PATHS,
        duration=metrics.NAR_FORWARD_DURATION,
    )
    async for info, nar in paths:
        yield info, Metered(nar, meter.on_bytes)
        meter.finish()


@dataclass
class WriteRouter:
    """Chooses the engine or the daemon for each write of one local store, and runs the write there.

    Each write method has Nix's name. A store with no *engine* sends every write to the daemon.
    """

    routes: Mapping[int, OpRoute]
    engine: StoreEngine | None
    daemon: DaemonWrites
    store_id: str
    #: The store's path-info cache, told of each path the engine adds.
    remember: Callable[[ValidPathInfo], None]
    reasons: Sequence[Callable[[Write, Facts], str | None]] = field(default=REASONS)
    #: Makes the `LocalStore::autoGC` state of a session; it runs while the engine collects (op 20).
    new_auto_gc: Callable[[], AutoGC] | None = None
    _auto_gcs: weakref.WeakKeyDictionary[ClientConn, AutoGC] = field(
        default_factory=weakref.WeakKeyDictionary, init=False, repr=False
    )

    @classmethod
    def daemon_only(cls, store: ScheduledStore) -> WriteRouter:
        """The router of a store pynixd does not write itself: every write goes to its daemon."""
        return cls({}, None, DaemonWrites(store), str(store.store_id), store.add_path_info)

    def route(self, op: str) -> OpRoute:
        return self.routes.get(_OP_CODES[op], OpRoute.daemon)

    async def why_the_daemon(self, write: Write) -> str | None:
        """Why the daemon takes *write*, or `None` when the engine does."""
        if self.engine is None or self.route(write.op) is not OpRoute.pynixd:
            return f"{write.op} takes the daemon route"
        facts = Facts(
            writable=self.engine.writable, settings=await self.engine.settings(), read_only=self.engine.read_only
        )
        return first_reason(write, facts, self.reasons)

    async def engine_takes(self, write: Write) -> bool:
        """Whether the engine takes *write*. A write on the `pynixd` route that falls through is logged and counted."""
        if self.engine is None or self.route(write.op) is not OpRoute.pynixd:
            return False
        # Nix's refusal either way. The daemon refuses it before it reads the body and closes, and under load the
        # forward then reads a reset connection, not the error (nixidae/nixidae#1270).
        refusal = nix_refusal(write, (await self.engine.settings()).experimental_features)
        if refusal is not None:
            raise PynixdError(refusal)
        reason = await self.why_the_daemon(write)
        if reason is None:
            return True
        if not self.daemon.runs:
            raise PynixdError(reason)
        subject = str(write.info.path) if write.info is not None else write.dump.name if write.dump else None
        log.info("write_fallthrough", op=write.op, subject=subject, reason=reason)
        metrics.OP_FALLTHROUGH.labels(store_id=self.store_id, op=write.op).inc()
        return False

    def _engine(self) -> StoreEngine:
        if self.engine is None:
            raise RuntimeError(f"store {self.store_id} has no engine")
        return self.engine

    async def signatures(self, info: ValidPathInfo, overrides: Mapping[str, str]) -> Signatures:
        """`LocalStore::pathInfoIsUntrusted` (`local-store.cc:1036`) under a client's key *overrides*."""
        if self.engine is None:
            return Signatures.UNCHECKED
        settings = await self.engine.settings()
        if not settings.require_sigs:
            return Signatures.TRUSTED
        try:
            self.engine.check_signatures(info, settings, overrides, [])
        except AddError:
            return Signatures.UNTRUSTED
        return Signatures.TRUSTED

    async def _check_signatures(self, info: ValidPathInfo, settings: ImportSettings, client: WriteClient) -> None:
        """`pathInfoIsUntrusted` and its refusal (`local-store.cc:1048`), with the client's key overrides."""
        warnings: list[str] = []
        try:
            overrides = client.options.overrides if client.options is not None else {}
            self._engine().check_signatures(info, settings, overrides, warnings)
        finally:
            # Nix prints each warning as it checks, before any refusal.
            if client.conn is not None:
                for warning in warnings:
                    await client.conn.send(LogNext(text=f"{warning}\n"))

    async def _engine_add(
        self,
        info: ValidPathInfo,
        source: Source,
        settings: ImportSettings,
        *,
        repair: bool,
        dont_check_sigs: bool,
        client: WriteClient,
        root: bool,
    ) -> None:
        """`LocalStore::addToStore` (`local-store.cc:1046`): the signatures, then the root, then the NAR."""
        engine = self._engine()
        try:
            if not dont_check_sigs and settings.require_sigs:
                await self._check_signatures(info, settings, client)
            if root:
                await client.root(str(info.path))
            await self.auto_gc_before_import(client.conn)
            await engine.add_to_store(
                info,
                source,
                repair=repair,
                ignored_acls=settings.ignored_acls,
                auto_optimise=_override(client, "auto-optimise-store"),
                fsync=_override(client, "fsync-store-paths"),
                report=partial(_client_log, client.conn),
            )
        finally:
            engine.invalidate_path_info_cache_for(info.path)

    async def add_to_store(self, write: Write, source: Source, *, dont_check_sigs: bool, client: WriteClient) -> None:
        """AddToStoreNar (op 39): `Store::addToStore` with a NAR. The client holds the path already."""
        info = write.info
        if info is None:
            raise ValueError("AddToStoreNar carries its info")
        meter = _add_meter(client)
        if await self.engine_takes(write):
            settings = await self._engine().settings()
            await self._engine_add(
                info,
                Metered(source, meter.on_bytes),
                settings,
                repair=write.repair,
                dont_check_sigs=dont_check_sigs,
                client=client,
                root=False,
            )
        else:
            # The daemon's logger is the tunnel during `addToStore` (`daemon.cc:943`). nixidae/nixidae#1161.
            await self.daemon.add_to_store(
                info,
                source,
                repair=write.repair,
                dont_check_sigs=dont_check_sigs,
                options=client.options,
                logs=client.conn,
                on_bytes=meter.on_bytes,
            )
        meter.finish()

    async def add_to_store_from_dump(
        self,
        write: Write,
        source: Source,
        *,
        client: WriteClient,
        sign: Callable[[ValidPathInfo, Connection | None], Awaitable[ValidPathInfo]],
    ) -> ValidPathInfo:
        """AddToStore (op 7): `Store::addToStoreFromDump`, then *sign* over the daemon connection when there is one."""
        dump = write.dump
        if dump is None:
            raise ValueError("AddToStore carries its dump")
        meter = _add_meter(client)
        if not await self.engine_takes(write):
            info = await self.daemon.add_to_store_from_dump(
                dump.name,
                dump.method_algo,
                dump.references,
                source,
                repair=write.repair,
                provenance=dump.provenance,
                options=client.options,
                logs=client.conn,
                root=client.root,
                sign=sign,
                on_bytes=meter.on_bytes,
            )
            meter.finish()
            return info
        engine = self._engine()
        settings = await engine.settings()
        method, algo = parse_dump_header(dump, settings.experimental_features)
        await self.auto_gc_before_import(client.conn)
        path = await engine.add_to_store_from_dump(
            dump.name,
            method,
            algo,
            dump.references,
            Metered(source, meter.on_bytes),
            root=client.root,
            repair=write.repair,
            provenance=dump.provenance,
            ignored_acls=settings.ignored_acls,
            auto_optimise=_override(client, "auto-optimise-store"),
            fsync=_override(client, "fsync-store-paths"),
            report=partial(_client_log, client.conn),
        )
        meter.finish()
        # `invalidatePathInfoCacheFor`: a path valid before this op may hold a negative entry.
        engine.invalidate_path_info_cache_for(path)
        added = await engine.query_path_info(path)
        if added is None:
            raise RuntimeError(f"pynixd added '{path}', and the store database does not hold it")
        added = await sign(added, None)
        self.remember(added)
        return added

    async def add_multiple_to_store(
        self,
        write: Write,
        count: int,
        paths: AsyncIterator[tuple[ValidPathInfo, Source]],
        *,
        dont_check_sigs: bool,
        client: WriteClient,
    ) -> None:
        """AddMultipleToStore (op 44). Each NAR of *paths* is read to its end before the next path.

        On the engine route, the loop of `daemon.cc:505`: `addToStore` per
        path, in stream order, and a path the engine does not take goes to the
        daemon alone. The first error ends the op, and the paths before it
        stay valid.
        """
        if not await self.engine_takes(write):
            async with contextlib.aclosing(_forward_metered(paths, client.metered)) as metered:
                infos = await self.daemon.add_multiple_to_store(
                    count, metered, repair=write.repair, dont_check_sigs=dont_check_sigs, options=client.options
                )
            # The session holds each path, as Nix's worker does for its client. nixidae/nixidae#1007.
            for info in infos:
                await client.root(str(info.path))
            return
        engine = self._engine()
        settings = await engine.settings()
        meter = _add_meter(client)
        async for info, nar in paths:
            await checkpoint()
            # `daemon.cc:515`: for every client, not only an untrusted one.
            copied = as_copied(info)
            if await self.engine_takes(replace(write, info=copied)):
                # `daemon.cc:518`: each path is added with the op's repair flag.
                await self._engine_add(
                    copied,
                    nar,
                    settings,
                    repair=write.repair,
                    dont_check_sigs=dont_check_sigs,
                    client=client,
                    root=True,
                )
            else:
                await client.root(str(copied.path))
                try:
                    await self.daemon.add_to_store(
                        copied,
                        nar,
                        repair=write.repair,
                        dont_check_sigs=dont_check_sigs,
                        options=client.options,
                        logs=client.conn,
                    )
                finally:
                    engine.invalidate_path_info_cache_for(copied.path)
            if meter.on_bytes is not None:
                meter.on_bytes(copied.info.nar_size)
            meter.finish()

    async def _auto_gc(self, conn: ClientConn | None) -> tuple[AutoGC, AutoGCSettings, Mapping[str, str]] | None:
        """The session's auto-GC and its `min-free` settings, when the engine collects; `None` when the daemon does."""
        engine = self._root_engine("CollectGarbage")
        if engine is None or self.new_auto_gc is None or conn is None:
            return None
        overrides = conn.options.overrides if conn.options is not None else {}
        settings = AutoGCSettings.from_nix_config(await engine.nix_config(), overrides)
        if not settings.enabled:
            return None
        auto_gc = self._auto_gcs.get(conn)
        if auto_gc is None:
            auto_gc = self._auto_gcs[conn] = self.new_auto_gc()
        return auto_gc, settings, overrides

    async def auto_gc_before_import(self, conn: ClientConn | None) -> None:
        """`autoGC()` before an import writes its NAR (`local-store.cc:1151`, `:1533`): it waits for a collection."""
        found = await self._auto_gc(conn)
        if found is not None:
            auto_gc, settings, overrides = found
            await auto_gc.check(settings, overrides, wait=True)

    async def auto_gc_while_building(self, conn: ClientConn | None) -> None:
        """`autoGC(false)` while a request builds (`worker.cc:294`): it starts a collection and does not wait.

        NIX-DEVIATION (#27): `Worker::run` checks at each pass of its loop,
        which wakes on a goal's output and at least every 10 s while
        `min-free` is set (`worker.cc:395`). A request of pynixd has no such
        loop, so this checks every `min-free-check-interval`, the rate limit
        of each check in Nix too. Free space has no notification to wait on.
        Measure a check that comes later than Nix's to reverse it.

        A build on the daemon route leaves this to the daemon's worker, which
        has the client's `min-free` and checks as it builds: both collecting
        took every dead path where Nix leaves one (`gc-auto.sh:44`).
        """
        if self.route("BuildDerivation") is not OpRoute.pynixd:
            return
        found = await self._auto_gc(conn)
        if found is None:
            return
        auto_gc, settings, overrides = found
        while True:
            await auto_gc.check(settings, overrides, wait=False)
            await anyio.sleep(max(settings.check_interval, 1))

    def engine_collects(self) -> bool:
        """Whether CollectGarbage (op 20) takes the engine, which pynixd's own collector (op 101) follows."""
        return self._root_engine("CollectGarbage") is not None

    def _root_engine(self, op: str) -> StoreEngine | None:
        """The engine when *op* takes the `pynixd` route. These ops read no import setting, so no reason sends it back."""
        if self.engine is None or self.route(op) is not OpRoute.pynixd:
            return None
        return self.engine

    async def add_indirect_root(self, path: str) -> AddIndirectRootResponse:
        """AddIndirectRoot (op 12): `LocalStore::addIndirectRoot`."""
        engine = self._root_engine("AddIndirectRoot")
        if engine is None:
            return await self.daemon.add_indirect_root(path)
        await engine.add_indirect_root(path)
        return AddIndirectRootResponse.fast(value=1)

    async def add_perm_root(
        self, store_path: str, gc_root: str, *, hold: Callable[[str], Awaitable[None]]
    ) -> AddPermRootResponse:
        """AddPermRoot (op 47): `IndirectRootStore::addPermRoot`. *hold* is the session's temporary root."""
        engine = self._root_engine("AddPermRoot")
        if engine is None:
            return await self.daemon.add_perm_root(store_path, gc_root)
        return AddPermRootResponse.fast(gc_root=await engine.add_perm_root(store_path, gc_root, hold=hold))

    async def add_signatures(self, path: str, sigs: Iterable[Signature]) -> AddSignaturesResponse:
        """AddSignatures (op 37): `LocalStore::addSignatures`. The engine forgets the path's cached info itself."""
        engine = self._root_engine("AddSignatures")
        if engine is None:
            return await self.daemon.add_signatures(path, sigs)
        await engine.add_signatures(path, sigs)
        return AddSignaturesResponse.fast(value=1)

    async def add_build_log(self, path: str, log: bytes) -> AddBuildLogResponse:
        """AddBuildLog (op 45): `LocalStore::addBuildLog`. The log is read whole, as Nix drains it before writing."""
        engine = self._root_engine("AddBuildLog")
        if engine is None:
            return await self.daemon.add_build_log(path, log)
        await engine.add_build_log(path, log)
        return AddBuildLogResponse.fast(value=1)

    async def optimise_store(
        self, *, hold: Callable[[str], Awaitable[None]], conn: ClientConn | None = None
    ) -> OptimiseStoreResponse:
        """OptimiseStore (op 34): `LocalStore::optimiseStore`. *hold* is the session's temporary root.

        On the daemon route the daemon's own summary line travels in the answer.
        On the engine route pynixd sends it, as Nix's `printInfo` does
        (`optimise-store.cc:328`); the parity suite's `_builds` reads it.
        """
        engine = self._root_engine("OptimiseStore")
        if engine is None:
            return await self.daemon.optimise_store()
        await engine.optimise_store(hold=hold, report=partial(_client_log, conn))
        return OptimiseStoreResponse.fast(value=1)

    async def verify_store(
        self,
        *,
        check_contents: bool,
        repair: bool,
        conn: ClientConn | None = None,
        repair_path: Callable[[str], Awaitable[None]] | None = None,
    ) -> VerifyStoreResponse:
        """VerifyStore (op 35): `LocalStore::verifyStore`. Returns whether it found errors.

        A `repair` request needs *repair_path*, pynixd's `Store::repairPath`;
        without it the daemon repairs. The handler refuses an untrusted repair
        before this. On the engine route pynixd sends the lines Nix prints;
        the parity suite's `_queries` reads them.
        """
        engine = self._root_engine("VerifyStore")
        if engine is None or (repair and repair_path is None):
            if engine is not None:
                log.info("write_fallthrough", op="VerifyStore", subject=None, reason="repair")
                metrics.OP_FALLTHROUGH.labels(store_id=self.store_id, op="VerifyStore").inc()
            return await self.daemon.verify_store(check_contents=check_contents, repair=repair, client=conn)
        errors = await engine.verify_store(
            check_contents=check_contents, report=partial(_client_log, conn), repair=repair_path if repair else None
        )
        return VerifyStoreResponse.fast(value=int(errors))

    async def collect_garbage(
        self, request: CollectGarbageRequest, *, conn: ClientConn | None = None, censor: bool = False
    ) -> CollectGarbageResponse:
        """CollectGarbage (op 20): `LocalStore::collectGarbage`. The handler has refused what Nix refuses.

        On the engine route pynixd sends the lines Nix prints, and the
        session's overrides reach `keep-outputs` and `keep-derivations`.
        """
        engine = self._root_engine("CollectGarbage")
        if engine is None:
            return await self.daemon.collect_garbage(request, client=conn)
        gc_paths = request.with_both_shapes().gc_paths
        named = None if gc_paths is None or gc_paths.whole_store else gc_paths
        options = GCOptions(
            action=GCAction(request.action),
            paths=None if named is None else frozenset(path.path for path in named.paths),
            delete_referrers=named is not None and bool(named.delete_referrers),
            max_freed=request.max_freed,
            ignore_liveness=bool(request.ignore_liveness),
            censor=censor,
        )
        overrides = conn.options.overrides if conn is not None and conn.options is not None else {}
        results = await engine.collect_garbage(
            options, settings=await engine.gc_settings(overrides), report=partial(_client_log, conn)
        )
        return CollectGarbageResponse.fast(
            paths_deleted={StorePath(path=path) for path in results.paths}, bytes_freed=results.bytes_freed, obsolete=0
        )

    async def import_nar(
        self, info: ValidPathInfo, source: Source, *, dont_check_sigs: bool, repair: bool = False
    ) -> None:
        """A NAR pynixd itself fetched or received: a substitution, or the HTTP cache's upload."""
        if await self.engine_takes(pynixd_import(info, repair=repair)):
            await self._engine().import_nar(info, source, dont_check_sigs=dont_check_sigs, repair=repair)
        else:
            await self.daemon.add_to_store(info, source, dont_check_sigs=dont_check_sigs, repair=repair)
