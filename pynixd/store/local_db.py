"""LocalDBStore — LocalStore with SQLite database for fast-path queries."""

from __future__ import annotations

import contextlib
import dataclasses
import functools
import os
import types
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Union, get_args, get_origin

import anyio
import anyio.to_thread
import structlog
from pydantic import BaseModel

from nix_daemon_protocol.build_resources import BuildResources, ResourceSample
from nix_daemon_protocol.constants import FEATURE_REALISATION_WITH_PATH
from nix_daemon_protocol.derivation_output import OutputKind
from nix_daemon_protocol.drv_output import DrvOutput
from nix_daemon_protocol.operations import STANDARD_OPERATIONS
from nix_daemon_protocol.opt_microseconds import OptMicroseconds
from nix_daemon_protocol.protocol import Verbosity
from nix_daemon_protocol.realisation import Realisation
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation
from pynixd_store.builder import hooks, local as local_builder
from pynixd_store.builder.settings import BuildSettings
from pynixd_store.builder.vm import BuildVm
from pynixd_store.create_store import open_local_store
from pynixd_store.drv_parser import drv_bytes, from_basic_derivation
from pynixd_store.engine import StoreEngine, hashes_self_reference_positions
from pynixd_store.hintfmt import magenta, show_error
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.nix_config import nix_environment, read_nix_config, read_nix_config_text, read_nix_version
from pynixd_store.overlay_engine import LowerPathNeedsSync, OverlayEngine
from pynixd_store.signing import SecretKey, hash_keyed_realisation_fingerprint, realisation_fingerprint
from pynixd_store.source import Pieces

from .. import metrics
from ..activity import BuildActivity, PostBuildHookActivity
from ..op_routes import OpRoute, Owned
from ..serde import (
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
    StorePath,
)
from ..write_router import Dump, Write, WriteClient, WriteRouter
from .base import Store
from .daemon_writes import DaemonWrites
from .local_daemon import LocalStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Mapping, Set as AbstractSet
    from contextlib import AbstractAsyncContextManager

    from nix_daemon_protocol.logs import LogMessage
    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd_store.add import ImportSettings
    from pynixd_store.builder.restriction import RecursiveServer
    from pynixd_store.builder.usage import BuildUsage
    from pynixd_store.content_address import ContentAddress
    from pynixd_store.drv_parser import Derivation

    from ..config import LocalSocketStoreSpec
    from ..connection import ClientConn, Connection
    from ..serde import SetOptionsRequest
    from ..substitution_queue import SubstitutionQueue
    from ..trust import TrustPolicy

log = structlog.get_logger(__name__)

_OP_CODES = {op.name: op.code for op in STANDARD_OPERATIONS}
_OP_NAMES = {op.code: op.name for op in STANDARD_OPERATIONS}
_HASH_PART_LENGTH = 32  # `StorePath::HashLen`

#: The most of one value a shadow mismatch logs: `QueryAllValidPaths` answers the whole store.
_LOGGED_CHARACTERS = 2000


def _value(response: Any, unordered: frozenset[str] = frozenset()) -> Any:
    """What an answer says, without the log messages that came with it.

    The fields in *unordered* are sorted, because their order says nothing.
    """
    if isinstance(response, BaseModel):
        dumped = response.model_dump(exclude={"logs"})
        for name in unordered & dumped.keys():
            dumped[name] = sorted(dumped[name], key=repr)
        return dumped
    return response


def _bounded(value: Any) -> str:
    text = repr(value)
    if len(text) <= _LOGGED_CHARACTERS:
        return text
    return f"{text[:_LOGGED_CHARACTERS]}... ({len(text)} characters)"


_SCALAR_ANNOTATIONS = (str, bytes, int, float, bool)
"""Annotations whose values never pass the `isinstance` below.

A field keeps its scan unless its annotation is one of these, an enum, a
non-`StorePath` model, or a dict: the runtime only unwraps a `StorePath`
and a flat container of them, so anything else contributes nothing.
Everything uncertain -- `Any`, a missing annotation, a union, a container,
a custom class -- keeps its scan, and a new operation that carries a
`StorePath` is still counted on the day it is added.
"""


def _may_hold_paths(annotation: Any) -> bool:
    """Whether a value of `annotation` could pass the scan in `referenced_paths`."""
    if annotation is None or annotation is Any:
        return True
    if annotation is StorePath:
        return True
    if isinstance(annotation, type):
        if issubclass(annotation, StorePath):
            return True
        if issubclass(annotation, (list, set, frozenset, tuple)):
            return True
        # Certain non-carriers: scalars, enums, nested models, anything else
        # the runtime `isinstance` cannot match.
        return not issubclass(annotation, (*_SCALAR_ANNOTATIONS, Enum, BaseModel, dict))
    origin = get_origin(annotation)
    if origin in (list, set, frozenset, tuple):
        return True
    if origin is dict:
        # The runtime only unwraps a `StorePath` and a flat container of
        # them; a mapping is never scanned, so its paths stay uncounted
        # either way.
        return False
    if origin is Union or origin is types.UnionType:
        return any(_may_hold_paths(arg) for arg in get_args(annotation) if arg is not type(None))
    return True


@functools.lru_cache(maxsize=256)
def _path_field_names(cls: type) -> tuple[str, ...]:
    """The fields of `cls` that can name a store path, resolved once.

    A build sends an operation for every derivation of its closure, and the
    scan below runs for each one. The declaration does not change between
    operations, so the field selection is per class and the values per
    operation.
    """
    try:
        fields = cls.model_fields
    except AttributeError:
        return ()
    return tuple(name for name, field in fields.items() if _may_hold_paths(field.annotation))


def referenced_paths(request: object) -> set[str]:
    """Every store path that a request names in its own fields.

    The fields are the declaration, so this reads the model rather than a
    list of operations that somebody has to keep current. A new operation
    that carries a `StorePath` is counted on the day it is added.

    Only the fields of the request itself. A nested model, such as the
    `BasicDerivation` of a build, names the paths that the build will
    *produce*, and a path that does not exist yet was referenced by nothing.
    """
    if not isinstance(request, BaseModel):
        return set()
    names = _path_field_names(type(request))  # type: ignore[arg-type]  # zuban-bug: zuban checks type[BaseModel] against Hashable through BaseModel's __eq__-derived __hash__ = None; mypy accepts the class object
    if not names:
        return set()
    found: set[str] = set()
    for name in names:
        value = getattr(request, name, None)
        if isinstance(value, StorePath):
            found.add(str(value))
        elif isinstance(value, (set, frozenset, list, tuple)):
            found.update(str(item) for item in value if isinstance(item, StorePath))
    found.discard("")
    return found


def _resources(usage: BuildUsage) -> BuildResources:
    return BuildResources(
        measured=1,
        source=usage.source,
        cpu_user_us=usage.cpu_user_us,
        cpu_system_us=usage.cpu_system_us,
        peak_memory_bytes=usage.peak_memory_bytes,
        io_read_bytes=usage.io_read_bytes,
        io_write_bytes=usage.io_write_bytes,
        cpu_stall=_stall(usage.cpu_stall_us),
        memory_stall=_stall(usage.memory_stall_us),
        io_stall=_stall(usage.io_stall_us),
        samples=[ResourceSample(**dataclasses.asdict(sample)) for sample in usage.samples],
        host_source=usage.host_source or "",
        host_cpu_user_us=usage.host_cpu_user_us or 0,
        host_cpu_system_us=usage.host_cpu_system_us or 0,
        host_peak_memory_bytes=usage.host_peak_memory_bytes or 0,
    )


def _stall(us: int | None) -> OptMicroseconds:
    return OptMicroseconds() if us is None else OptMicroseconds(tag=1, value=us)


class LocalDBStore(Store):
    """pynixd's local store: the engine, the managed daemon, and the router between them.

    The daemon client (`LocalStore`) builds, and answers every operation the
    engine does not. The scheduler builds on it through `daemon_client`.

    Each `own_*` method answers from the engine when the database is open,
    and returns `None` when it is not. `execute` then asks the daemon, so a
    database pynixd cannot open costs correctness nothing.

    An overlay store (`StoreLayout.overlay`) gets an `OverlayEngine`, which
    reads the lower store's database too. A read that must copy a lower path
    up, with `pynixd_writes` off, raises `LowerPathNeedsSync`, and the daemon
    answers. A collection and an optimisation delete through the upper
    layer and run the remount hook. A build writes through the merged
    directory, as a `LocalStore` build does.
    """

    #: pynixd's answer to each operation it can answer, by op code.
    OWNED: ClassVar[dict[int, Owned]] = {
        1: Owned("own_is_valid_path", default=OpRoute.pynixd),
        6: Owned("own_query_referrers", default=OpRoute.pynixd),
        14: Owned("own_find_roots", default=OpRoute.pynixd, unordered=frozenset({"roots"})),
        23: Owned("own_query_all_valid_paths", default=OpRoute.pynixd),
        26: Owned("own_query_path_info", default=OpRoute.pynixd),
        29: Owned("own_query_path_from_hash_part", default=OpRoute.pynixd),
        31: Owned("own_query_valid_paths", default=OpRoute.pynixd),
        # The daemon in a deployment: the shadow reads every NAR twice. The test stores run it in shadow.
        38: Owned("own_nar_from_path"),
        # It writes through the route of AddToStoreNar; `writes` keeps it off the shadow and off by default.
        10: Owned("own_ensure_path", writes=True),
        7: Owned("own_add_to_store", writes=True),
        39: Owned("own_add_to_store_nar", writes=True),
        44: Owned("own_add_multiple_to_store", writes=True),
        12: Owned("own_add_indirect_root", writes=True),
        37: Owned("own_add_signatures", writes=True),
        47: Owned("own_add_perm_root", writes=True),
        32: Owned("own_query_substitutable_paths", default=OpRoute.pynixd),
        21: Owned("own_query_substitutable_path_info"),
        30: Owned("own_query_substitutable_path_infos"),
        33: Owned("own_query_valid_derivers", default=OpRoute.pynixd),
        41: Owned("own_query_derivation_output_map", default=OpRoute.pynixd),
        42: Owned("own_register_drv_output", writes=True),
        45: Owned("own_add_build_log", writes=True),
        34: Owned("own_optimise_store", writes=True),
        35: Owned("own_verify_store", writes=True),
        20: Owned("own_collect_garbage", writes=True),
        43: Owned("own_query_realisation", default=OpRoute.pynixd),
        # The scheduler calls `local_build_settings` and `build_here` itself, for its own log fan-out.
        36: Owned("own_build_derivation", writes=True),
    }

    #: Operations of pynixd's own protocol extension that the engine answers.
    #: They take no route: Nix has no such operation, and the daemon's
    #: fallback for each asks one path at a time.
    EXTENDED: ClassVar[dict[int, str]] = {
        103: "query_path_infos",
        104: "query_closure",
        105: "query_closure_with_info",
        106: "query_derivation_output_map_batch",
    }

    def __init__(self, spec: LocalSocketStoreSpec) -> None:
        super().__init__(spec)
        self._daemon = LocalStore(spec)
        # One cache: an engine write forgets the entry that a daemon answer reads.
        self.path_info_cache = self._daemon.path_info_cache
        self.layout = self._daemon.layout
        self.managed = self._daemon.managed
        self.settings = self._daemon.settings
        # The substituter list reads `nix_config` with `getattr`, because it takes any store.
        # The collector reads `gc_allow_execute` the same way, and a missing one denies.
        self.nix_config = spec.nix_config
        self.gc_max_age = spec.gc_max_age
        self.gc_target_usage = spec.gc_target_usage
        self.gc_allow_execute = spec.gc_allow_execute
        #: pynixd's own local store (`docs/design/store-engine.md`).
        self.engine = (OverlayEngine if self.layout.overlay is not None else StoreEngine)(
            self.layout,
            writes=spec.pynixd_writes,
            read_config=lambda: read_nix_config(spec.nix_bin, nix_environment(self.layout, spec.extra_env)),
            on_change=self.forget_path_info,
            name=str(self.store_id),
        )
        self._nix_version_command = (spec.nix_bin, spec.extra_env)
        self._nix_version: str | None = None
        #: The scheduler's race over the substituters; the instance sets it at start.
        self.substitution_queue: SubstitutionQueue | None = None
        #: What a recursive-nix build's builder talks to; the instance sets it at start, and without it the daemon
        #: builds those.
        self.recursive_server: RecursiveServer | None = None
        self.usage_sample_seconds = spec.usage_sample_seconds
        self.build_vm = (
            BuildVm.from_environment(
                memory_mib=spec.build_vm.memory_mib,
                cpus=spec.build_vm.cpus,
                dax_window_mib=spec.build_vm.dax_window_mib,
                disk_gib=spec.build_vm.disk_gib,
            )
            if spec.build_vm is not None
            else None
        )
        self.configured_routes = {_OP_CODES[name]: route for name, route in spec.op_routes.items()}
        for op, route in self.configured_routes.items():
            if route is OpRoute.daemon:
                continue
            owned = self.OWNED.get(op)
            if owned is None:
                raise ValueError(f"op_routes: pynixd has no answer to {_OP_NAMES[op]}; its route is daemon")
            if route is OpRoute.shadow and owned.writes:
                raise ValueError(f"op_routes: {_OP_NAMES[op]} writes, so it cannot run beside the daemon in shadow")
            if owned.writes and not spec.pynixd_writes:
                raise ValueError(f"op_routes: {_OP_NAMES[op]} writes the store, and pynixd_writes is off")
        #: Whether pynixd itself may change the store (`LocalSocketStoreSpec.pynixd_writes`).
        self.pynixd_writes = spec.pynixd_writes
        # An answer that writes defaults to the daemon even with writes on: only a route set by hand takes it.
        # With no daemon, every answer is pynixd's.
        self.routes = {
            op: self.configured_routes.get(
                op, OpRoute.pynixd if not spec.daemon else OpRoute.daemon if owned.writes else owned.default
            )
            for op, owned in self.OWNED.items()
        }
        #: Each layer's big lock (`open_local_store`), held shared while a store with no daemon is open.
        self._big_locks: list[int] = []
        #: Who writes: the engine or the daemon (`write_router.py`).
        self.router = WriteRouter(
            self.routes, self.engine, DaemonWrites(self._daemon), str(self.store_id), self.add_path_info
        )

    # ── The daemon client ───────────────────────────────────────────

    @property
    def daemon_client(self) -> LocalStore:
        return self._daemon

    @property
    def features(self) -> AbstractSet[str]:
        return self._daemon.features

    @property
    def feature_matrix(self) -> dict[str, set[str]]:
        return self._daemon.feature_matrix

    @property
    def is_healthy(self) -> bool:
        return self._daemon.is_healthy

    @property
    def in_flight(self) -> int:
        return self._daemon.in_flight

    @property
    def version(self) -> int:
        return self._daemon.version

    async def trust_policy(self) -> TrustPolicy:
        return await self._daemon.trust_policy()

    async def read_nix_config(self) -> dict[str, Any]:
        return await self._daemon.read_nix_config()

    async def probe(self) -> None:
        await self._daemon.probe()

    async def create_conn(self) -> Connection:
        return await self._daemon.create_conn()

    def transfer_conn(self, options: SetOptionsRequest | None = None) -> AbstractAsyncContextManager[Connection]:
        return self._daemon.transfer_conn(options)

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        return await self._daemon.call(
            request, client=client, suppress_last=suppress_last, raise_on_error=raise_on_error, skip_probe=skip_probe
        )

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        parsed = await self.engine.read_derivation(str(drv_store_path))
        if parsed is not None or not self._daemon.runs_daemon:
            return parsed
        return await self._daemon.read_derivation(drv_store_path)

    async def add_text_to_store(self, name: str, text: str, references: AbstractSet[str]) -> str:
        """`Store::addTextToStore`: an AddToStore of `text:sha256`, on that op's route."""

        async def body() -> AsyncIterator[bytes]:
            yield drv_bytes(text)

        async def unsigned(info: ValidPathInfo, conn: Connection | None) -> ValidPathInfo:
            return info

        async with Pieces(body()) as source:
            info = await self.router.add_to_store_from_dump(
                Write(op="AddToStore", dump=Dump(name, "text:sha256", frozenset(references))),
                source,
                client=WriteClient(),
                sign=unsigned,
            )
        return str(info.path)

    async def retire_idle_connections(self) -> int:
        return await self._daemon.retire_idle_connections()

    async def sign_path_info(
        self, request: Any, client: Any = None, suppress_last: bool = False, conn: Connection | None = None
    ) -> Any:
        return await self._daemon.sign_path_info(request, client=client, suppress_last=suppress_last, conn=conn)

    # The five operations `Store` declares abstract. `execute` never reaches
    # them: it gives the daemon client every operation the engine does not answer.

    async def is_valid_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        return await self._daemon.is_valid_path(request, client=client, suppress_last=suppress_last)

    async def query_path_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        return await self._daemon.query_path_info(request, client=client, suppress_last=suppress_last)

    async def query_path_from_hash_part(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        return await self._daemon.query_path_from_hash_part(request, client=client, suppress_last=suppress_last)

    async def query_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        return await self._daemon.query_valid_paths(request, client=client, suppress_last=suppress_last)

    async def nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        return await self._daemon.nar_from_path(request, client=client, suppress_last=suppress_last)

    # ── The engine ──────────────────────────────────────────────────

    @property
    def db(self) -> LocalStoreDB:
        """The engine's database. `start` replaces it; a start that fails before then still reaches `close`."""
        return self.engine.db

    def _refuse_routes_without_a_database(self) -> None:
        """A route set by hand to an answer that reads SQLite needs SQLite.

        A default route falls through instead, and the fall-through counter
        shows it. A route set by hand says the operator wants pynixd's answer,
        so a store that cannot give one says so at start.
        """
        if self.db.active:
            return
        asked = sorted(_OP_NAMES[op] for op, route in self.configured_routes.items() if route is not OpRoute.daemon)
        if asked:
            raise RuntimeError(
                f"op_routes: {', '.join(asked)} need the store database, and pynixd cannot read it: "
                f"{self._refuses_a_database() or 'the database did not open'}"
            )

    def _refuses_a_database(self) -> str | None:
        """Why this store must not use SQLite, or `None` when it may.

        Only the store URI can answer this, and pynixd builds the managed
        daemon's URI itself -- `StoreLayout.daemon_arguments` passes `--store
        <root>` for a chroot store and nothing at all for a relocated one, and
        both are a plain local store. `extra_args` is the one way a different
        store reaches the daemon, because it is appended after those arguments
        and a later `--store` wins.
        """
        overlay = next((arg for arg in self._daemon.extra_args if "local-overlay" in arg), None)
        if overlay is not None:
            return (
                f"the daemon is started with {overlay!r}; the store spec's `overlay` names an overlay "
                f"store, so that pynixd reads the lower store too"
            )
        return None

    @contextlib.asynccontextmanager
    async def _run(self) -> AsyncIterator[None]:
        """The daemon client's lifetime holds the engine's: the database opens after the daemon creates it."""
        async with self._daemon, super()._run():
            yield

    async def start(self, sync_paths: bool = True) -> None:
        """Open the SQLite database. The daemon client is running."""
        refusal = self._refuses_a_database()
        if refusal is not None:
            log.warning("local_store_db_refused", store_id=str(self.store_id), reason=refusal)
            self.engine.db = LocalStoreDB.inactive(self.layout)
        else:
            if not self._daemon.runs_daemon:
                # A daemon opens its store when it starts; with none, pynixd opens it. An overlay
                # opens its lower store as a plain local store, which makes it (`local-overlay-store.cc:50`).
                config = await self.engine.nix_config()
                layouts = [self.layout]
                if isinstance(self.engine, OverlayEngine):
                    layouts.append(self.engine.lower.layout)
                for layout in layouts:
                    self._big_locks.append(
                        await anyio.to_thread.run_sync(open_local_store, layout, config, self._daemon.nix_version)
                    )
            self.engine.db = await LocalStoreDB.open(self.layout)
            if isinstance(self.engine, OverlayEngine) and self.engine.db.active:
                self.engine.lower.db = await LocalStoreDB.open(self.engine.lower.layout, read_only=True)
                if not self.engine.lower.db.active:
                    # The upper database alone would call each lower path invalid.
                    log.warning("lower_store_db_unavailable", store_id=str(self.store_id))
                    await self.engine.db.close()
                    self.engine.db = LocalStoreDB.inactive(self.layout)
        self._refuse_routes_without_a_database()
        self.engine.hashes_self_reference_positions = hashes_self_reference_positions(self._daemon.nix_version)
        self.db.start(self._background)

    async def close(self) -> None:
        """Close the SQLite databases, and let go of the big lock."""
        await self.db.close()
        if isinstance(self.engine, OverlayEngine):
            await self.engine.lower.db.close()
        while self._big_locks:
            os.close(self._big_locks.pop())

    async def execute(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        skip_probe: bool = False,
        mark: bool = True,
    ) -> Any:
        """Note the paths of the request, then run it.

        This is the one place that sees every operation, whichever route
        answers it: a fast path over SQLite, or the wire. `LocalStoreDB`
        collects the paths and writes them a few seconds later.

        `mark` is for the collector's own bookkeeping queries: weighing
        names dead candidates to read their sizes, and recording those
        names as referenced would freshen exactly the paths the pass is
        judging. A dry-run must not move what it measures, so those calls
        pass `mark=False`. The veto keeps its marks: its seeds are live
        roots, and recording last-seen-live is what the access table is for.

        `mark_path` and `mark_paths` had no caller anywhere, in any project of
        this repository. The set they fill was therefore always empty,
        `flush_references` returned at its first line every time, and the
        background task woke every five seconds to do nothing. So
        `registrationTime` was never refreshed, and the LRU garbage collection
        that the refresh exists for never had an input. Issue nixidae/nixidae#205.

        Then the route of the operation picks who answers (`op_routes.py`).
        """
        if mark:
            self.db.mark_paths(referenced_paths(request))
        daemon = self._daemon
        if (extended := self.EXTENDED.get(request.op)) is not None:
            if not skip_probe:
                await daemon.probe()
            if answer := await getattr(self, extended)(request, client=client, suppress_last=suppress_last):
                return answer
            return await daemon.call(request, client=client, suppress_last=suppress_last)
        owned = self.OWNED.get(request.op)
        route = self.routes.get(request.op, OpRoute.daemon)
        if owned is None or route is OpRoute.daemon:
            return await daemon.execute(request, client=client, suppress_last=suppress_last, skip_probe=skip_probe)
        if route is OpRoute.pynixd:
            if not skip_probe:
                await daemon.probe()
            answer = await self._own_answer(owned, request, client, suppress_last)
            if answer is not None:
                return answer
            metrics.OP_FALLTHROUGH.labels(store_id=self.store_id, op=request.name).inc()
            return await daemon.execute(request, client=client, suppress_last=suppress_last, skip_probe=True)
        theirs = await daemon.execute(request, client=client, suppress_last=suppress_last, skip_probe=skip_probe)
        await self._shadow(owned, request, client, theirs)
        return theirs

    async def _own_answer(
        self, owned: Owned, request: WireRequest, client: ClientConn | None, suppress_last: bool
    ) -> Any:
        """pynixd's answer, or `None` where it has none and the daemon answers."""
        if not self.db.active:
            return None
        method = getattr(self, owned.method)
        try:
            return await method(request, client=client, suppress_last=suppress_last)
        except LowerPathNeedsSync:
            return None

    async def _shadow(self, owned: Owned, request: WireRequest, client: ClientConn | None, theirs: Any) -> None:
        """Compute pynixd's answer beside the daemon's, and count how they compare.

        The daemon's answer is the one the client gets, whatever happens here.
        The logs are left out of the comparison: pynixd notes its own answers
        there (`"pynixd: IsValidPath (SQLite hit)"`). A match logs nothing,
        because `IsValidPath` comes once for each derivation of a closure.
        """
        counter = metrics.OP_SHADOW
        try:
            ours = await self._own_answer(owned, request, client, suppress_last=True)
        except Exception:
            log.warning("op_shadow_error", op=request.name, exc_info=True)
            counter.labels(store_id=self.store_id, op=request.name, outcome="error").inc()
            return
        if ours is None:
            counter.labels(store_id=self.store_id, op=request.name, outcome="declined").inc()
            return
        mine, its = _value(ours, owned.unordered), _value(theirs, owned.unordered)
        if mine == its:
            counter.labels(store_id=self.store_id, op=request.name, outcome="match").inc()
            return
        counter.labels(store_id=self.store_id, op=request.name, outcome="mismatch").inc()
        log.warning(
            "op_shadow_mismatch",
            op=request.name,
            request=_bounded(request.model_dump(exclude={"logs"})),
            pynixd=_bounded(mine),
            daemon=_bounded(its),
        )

    # ── pynixd's answers, by route ─────────────────────────────────

    async def own_ensure_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: `DaemonProxy._substitute` runs the goals on this route (`goals/substitute_paths.py`).

        It stays None: the goal engine sends EnsurePath here to reach the
        daemon's substituters (`EnsureDerivedPathGoal._try_substitute_upstream`).
        """
        return None

    async def own_nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: a NAR is a stream, which `NarFromPathHandler` takes from `nar_pieces` on this route."""
        return None

    async def own_add_to_store(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: the dump follows the request, and `AddToStoreHandler` adds it with `store/add.py`."""
        return None

    async def own_add_to_store_nar(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: the NAR follows the request, and `AddToStoreNarHandler` imports it with `store/add.py`."""
        return None

    async def own_add_multiple_to_store(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: the paths follow the request, and `AddMultipleToStoreHandler` imports each with `store/add.py`."""
        return None

    async def own_add_indirect_root(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: `AddIndirectRootHandler` adds the root through the write router."""
        return None

    async def own_add_signatures(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: `AddSignaturesHandler` merges the signatures through the write router."""
        return None

    async def own_add_perm_root(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: `AddPermRootHandler` adds the root through the write router, with the session's temporary root."""
        return None

    async def own_add_build_log(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: `AddBuildLogHandler` records the log through the write router, after the framed body."""
        return None

    async def own_optimise_store(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: `OptimiseStoreHandler` deduplicates the store through the write router, holding each path."""
        return None

    async def own_verify_store(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: `VerifyStoreHandler` verifies the store through the write router, under the GC read lock."""
        return None

    async def own_collect_garbage(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """None: `CollectGarbageHandler` collects through the write router, under the GC write lock."""
        return None

    async def own_build_derivation(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """pynixd's sandbox builds it (`docs/design/builder.md`), or `None` and the daemon does."""
        options = client.options if client is not None else None
        # The scheduler refuses what no store builds when it places it; op 36 here has no scheduler.
        session = await self._session_build_settings(request, options)
        if session is not None:
            refusal = local_builder.rejection(str(request.drv_path), request.derivation, session)
            # With no daemon, no build hook takes what pynixd refuses.
            if refusal is not None and not (local_builder.hands_off(session) and self._daemon.runs_daemon):
                return BuildDerivationResponse(
                    result=BuildResult(
                        status=int(BuildResultStatus.INPUT_REJECTED),
                        error_msg=refusal,
                        times_built=0,
                        is_non_deterministic=0,
                        start_time=0,
                        stop_time=0,
                        built_outputs={},
                        built_outputs_by_name={},
                    )
                )
            if refusal is not None:
                # `execute` counts the fallthrough of an owned op that answers `None`.
                self._left_to_the_daemon(request, "a build only the daemon's builders can run", count=False)
                return None
        settings = await self.local_build_settings(request, options, count=False)
        if settings is None:
            return None

        async def send(message: LogMessage) -> None:
            if client is not None:
                await client.send(message)

        verbosity = options.verbosity if options is not None else Verbosity.INFO
        return await self.build_here(request, settings, send, verbosity)

    async def local_build_settings(
        self, request: BuildDerivationRequest, options: SetOptionsRequest | None, *, count: bool = True
    ) -> BuildSettings | None:
        """The settings pynixd's sandbox builds *request* with, or `None` when the daemon builds it.

        What `unsupported`, `_check_unsupported` or `_repair_unsupported` names stays on the daemon.
        """
        settings = await self._session_build_settings(request, options)
        if settings is None:
            return None
        layout = self.engine.layout
        reason = (
            local_builder.unsupported(
                request.derivation,
                settings,
                store_dir=str(layout.store_dir),
                relocated=layout.store_dir != layout.real_store_dir,
            )
            or await self._realisations_unsupported(request)
            or await self._check_unsupported(request, settings)
            or await self._repair_unsupported(request)
            or self._recursive_unsupported(request)
        )
        if reason is None:
            return settings
        self._left_to_the_daemon(request, reason, count=count)
        return None

    async def _session_build_settings(
        self, request: BuildDerivationRequest, options: SetOptionsRequest | None
    ) -> BuildSettings | None:
        """The store's settings under the session's, or `None` when pynixd builds nothing here."""
        if self.routes.get(BuildDerivationRequest.op) is not OpRoute.pynixd or not self.engine.writable:
            return None
        # `builtin:fetchurl` sends `nixVersion` in its User-Agent.
        if request.derivation.builder == "builtin:fetchurl" and self._nix_version is None:
            nix_bin, extra_env = self._nix_version_command
            self._nix_version = await read_nix_version(nix_bin, nix_environment(self.engine.layout, extra_env))
        return BuildSettings.from_nix_config(
            await self.engine.nix_config(), _build_overrides(options), nix_version=self._nix_version or ""
        )

    def _left_to_the_daemon(self, request: BuildDerivationRequest, reason: str, *, count: bool) -> None:
        log.info("build_left_to_the_daemon", drv=str(request.drv_path), reason=reason)
        if count:
            metrics.OP_FALLTHROUGH.labels(store_id=self.store_id, op=request.name).inc()

    def _recursive_unsupported(self, request: BuildDerivationRequest) -> str | None:
        features = set(request.derivation.required_system_features)
        if {"recursive-nix", local_builder.BUILDER_RPC} & features and self.recursive_server is None:
            return "recursive-nix or builder-rpc-v0 with no server for the builder"
        return None

    async def _check_unsupported(self, request: BuildDerivationRequest, settings: BuildSettings) -> str | None:
        """Why the daemon checks *request*, or `None`; also `None` for a request that does not check.

        `DerivationGoal` substitutes a missing output before it checks
        (`derivation-goal.cc:113`). pynixd's goals do that before they send a
        check; an output that is still not valid here goes to the daemon.
        """
        if request.build_mode != BuildMode.CHECK:
            return None
        paths = await self._known_paths(request)
        if paths is None:
            return "a check of an output with no path"
        if not all([await self.engine.is_valid_path_uncached(path) for path in paths]):
            return "a check of an output that is not valid"
        return None

    async def _repair_unsupported(self, request: BuildDerivationRequest) -> str | None:
        """Why the daemon repairs *request*, or `None`; also `None` for a request that does not repair.

        pynixd repairs an output whose path the derivation or a realisation names, and builds an impure one again.
        """
        if request.build_mode != BuildMode.REPAIR:
            return None
        return "a repair of an output with no path" if await self._known_paths(request) is None else None

    async def _known_paths(self, request: BuildDerivationRequest) -> list[str] | None:
        """Each output's path: the derivation's, else a floating output's realised one. `None` when one has none.

        An impure derivation has none to check or replace: it builds again (`derivation-goal.cc:71`).
        """
        if request.derivation.has_impure:
            return []
        paths: list[str] = []
        by_path, keys = self._realisation_keys(request)
        for name, output in request.derivation.outputs.items():
            path = output.path
            if not path and await self._ca_derivations():
                found = await self.engine.query_realisation(keys[name], name, by_path=by_path)
                path = str(found[0]) if found is not None else ""
            if not path:
                return None
            paths.append(path)
        return paths

    async def contents_good(self, path: str) -> bool:
        """`Worker::pathContentsGood` (`worker.cc:553`) without its cache and its lines."""
        return await self.engine.contents_good(path)

    async def _ca_derivations(self) -> bool:
        return "ca-derivations" in (await self.trust_policy()).experimental_features

    async def _realisations_unsupported(self, request: BuildDerivationRequest) -> str | None:
        """Why the daemon builds *request* for its realisations, or `None`.

        Without `ca-derivations` the daemon refuses a floating output when it
        reads the derivation, and without `impure-derivations` an impure one
        (`derivations.cc:325`).
        """
        kinds = {o.kind for o in request.derivation.outputs.values()}
        features = (await self.trust_policy()).experimental_features
        if OutputKind.IMPURE in kinds and "impure-derivations" not in features:
            return "an impure output without impure-derivations"
        if not await self._ca_derivations():
            return "a floating output without ca-derivations" if OutputKind.CA_FLOATING in kinds else None
        return None

    def _realisation_keys(self, request: BuildDerivationRequest) -> tuple[bool, dict[str, str]]:
        """Whether this store keys a realisation by the derivation's path, and each output's key.

        Nix 2.34 keys it by `staticOutputHashes` of the derivation the daemon read (`derivation-building-goal.cc:307`
        of 2.34), which has no input derivation.
        """
        if FEATURE_REALISATION_WITH_PATH in self.features:
            return True, dict.fromkeys(request.derivation.outputs, request.drv_path.name)
        hashes = from_basic_derivation(request.derivation).hash_derivation_modulo(mask_outputs=True)
        return False, {name: f"sha256:{digest}" for name, digest in hashes.items()}

    async def _realised(self, request: BuildDerivationRequest) -> dict[str, str]:
        """`checkPathValidity` (`derivation-building-goal.cc:1329`): the realised path of each output.

        A valid static output with no realisation gets one, as Nix registers it there. An impure
        derivation has none.
        """
        realised: dict[str, str] = {}
        if request.derivation.has_impure or not await self._ca_derivations():
            return realised
        by_path, keys = self._realisation_keys(request)
        for name, output in request.derivation.outputs.items():
            found = await self.engine.query_realisation(keys[name], name, by_path=by_path)
            if found is not None:
                realised[name] = str(found[0])
            elif output.path and await self.engine.is_valid_path_uncached(output.path):
                await self.engine.register_drv_output(keys[name], name, output.path, (), by_path=by_path)
        return realised

    async def _register_realisations(
        self, request: BuildDerivationRequest, built: Iterable[tuple[str, str]], settings: BuildSettings
    ) -> dict[str, set[Signature]]:
        """The end of `registerOutputs` (`derivation-builder.cc:1656`): sign and register each built output.

        Answers each output's signatures, which `builtOutputs` carries too.
        """
        by_path, drv_keys = self._realisation_keys(request)
        fingerprint = realisation_fingerprint if by_path else hash_keyed_realisation_fingerprint
        keys = [SecretKey.from_file(Path(file)) for file in settings.secret_key_files]
        signed: dict[str, set[Signature]] = {}
        for name, path in built:
            fp = fingerprint(drv_keys[name], name, path.rsplit("/", 1)[-1])
            sigs = signed[name] = {Signature(key.sign_fingerprint(fp)) for key in keys}
            await self.engine.register_drv_output(drv_keys[name], name, path, sigs, by_path=by_path)
        return signed

    async def build_here(
        self,
        request: BuildDerivationRequest,
        settings: BuildSettings,
        on_log: Callable[[LogMessage], Awaitable[None]],
        verbosity: int,
    ) -> BuildDerivationResponse:
        """Build *request* in pynixd's sandbox and register its outputs in this store.

        *on_log* gets Nix's `actBuild` activity (`BuildActivity`). *verbosity*
        is the client's, which a builder's `@nix` message must pass.
        """
        drv_path = str(request.drv_path)
        realised = await self._realised(request)
        checking = request.build_mode == BuildMode.CHECK
        repairing = request.build_mode == BuildMode.REPAIR
        activity = BuildActivity(drv_path, on_log, verbosity, checking=checking, repairing=repairing)
        used: list[BuildUsage] = []
        await activity.start()
        try:
            built = await local_builder.build(
                self.engine,
                drv_path,
                request.derivation,
                settings,
                on_lines=activity.emit,
                realised=realised,
                check=checking,
                repair=repairing,
                recursive=self.recursive_server,
                vm=self.build_vm,
                on_usage=used.append,
                sample_seconds=self.usage_sample_seconds,
            )
            signed: dict[str, set[Signature]] = {}
            if (
                built.status is BuildResultStatus.BUILT
                and not request.derivation.has_impure
                and await self._ca_derivations()
            ):
                signed = await self._register_realisations(
                    request, ((name, info.path.path) for name, info in built.outputs.items()), settings
                )
            # Inside `actBuild`, which the goal keeps until it ends (`derivation-building-goal.cc:1079`).
            if settings.post_build_hook and built.status is BuildResultStatus.BUILT:
                failure = await self._post_build_hook(settings, drv_path, built.paths.values(), on_log)
                if failure is not None:
                    built = dataclasses.replace(built, status=BuildResultStatus.MISC_FAILURE, error_msg=failure)
        finally:
            with anyio.CancelScope(shield=True):
                await activity.stop()
        # `--print-out-paths` prints these; `GoalEngine` keys them by `DrvOutput`, and copies the signatures.
        # `BuildResult.for_the_wire` fills the shape the client reads from the one this store's Nix writes.
        by_path, keys = self._realisation_keys(request)
        by_name: dict[str, UnkeyedRealisation] | None = None
        by_id: dict[str, Realisation] | None = None
        if by_path:
            by_name = {
                name: UnkeyedRealisation(out_path=StorePath(path), signatures=signed.get(name, set()))
                for name, path in built.paths.items()
            }
        else:
            by_id = {
                f"{keys[name]}!{name}": Realisation(
                    id=DrvOutput(drv_hash=keys[name], output_name=name),
                    out_path=StorePath(path.rsplit("/", 1)[-1]),
                    signatures=sorted(signed.get(name, set()), key=lambda s: (s.name, s.signature)),
                )
                for name, path in built.paths.items()
            }
        return BuildDerivationResponse(
            result=BuildResult(
                status=int(built.status),
                error_msg=built.error_msg,
                times_built=1 if built.status is BuildResultStatus.BUILT else 0,
                is_non_deterministic=int(built.status is BuildResultStatus.NOT_DETERMINISTIC),
                start_time=built.start_time,
                stop_time=built.stop_time,
                built_outputs=by_id,
                built_outputs_by_name=by_name,
                # NIX-DEVIATION (#27): `linux-derivation-builder.cc:950` fills `cpuUser` and `cpuSystem`
                # from the build's cgroup alone, so without `use-cgroups` Nix sends neither. pynixd sends
                # the helper's `wait4` usage too: the scheduler's observations read it (nixidae/nixidae#1187).
                # To reverse it, show a client that misreads a BuildResult with CPU times.
                cpu_user=OptMicroseconds(tag=1, value=used[0].cpu_user_us) if used else OptMicroseconds(),
                cpu_system=OptMicroseconds(tag=1, value=used[0].cpu_system_us) if used else OptMicroseconds(),
                resources=_resources(used[0]) if used else BuildResources(),
            )
        )

    async def _post_build_hook(
        self,
        settings: BuildSettings,
        drv_path: str,
        out_paths: Iterable[str],
        on_log: Callable[[LogMessage], Awaitable[None]],
    ) -> str | None:
        """`runPostBuildHook` (`derivation-building-goal.cc:1105`) after a build or a check (`:1066`): its failure.

        The hook gets the daemon's environment, and `NIX_CONFIG` holds every setting as the session set them.
        """
        nix_bin, extra_env = self._nix_version_command
        env = nix_environment(self.engine.layout, extra_env)
        # An inherited `NIX_CONFIG` wins (`hooks.post_build_hook`), so the dump would go unused.
        config = (
            env["NIX_CONFIG"]
            if "NIX_CONFIG" in env
            else await read_nix_config_text(nix_bin, env, self._daemon.config_arguments(), settings.session)
        )
        activity = PostBuildHookActivity(settings.post_build_hook, drv_path, on_log)
        await activity.start()
        try:
            code = await hooks.post_build_hook(
                settings.post_build_hook, env, drv_path, out_paths, config, on_line=activity.line
            )
        finally:
            with anyio.CancelScope(shield=True):
                await activity.stop()
        if code == 0:
            return None
        return show_error(f'program "{magenta(settings.post_build_hook)}" {magenta(hooks.status_text(code))}')

    async def import_settings(self) -> ImportSettings:
        """What `addToStore` reads of this store's Nix settings. Read once, as the daemon reads them."""
        return await self.engine.settings()

    async def nar_pieces(self, path: StorePath) -> AsyncIterator[bytes]:
        """The NAR of *path*, dumped from the store's files (`Store::narFromPath`, `store-api.cc:385`).

        Each piece is read on a worker thread, one hop per piece of about 64
        KiB. A thread that fed a queue would need a task group across the
        `yield`, which an async generator cannot hold.

        The files are under this store's `real_store_dir`. `on_disk` reads the
        process's one store setting, which in a process with several chroot
        stores names whichever was set last.
        """
        from anyio.to_thread import run_sync

        from pynixd_store.nar_fs import dump_path

        pieces = dump_path(self.layout.real_store_dir / path.name)
        try:
            while (piece := await run_sync(next, pieces, None)) is not None:
                yield piece
        finally:
            pieces.close()

    async def register_valid_paths(self, infos: Iterable[ValidPathInfo]) -> None:
        """Make *infos* valid in Nix's own tables, as `LocalStore::registerValidPaths` does.

        Only a store with `pynixd_writes` on may call this (`StoreEngine.register_valid_paths`).
        """
        await self.engine.register_valid_paths(infos)

    async def own_is_valid_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """IsValidPath, from the engine.

        The session's synchronous reader answers when it can. It skips the
        `aiosqlite` thread hop, which is most of the cost of the query: one
        build sends an `IsValidPath` for every derivation of its closure.
        """
        from pynixd.serde import IsValidPathResponse

        valid = await self.engine.is_valid_path(str(request.path), reader=getattr(client, "sync_reader", None))
        return IsValidPathResponse.fast(valid=valid)

    async def own_query_path_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathInfo: the path-info cache, then the engine."""
        from pynixd.serde import QueryPathInfoResponse

        info = self.get_path_info(request.path) or await self.engine.query_path_info(
            str(request.path), reader=getattr(client, "sync_reader", None)
        )
        if info is None:
            return QueryPathInfoResponse.fast(valid=False)
        return QueryPathInfoResponse.fast(valid=True, info=info.info)

    async def own_query_all_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryAllValidPaths, from the engine."""
        from pynixd.serde import QueryAllValidPathsResponse

        return QueryAllValidPathsResponse(paths=await self.engine.query_all_valid_paths())

    async def own_query_referrers(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryReferrers (`daemon.cc:397`), from the engine."""
        from nix_daemon_protocol.query_referrers import QueryReferrersResponse

        return QueryReferrersResponse.fast(paths=await self.engine.query_referrers(str(request.path)))

    async def own_query_valid_derivers(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryValidDerivers (`daemon.cc:398`), from the engine."""
        from nix_daemon_protocol.query_valid_derivers import QueryValidDeriversResponse

        return QueryValidDeriversResponse.fast(paths=await self.engine.query_valid_derivers(str(request.path)))

    async def own_query_realisation(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryRealisation: the build trace of one derivation output, in both wire shapes.

        The table and its key follow the shape this store's daemon reads, the
        way `query_realisation` of `DaemonStore` turns the request. Without
        `ca-derivations` the daemon answers: Nix 2.34 answers nothing there,
        and Nix master never prepares the statement.
        """
        from nix_daemon_protocol.constants import FEATURE_REALISATION_WITH_PATH
        from nix_daemon_protocol.query_realisation import QueryRealisationResponse
        from nix_daemon_protocol.realisation import Realisation
        from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation

        from .daemon import answer_in_both_shapes

        if "ca-derivations" not in (await self.trust_policy()).experimental_features:
            return None
        shaped = await self._daemon.request_in_the_shape_this_store_reads(request)
        by_path = FEATURE_REALISATION_WITH_PATH in self.features
        if by_path:
            keyed = shaped.keyed_drv_output
            found = await self.engine.query_realisation(keyed.drv_path.name, keyed.output_name, by_path=True)
        else:
            drv_output = shaped.drv_output
            found = await self.engine.query_realisation(drv_output.drv_hash, drv_output.output_name, by_path=False)
        if found is None:
            return QueryRealisationResponse()
        out_path, signatures = found
        if by_path:
            response = QueryRealisationResponse(
                present=1, realisation=UnkeyedRealisation(out_path=out_path, signatures=set(signatures))
            )
        else:
            response = QueryRealisationResponse(
                realisations=[Realisation(id=shaped.drv_output, out_path=out_path, signatures=signatures)]
            )
        return answer_in_both_shapes(response, request)

    async def own_register_drv_output(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """RegisterDrvOutput: record one build-trace entry, as `LocalStore::registerDrvOutput` does.

        The table and its key follow the shape this store's daemon reads, the
        way `register_drv_output` of `DaemonStore` turns the request. Without
        `ca-derivations` the daemon answers, which raises Nix's own
        `experimental-feature` error. This op runs through `execute` on both
        the client wire and the scheduler's own registration (`goals/ensure.py`).
        """
        from nix_daemon_protocol.constants import FEATURE_REALISATION_WITH_PATH
        from nix_daemon_protocol.register_drv_output import RegisterDrvOutputResponse

        if "ca-derivations" not in (await self.trust_policy()).experimental_features:
            return None
        shaped = await self._daemon.request_in_the_shape_this_store_reads(request)
        by_path = FEATURE_REALISATION_WITH_PATH in self.features
        if by_path:
            keyed, unkeyed = shaped.keyed_drv_output, shaped.unkeyed_realisation
            drv_key, output_name, out_path, sigs = (
                keyed.drv_path.name,
                keyed.output_name,
                str(unkeyed.out_path),
                unkeyed.signatures,
            )
        else:
            realisation = shaped.realisation
            drv_key, output_name, out_path, sigs = (
                realisation.id.drv_hash,
                realisation.id.output_name,
                str(realisation.out_path),
                realisation.signatures,
            )
        await self.engine.register_drv_output(drv_key, output_name, out_path, sigs, by_path=by_path)
        return RegisterDrvOutputResponse()

    async def own_query_substitutable_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QuerySubstitutablePaths: the paths some substituter holds, from the scheduler's race.

        NIX-DEVIATION (#27): `Store::querySubstitutablePaths` at
        `store-api.cc:504` asks the substituters one after another, in
        priority order, and skips each one without `want-mass-query`. pynixd
        asks every substituter backend at once for each path, takes the first
        hit, and keeps hits and misses in the queue's caches
        (`SubstitutionQueue.can_substitute`), as its binary cache proxy does.
        The set is the same while the caches agree: a path counts when any
        substituter holds it, in both. The cost is a path a cache without
        `want-mass-query` holds, which Nix leaves out and pynixd includes.
        To reverse it, measure that against the time of the sequential walk.

        A client that names its own substituters is asked of backends for
        those (`SubstitutionQueue.substituters_for`). A substituter no backend
        stands for, the client's or the daemon's, sends the question to the
        daemon. Without `substitute`, nothing is substitutable
        (`store-api.cc:506`).
        """
        from nix_daemon_protocol.query_substitutable_paths import QuerySubstitutablePathsResponse

        queue = self.substitution_queue
        if queue is None:
            return None
        stores = await queue.substituters_for(client)
        if stores is None:
            return None
        if not stores:
            return QuerySubstitutablePathsResponse.fast(paths=set())

        found: set[StorePath] = set()

        async def ask(path: StorePath) -> None:
            if (await queue.can_substitute(path, stores)).available:
                found.add(path)

        async with anyio.create_task_group() as tg:
            for path in request.paths:
                tg.start_soon(ask, path)
        return QuerySubstitutablePathsResponse.fast(paths=found)

    async def own_query_substitutable_path_info(
        self, request: Any, client: Any = None, suppress_last: bool = False
    ) -> Any:
        """QuerySubstitutablePathInfo (`daemon.cc:819`): op 30 for one path, with no path in the answer."""
        from nix_daemon_protocol.query_substitutable_path_info import (
            QuerySubstitutablePathInfoResponse,
            UnkeyedSubstitutablePathInfo,
        )

        found = await self._substitutable_path_infos({request.path: None}, client)
        if found is None:
            return None
        if not found:
            return QuerySubstitutablePathInfoResponse.fast(found=False)
        (info,) = found
        return QuerySubstitutablePathInfoResponse.fast(
            found=True,
            info=UnkeyedSubstitutablePathInfo(
                deriver=info.deriver,
                references=info.references,
                download_size=info.download_size,
                nar_size=info.nar_size,
            ),
        )

    async def own_query_substitutable_path_infos(
        self, request: Any, client: Any = None, suppress_last: bool = False
    ) -> Any:
        """QuerySubstitutablePathInfos (`daemon.cc:837`): what a substituter knows of each path it holds.

        A content address lets a substituter of another store directory hold the path (`store-api.cc:461`).
        """
        from nix_daemon_protocol.query_substitutable_path_infos import QuerySubstitutablePathInfosResponse
        from pynixd_store.content_address import ContentAddress

        addresses = {path: ContentAddress.parse(str(ca)) if ca else None for path, ca in request.paths.items()}
        found = await self._substitutable_path_infos(addresses, client)
        if found is None:
            return None
        return QuerySubstitutablePathInfosResponse.fast(infos=found)

    async def _substitutable_path_infos(
        self, paths: Mapping[StorePath, ContentAddress | None], client: Any
    ) -> list[Any] | None:
        """What the highest-priority backend holding each path knows of it, in Nix's map order.

        `querySubstitutablePathInfos` (`misc.cc:90`) walks the substituters
        in priority order and takes the first that holds the path, which is
        the one `SubstitutionQueue.get_substituter` picks. Without
        `substitute` nothing is substitutable (`misc.cc:92`). `None` where
        the daemon answers, as `own_query_substitutable_paths` decides.

        NIX-DEVIATION (#27): a substituter whose query fails reads as one
        that lacks the path. With `fallback` off Nix raises that error
        (`misc.cc:151`). pynixd's race counts the failure against the
        store's health instead, so one unreachable cache does not fail every
        plan. To reverse it, measure how often a plan fails that way on Nix.
        """
        from nix_daemon_protocol.query_substitutable_path_infos import SubstitutablePathInfo

        queue = self.substitution_queue
        if queue is None:
            return None
        stores = await queue.substituters_for(client)
        if stores is None:
            return None
        if not stores:
            return []

        found: list[SubstitutablePathInfo] = []

        async def ask(path: StorePath, ca: ContentAddress | None) -> None:
            candidate = await queue.get_substituter(path, stores, ca=ca)
            if candidate is None:
                return
            info = candidate.path_info.info
            found.append(
                SubstitutablePathInfo(
                    path=path,
                    deriver=info.deriver,
                    references=set(info.references),
                    download_size=candidate.download_size,
                    nar_size=info.nar_size,
                )
            )

        async with anyio.create_task_group() as tg:
            for path, ca in paths.items():
                tg.start_soon(ask, path, ca)
        return sorted(found, key=lambda info: info.path.name)

    async def own_find_roots(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """FindRoots: every root and the links that name it, uncensored (`gc.cc:319`).

        The handler censors for an untrusted client. Nix walks the roots in
        the daemon, so the walk sees what the daemon's user sees. A managed
        daemon is pynixd's own child, and root sees everything; any other
        daemon may see more than pynixd, and it answers instead.
        """
        import os

        from nix_daemon_protocol.find_roots import FindRootsEntry, FindRootsResponse

        if not self.managed and os.geteuid() != 0:
            return None
        # As on the daemon route (`find_roots` of `DaemonStore`): an idle pooled
        # worker holds a temporary root for each path it took, which no client holds.
        await self.retire_idle_connections()
        # Nix tidies in the walk; pynixd only with `pynixd_writes`.
        roots = await self.engine.find_roots(tidy=self.pynixd_writes)
        return FindRootsResponse.fast(roots=[FindRootsEntry(link=link, target=path) for link, path in roots])

    async def own_query_derivation_output_map(
        self, request: Any, client: Any = None, suppress_last: bool = False
    ) -> Any:
        """QueryDerivationOutputMap: the outputs of a derivation (`store-api.cc:467`).

        `DerivationOutputs` holds the outputs whose path is known statically.
        With `ca-derivations`, each output of the derivation then takes its
        realisation, and an output with none and no row maps to the empty
        path (`outputs-query.cc:119`). Nix master keys the realisation by the
        derivation path; Nix 2.34 by the hash of each output
        (`staticOutputHashes`, `store-api.cc:406` of 2.34).

        A derivation that is not valid goes to the daemon, which refuses it
        with Nix's own error. So does one whose file pynixd cannot read or
        whose hashes the closure cannot give.
        """
        from nix_daemon_protocol.constants import FEATURE_REALISATION_WITH_PATH
        from nix_daemon_protocol.query_derivation_output_map import QueryDerivationOutputMapResponse
        from pynixd_store.drv_hash import output_hashes

        items = await self.engine.query_static_derivation_outputs(str(request.path))
        if items is None:
            return None
        if "ca-derivations" not in (await self.trust_policy()).experimental_features:
            return QueryDerivationOutputMapResponse.fast(items=items)

        parsed = await self.read_derivation(request.path)
        if parsed is None:
            return None
        by_path = FEATURE_REALISATION_WITH_PATH in self.features
        if by_path:
            keys = {output.output_name: request.path.name for output in parsed.outputs}
        else:
            hashes = await output_hashes(parsed, self.read_derivation)
            if hashes is None:
                return None
            keys = {name: f"sha256:{digest}" for name, digest in hashes.items()}
        for name, drv in keys.items():
            found = await self.engine.query_realisation(drv, name, by_path=by_path)
            if found is not None:
                items[name] = found[0]
            else:
                items.setdefault(name, StorePath())
        return QueryDerivationOutputMapResponse.fast(items=items)

    async def own_query_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryValidPaths — fast-path via SQLite."""
        if request.substitute:
            # The daemon substitutes before it queries: `daemon.cc:377` runs
            # `substitutePaths` ahead of `queryValidPaths`, so a missing path
            # its substituters hold comes back valid. SQLite holds no
            # substituter, so answering from it would call such a path
            # invalid. Yield to the wire instead, where the daemon does the
            # fetching. This also carries the flag the deployment forces
            # (`force_substitute_on_destination`): without it, forcing would
            # answer from the mirror and substitute nothing. Issue #54.
            return None  # fall through
        from pynixd.serde import QueryValidPathsResponse

        return QueryValidPathsResponse(paths=await self.engine.query_valid_paths(str(p) for p in request.paths))

    async def own_query_path_from_hash_part(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathFromHashPart: the valid path whose hash part this is (`local-store.cc:958`).

        A miss is the empty path. A hash part of another length goes to the
        daemon, which refuses it with Nix's own error.
        """
        from pynixd.serde import QueryPathFromHashPartResponse, StorePath

        if len(request.path) != _HASH_PART_LENGTH:
            return None
        found = await self.engine.query_path_from_hash_part(str(request.path))
        return QueryPathFromHashPartResponse.fast(value=found or StorePath(path=""))

    async def query_closure(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryClosure, from the engine."""
        from pynixd.serde import QueryClosureResponse

        return QueryClosureResponse(paths=await self.engine.query_closure(str(p) for p in request.paths))

    async def query_closure_with_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryClosureWithInfo, from the engine."""
        from pynixd.serde import QueryClosureWithInfoResponse

        if not request.paths:
            return QueryClosureWithInfoResponse(infos=[])
        return QueryClosureWithInfoResponse(
            infos=await self.engine.query_closure_with_info(str(p) for p in request.paths)
        )

    async def query_path_infos(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathInfos: the path-info cache for each path, then the engine for the rest."""
        from pynixd.serde import QueryPathInfosResponse

        cached: list[ValidPathInfo] = []
        uncached: list[str] = []
        for path in request.paths:
            info = self.get_path_info(path)
            if info is not None:
                cached.append(info)
            else:
                uncached.append(str(path))
        if not uncached:
            return QueryPathInfosResponse(infos=cached)
        return QueryPathInfosResponse(infos=[*cached, *await self.engine.query_path_infos(uncached)])

    async def query_derivation_output_map_batch(
        self, request: Any, client: Any = None, suppress_last: bool = False
    ) -> Any:
        """QueryDerivationOutputMapBatch: the engine's static outputs, else the outputs of the parsed derivation."""
        from pynixd.daemon_extensions.query_derivation_output_map_batch import DerivationOutputMapBatchResponse

        if not request.drv_paths:
            return DerivationOutputMapBatchResponse(outputs={})
        result = await self.engine.query_derivation_output_maps(str(p) for p in request.drv_paths)
        for drv_path in request.drv_paths:
            sp = StorePath(path=str(drv_path))
            if sp in result:
                continue
            try:
                parsed = await self.read_derivation(drv_path)
                if parsed is None:
                    continue
                result[sp] = dict(parsed.output_paths().items())
            except FileNotFoundError:
                pass

        return DerivationOutputMapBatchResponse(outputs=result)


# `daemon.cc:271-283`: the daemon keeps its own value of these, even for a trusted client.
_NEVER_APPLIED = frozenset({"experimental-features", "plugin-files", "ssh-auth-sock"})


def _build_overrides(options: SetOptionsRequest | None) -> dict[str, str]:
    """The session's settings for a build: `ClientSettings::apply` (`daemon.cc:234`).

    The fixed fields of SetOptions apply first and the overrides over them.
    `build-timeout` is the old name of `timeout`.
    """
    if options is None:
        return {}
    overrides = {
        "keep-failed": "true" if options.keep_failed else "false",
        "keep-going": "true" if options.keep_going else "false",
        "fallback": "true" if options.try_fallback else "false",
        "max-jobs": str(options.max_build_jobs),
        "max-silent-time": str(int(options.max_silent_time)),
        "cores": str(options.build_cores),
        "substitute": "true" if options.use_substitutes else "false",
    }
    # `ClientSettings::overrides` is a `std::map`: `apply` walks it sorted.
    overrides.update((k, v) for k, v in sorted(options.overrides.items()) if k not in _NEVER_APPLIED)
    if "build-timeout" in overrides:
        overrides.setdefault("timeout", overrides["build-timeout"])
    return overrides


def write_router(store: Store) -> WriteRouter:
    """Who writes *store*: its own router, or the daemon alone for a store pynixd does not write."""
    if isinstance(store, LocalDBStore):
        return store.router
    client = store.daemon_client
    if client is None:
        raise TypeError(f"store {store.store_id} has no daemon to write to")
    return WriteRouter.daemon_only(client)
