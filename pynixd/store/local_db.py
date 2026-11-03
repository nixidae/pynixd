"""LocalDBStore — LocalStore with SQLite database for fast-path queries."""

from __future__ import annotations

import contextlib
import functools
import types
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Union, get_args, get_origin

import anyio
import structlog
from pydantic import BaseModel

from nix_daemon_protocol.operations import STANDARD_OPERATIONS
from pynixd_store.engine import StoreEngine
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.nix_config import nix_environment, read_nix_config

from .. import metrics
from ..op_routes import OpRoute, Owned
from ..serde import StorePath
from ..write_router import WriteRouter
from .base import Store
from .daemon_writes import DaemonWrites
from .local_daemon import LocalStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Set as AbstractSet
    from contextlib import AbstractAsyncContextManager

    from nix_daemon_protocol.valid_path_info import ValidPathInfo
    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd_store.add import ImportSettings
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


class LocalDBStore(Store):
    """pynixd's local store: the engine, the managed daemon, and the router between them.

    The daemon client (`LocalStore`) builds, and answers every operation the
    engine does not. The scheduler builds on it through `daemon_client`.

    Each `own_*` method answers from the engine when the database is open,
    and returns `None` when it is not. `execute` then asks the daemon, so a
    database pynixd cannot open costs correctness nothing.

    **The fast paths are valid for a plain local store only. A
    `local-overlay-store` must not use this class.** They read one database,
    and an overlay store keeps its lower store's paths in a second one:
    `LocalOverlayStore::isValidPathUncached` asks `LocalStore` first, then
    `lowerStore`, and only then copies the lower path's info up with
    `LocalStore::registerValidPath`. Reading the upper database alone would
    report a valid lower path as invalid *and* skip the sync that would have
    made it valid. The same applies to `queryPathInfoUncached`,
    `queryReferrers`, `queryValidPaths` and `queryPathFromHashPart`, each of
    which overlay overrides for the same reason.

    `_refuses_a_database` is what keeps that from happening quietly.
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
        7: Owned("own_add_to_store", writes=True),
        39: Owned("own_add_to_store_nar", writes=True),
        44: Owned("own_add_multiple_to_store", writes=True),
        12: Owned("own_add_indirect_root", writes=True),
        37: Owned("own_add_signatures", writes=True),
        47: Owned("own_add_perm_root", writes=True),
        32: Owned("own_query_substitutable_paths", default=OpRoute.pynixd),
        33: Owned("own_query_valid_derivers", default=OpRoute.pynixd),
        41: Owned("own_query_derivation_output_map", default=OpRoute.pynixd),
        42: Owned("own_register_drv_output", writes=True),
        45: Owned("own_add_build_log", writes=True),
        43: Owned("own_query_realisation", default=OpRoute.pynixd),
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
        self.engine = StoreEngine(
            self.layout,
            writes=spec.pynixd_writes,
            read_config=lambda: read_nix_config(spec.nix_bin, nix_environment(self.layout, spec.extra_env)),
            on_change=self.forget_path_info,
            name=str(self.store_id),
        )
        #: The scheduler's race over the substituters; the instance sets it at start.
        self.substitution_queue: SubstitutionQueue | None = None
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
        self.routes = {
            op: self.configured_routes.get(op, OpRoute.daemon if owned.writes else owned.default)
            for op, owned in self.OWNED.items()
        }
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
        if parsed is not None:
            return parsed
        return await self._daemon.read_derivation(drv_store_path)

    async def add_text_to_store(self, name: str, text: str, references: AbstractSet[str]) -> str:
        return await self._daemon.add_text_to_store(name, text, references)

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
                f"the daemon is started with {overlay!r}, and the SQLite fast paths read one "
                f"database. An overlay store keeps its lower paths in another one, so a fast "
                f"path would call a valid path invalid."
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
            self.engine.db = await LocalStoreDB.open(self.layout)
        self._refuse_routes_without_a_database()
        self.db.start(self._background)

    async def close(self) -> None:
        """Close the SQLite database."""
        await self.db.close()

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
        return await method(request, client=client, suppress_last=suppress_last)

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

        A client that names its own substituters gets the daemon's answer:
        those are not pynixd's backends. Without `substitute`, nothing is
        substitutable (`store-api.cc:506`).
        """
        from nix_daemon_protocol.query_substitutable_paths import QuerySubstitutablePathsResponse

        from ..goals.query_missing import client_names_a_substituter

        queue = self.substitution_queue
        if queue is None or client_names_a_substituter(client):
            return None
        options = client.options if client is not None else None
        if options is not None and not options.use_substitutes:
            return QuerySubstitutablePathsResponse.fast(paths=set())

        found: set[StorePath] = set()

        async def ask(path: StorePath) -> None:
            if (await queue.can_substitute(path)).available:
                found.add(path)

        async with anyio.create_task_group() as tg:
            for path in request.paths:
                tg.start_soon(ask, path)
        return QuerySubstitutablePathsResponse.fast(paths=found)

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


def write_router(store: Store) -> WriteRouter:
    """Who writes *store*: its own router, or the daemon alone for a store pynixd does not write."""
    if isinstance(store, LocalDBStore):
        return store.router
    client = store.daemon_client
    if client is None:
        raise TypeError(f"store {store.store_id} has no daemon to write to")
    return WriteRouter.daemon_only(client)
