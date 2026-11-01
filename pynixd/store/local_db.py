"""LocalDBStore — LocalStore with SQLite database for fast-path queries."""

from __future__ import annotations

import functools
import types
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Union, get_args, get_origin

import structlog
from pydantic import BaseModel

from nix_daemon_protocol.operations import STANDARD_OPERATIONS
from nix_daemon_protocol.store_dir import store_prefix

from .. import metrics
from ..local_store_db import LocalStoreDB
from ..op_routes import OpRoute, Owned
from ..serde import StorePath
from .local_daemon import LocalStore

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireRequest

    from ..config import LocalSocketStoreSpec
    from ..connection import ClientConn

log = structlog.get_logger(__name__)

_OP_CODES = {op.name: op.code for op in STANDARD_OPERATIONS}
_OP_NAMES = {op.code: op.name for op in STANDARD_OPERATIONS}
_HASH_PART_LENGTH = 32  # `StorePath::HashLen`

#: The most of one value a shadow mismatch logs: `QueryAllValidPaths` answers the whole store.
_LOGGED_CHARACTERS = 2000


def _value(response: Any) -> Any:
    """What an answer says, without the log messages that came with it."""
    if isinstance(response, BaseModel):
        return response.model_dump(exclude={"logs"})
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


def _path_info_response(row: tuple[Any, ...], ref_paths: list[str] | set[str]) -> Any:
    """Answer a `QueryPathInfo` from a `ValidPaths` row and its references.

    Both the synchronous reader and the pooled connection land here, so the
    two paths cannot diverge in what they build. The imports stay local:
    `pynixd.serde` cannot be imported at module level without a cycle.
    """
    from nix_daemon_protocol.content_address import ContentAddress
    from nix_daemon_protocol.nar_hash import NARHash
    from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
    from nix_daemon_protocol.signature import Signature
    from nix_daemon_protocol.wire_time import Time
    from pynixd.serde import QueryPathInfoResponse, StorePath

    _path, deriver, nar_hash, reg_time, nar_size, ultimate, sigs, ca = row

    sig_set: set[Signature] = set()
    if sigs:
        for s in sigs.split():
            sig_set.add(Signature(**Signature.from_str(s)))

    info = UnkeyedValidPathInfo(
        deriver=StorePath(path=deriver or ""),
        nar_hash=NARHash(hash=nar_hash),
        references={StorePath(path=r) for r in ref_paths},  # type: ignore[arg-type]
        registration_time=Time(ts=reg_time),
        nar_size=nar_size or 0,
        ultimate=bool(ultimate),
        sigs=sig_set,
        ca=ContentAddress(value=ca or ""),
    )
    return QueryPathInfoResponse.fast(valid=True, info=info)


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


class LocalDBStore(LocalStore):
    """LocalStore with SQLite database for fast-path query optimizations.

    Each executor method answers from SQLite when the database is open, and
    returns `None` when it is not. `DaemonStore.execute` treats a falsy result
    as "no fast path" and calls the wire, so a database pynixd cannot open
    costs correctness nothing.

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
        23: Owned("own_query_all_valid_paths", default=OpRoute.pynixd),
        26: Owned("own_query_path_info", default=OpRoute.pynixd),
        29: Owned("own_query_path_from_hash_part", default=OpRoute.pynixd),
        31: Owned("own_query_valid_paths", default=OpRoute.pynixd),
        33: Owned("own_query_valid_derivers", default=OpRoute.pynixd),
        41: Owned("own_query_derivation_output_map", default=OpRoute.pynixd),
        43: Owned("own_query_realisation", default=OpRoute.pynixd),
    }

    def __init__(self, spec: LocalSocketStoreSpec) -> None:
        super().__init__(spec)
        # `start` replaces it. A start that fails before then still reaches `close`.
        self.db = LocalStoreDB.inactive(self.layout)
        self.configured_routes = {_OP_CODES[name]: route for name, route in spec.op_routes.items()}
        for op, route in self.configured_routes.items():
            if route is OpRoute.daemon:
                continue
            owned = self.OWNED.get(op)
            if owned is None:
                raise ValueError(f"op_routes: pynixd has no answer to {_OP_NAMES[op]}; its route is daemon")
            if route is OpRoute.shadow and owned.writes:
                raise ValueError(f"op_routes: {_OP_NAMES[op]} writes, so it cannot run beside the daemon in shadow")
        self.routes = {op: self.configured_routes.get(op, owned.default) for op, owned in self.OWNED.items()}

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
        overlay = next((arg for arg in self.extra_args if "local-overlay" in arg), None)
        if overlay is not None:
            return (
                f"the daemon is started with {overlay!r}, and the SQLite fast paths read one "
                f"database. An overlay store keeps its lower paths in another one, so a fast "
                f"path would call a valid path invalid."
            )
        return None

    async def start(self, sync_paths: bool = True) -> None:
        """Initialise the SQLite database and start the daemon store."""
        await self.ensure_daemon()
        refusal = self._refuses_a_database()
        if refusal is not None:
            log.warning("local_store_db_refused", store_id=str(self.store_id), reason=refusal)
            self.db = LocalStoreDB.inactive(self.layout)
        else:
            self.db = await LocalStoreDB.open(self.layout)
        self._refuse_routes_without_a_database()
        self.db.start(self._background)
        await super().start(sync_paths=sync_paths)

    async def close(self) -> None:
        """Close the SQLite database and the daemon store."""
        await self.db.close()
        await super().close()

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
        owned = self.OWNED.get(request.op)
        route = self.routes.get(request.op, OpRoute.daemon)
        if owned is None or route is OpRoute.daemon:
            return await super().execute(request, client=client, suppress_last=suppress_last, skip_probe=skip_probe)
        if route is OpRoute.pynixd:
            if not skip_probe:
                await self.probe()
            answer = await self._own_answer(owned, request, client, suppress_last)
            if answer is not None:
                return answer
            metrics.OP_FALLTHROUGH.labels(store_id=self.store_id, op=request.name).inc()
            return await super().execute(request, client=client, suppress_last=suppress_last, skip_probe=True)
        theirs = await super().execute(request, client=client, suppress_last=suppress_last, skip_probe=skip_probe)
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
        if _value(ours) == _value(theirs):
            counter.labels(store_id=self.store_id, op=request.name, outcome="match").inc()
            return
        counter.labels(store_id=self.store_id, op=request.name, outcome="mismatch").inc()
        log.warning(
            "op_shadow_mismatch",
            op=request.name,
            request=_bounded(request.model_dump(exclude={"logs"})),
            pynixd=_bounded(_value(ours)),
            daemon=_bounded(_value(theirs)),
        )

    # ── pynixd's answers, by route ─────────────────────────────────

    async def own_is_valid_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """IsValidPath — fast-path via SQLite lookup.

        The synchronous reader of the session answers this when it has one.
        It skips the `aiosqlite` thread hop, which is most of the cost of the
        query: one build sends an `IsValidPath` for every derivation of its
        closure. A reader that cannot answer reports `None`, and the pooled
        connection answers instead.
        """
        if not self.db.active:
            return None

        path_str = str(request.path)

        from pynixd.serde import IsValidPathResponse

        reader = getattr(client, "sync_reader", None)
        if reader is not None:
            valid = reader.is_valid_path(path_str)
            if valid is not None:
                return IsValidPathResponse.fast(valid=valid)

        from .queries import IS_VALID_PATH

        async with self.db.execute(IS_VALID_PATH, (path_str,)) as cursor:
            row = await cursor.fetchone()
        if row is not None:
            return IsValidPathResponse.fast(valid=True)

        return IsValidPathResponse.fast(valid=False)

    async def own_query_path_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathInfo — fast-path via SQLite, with in-memory cache check.

        The synchronous reader of the session answers this when it has one,
        for the same reason as `is_valid_path`: two pooled queries are two
        thread hops, and a closure query sends thousands of these. A reader
        that cannot answer reports `None`, and the pooled connection answers
        instead. Both land in `_path_info_response`, so they build the same
        answer.
        """
        cached = self.get_path_info(request.path)
        if cached is not None:
            from pynixd.serde import QueryPathInfoResponse

            return QueryPathInfoResponse.fast(valid=True, info=cached.info)

        from pynixd.serde import QueryPathInfoResponse

        from .queries import QUERY_PATH_INFO_WITH_REFS

        reader = getattr(client, "sync_reader", None)
        if reader is not None:
            found = reader.query_path_info(str(request.path))
            if found is not None:
                row, ref_paths = found
                if row is None:
                    return QueryPathInfoResponse.fast(valid=False)
                return _path_info_response(row, ref_paths)

        async with self.db.execute(QUERY_PATH_INFO_WITH_REFS, (str(request.path),)) as cursor:
            rows = [tuple(r) for r in await cursor.fetchall()]
        if not rows:
            return QueryPathInfoResponse.fast(valid=False)

        return _path_info_response(rows[0][:8], {r[8] for r in rows if r[8] is not None})

    async def own_query_all_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryAllValidPaths — fast-path via SQLite."""

        from pynixd.serde import QueryAllValidPathsResponse, StorePath

        from .queries import QUERY_ALL_VALID_PATHS

        async with self.db.execute(QUERY_ALL_VALID_PATHS) as cursor:
            rows = await cursor.fetchall()
        paths: set[StorePath] = {StorePath(path=r[0]) for r in rows}
        return QueryAllValidPathsResponse(paths=paths)

    async def own_query_referrers(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryReferrers: the paths whose references name this one (`daemon.cc:397`)."""
        from nix_daemon_protocol.query_referrers import QueryReferrersResponse

        from .queries import QUERY_REFERRERS

        async with self.db.execute(QUERY_REFERRERS, (str(request.path),)) as cursor:
            rows = await cursor.fetchall()
        return QueryReferrersResponse.fast(paths={StorePath(path=row[0]) for row in rows})

    async def own_query_valid_derivers(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryValidDerivers: the valid derivations that name this path as an output (`daemon.cc:398`)."""
        from nix_daemon_protocol.query_valid_derivers import QueryValidDeriversResponse

        from .queries import QUERY_VALID_DERIVERS

        async with self.db.execute(QUERY_VALID_DERIVERS, (str(request.path),)) as cursor:
            rows = await cursor.fetchall()
        return QueryValidDeriversResponse.fast(paths={StorePath(path=row[1]) for row in rows})

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
        from nix_daemon_protocol.signature import Signature
        from nix_daemon_protocol.unkeyed_realisation import UnkeyedRealisation

        from .daemon import answer_in_both_shapes

        if "ca-derivations" not in (await self.trust_policy()).experimental_features:
            return None
        shaped = await self._request_in_the_shape_this_store_reads(request)
        if FEATURE_REALISATION_WITH_PATH in self.features:
            keyed = shaped.keyed_drv_output
            row = await self._realisation_row(keyed.drv_path.name, keyed.output_name)
        else:
            row = await self._realisation_row(shaped.drv_output.drv_hash, shaped.drv_output.output_name)
        if row is None:
            return QueryRealisationResponse()
        out_path = StorePath(path=row[0])
        # Nix orders a `std::set<Signature>` by key name, then signature.
        signatures = sorted((Signature(text) for text in (row[1] or "").split()), key=lambda s: (s.name, s.signature))
        if FEATURE_REALISATION_WITH_PATH in self.features:
            response = QueryRealisationResponse(
                present=1, realisation=UnkeyedRealisation(out_path=out_path, signatures=set(signatures))
            )
        else:
            response = QueryRealisationResponse(
                realisations=[Realisation(id=shaped.drv_output, out_path=out_path, signatures=signatures)]
            )
        return answer_in_both_shapes(response, request)

    async def _realisation_row(self, drv: str, output_name: str) -> tuple[str, str | None] | None:
        """`(output path, signatures)` of one realisation, or `None`.

        *drv* is the key of the table this store's Nix reads: the hash of the
        derivation (`sha256:<hex>`) without `realisation-with-path-not-hash`,
        and the base name of the derivation with it.
        """
        from nix_daemon_protocol.constants import FEATURE_REALISATION_WITH_PATH

        from .queries import QUERY_REALISATION_BY_HASH, QUERY_REALISATION_BY_PATH

        query = (
            QUERY_REALISATION_BY_PATH if FEATURE_REALISATION_WITH_PATH in self.features else QUERY_REALISATION_BY_HASH
        )
        async with self.db.execute(query, (drv, output_name)) as cursor:
            row = await cursor.fetchone()
        return None if row is None else (row[0], row[1])

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

        from ..drv_hash import output_hashes
        from .queries import QUERY_DERIVATION_OUTPUTS

        async with self.db.execute(QUERY_DERIVATION_OUTPUTS, (str(request.path),)) as cursor:
            rows = await cursor.fetchall()
        if not rows:
            return None
        items = {name: StorePath(path=path) for _drv, name, path in rows if name is not None}
        if "ca-derivations" not in (await self.trust_policy()).experimental_features:
            return QueryDerivationOutputMapResponse.fast(items=items)

        parsed = await self.read_derivation(request.path)
        if parsed is None:
            return None
        if FEATURE_REALISATION_WITH_PATH in self.features:
            keys = {output.output_name: request.path.name for output in parsed.outputs}
        else:
            hashes = await output_hashes(parsed, self.read_derivation)
            if hashes is None:
                return None
            keys = {name: f"sha256:{digest}" for name, digest in hashes.items()}
        for name, drv in keys.items():
            row = await self._realisation_row(drv, name)
            if row is not None:
                items[name] = StorePath(path=row[0])
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

        import json

        from pynixd.serde import QueryValidPathsResponse, StorePath

        from .queries import QUERY_VALID_PATHS

        paths_json = json.dumps([str(p) for p in request.paths])
        async with self.db.execute(QUERY_VALID_PATHS, (paths_json,)) as cursor:
            rows = await cursor.fetchall()

        paths: set[StorePath] = {StorePath(path=r[0]) for r in rows}
        return QueryValidPathsResponse(paths=paths)

    async def own_query_path_from_hash_part(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathFromHashPart: the valid path whose hash part this is (`local-store.cc:958`).

        A miss is the empty path. A hash part of another length goes to the
        daemon, which refuses it with Nix's own error.
        """
        from pynixd.serde import QueryPathFromHashPartResponse, StorePath

        from .queries import QUERY_PATH_FROM_HASH_PART

        if len(request.path) != _HASH_PART_LENGTH:
            return None
        prefix = f"{store_prefix()}{request.path}"
        upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        async with self.db.execute(QUERY_PATH_FROM_HASH_PART, (prefix, upper)) as cursor:
            row = await cursor.fetchone()
        return QueryPathFromHashPartResponse.fast(value=StorePath(path=row[0] if row else ""))

    async def query_closure(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryClosure — fast-path via SQLite recursive CTE."""

        import json

        from pynixd.serde import QueryClosureResponse, StorePath

        from .queries import QUERY_CLOSURE

        seeds_json = json.dumps([str(p) for p in request.paths])
        async with self.db.execute(QUERY_CLOSURE, (seeds_json,)) as cursor:
            rows = await cursor.fetchall()
        paths: set[StorePath] = {StorePath(path=row[0]) for row in rows}
        return QueryClosureResponse(paths=paths)

    async def query_closure_with_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryClosureWithInfo — fast-path via SQLite recursive CTE with full info."""

        if not request.paths:
            from pynixd.serde import QueryClosureWithInfoResponse

            return QueryClosureWithInfoResponse(infos=[])

        import json

        from nix_daemon_protocol.content_address import ContentAddress
        from nix_daemon_protocol.nar_hash import NARHash
        from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
        from nix_daemon_protocol.signature import Signature
        from nix_daemon_protocol.valid_path_info import ValidPathInfo
        from nix_daemon_protocol.wire_time import Time
        from pynixd.serde import QueryClosureWithInfoResponse, StorePath

        from .queries import QUERY_CLOSURE_WITH_INFO

        seeds_json = json.dumps([str(p) for p in request.paths])
        async with self.db.execute(QUERY_CLOSURE_WITH_INFO, (seeds_json,)) as cursor:
            rows = await cursor.fetchall()

        sorted_infos: list[Any] = []
        for path, deriver, nar_hash, reg_time, nar_size, ultimate, sigs, ca, refs_str in rows:
            sp = StorePath(path=path)
            references: set[StorePath] = {StorePath(path=r) for r in refs_str.split()} if refs_str else set()
            sig_set: set[Signature] = set()
            if sigs:
                for s in sigs.split():
                    sig_set.add(Signature(**Signature.from_str(s)))
            uinfo = UnkeyedValidPathInfo(
                deriver=StorePath(path=deriver or ""),
                nar_hash=NARHash(hash=nar_hash),
                references=references,
                registration_time=Time(ts=reg_time),
                nar_size=nar_size or 0,
                ultimate=bool(ultimate),
                sigs=sig_set,
                ca=ContentAddress(value=ca or ""),
            )
            sorted_infos.append(ValidPathInfo(path=sp, info=uinfo))

        return QueryClosureWithInfoResponse(infos=sorted_infos)

    async def query_path_infos(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathInfos — batch path info query via SQLite with per-path cache check."""

        if not request.paths:
            from pynixd.serde import QueryPathInfosResponse

            return QueryPathInfosResponse(infos=[])

        cached: dict[Any, Any] = {}
        uncached: list[Any] = []
        for path in request.paths:
            cached_info = self.get_path_info(path)
            if cached_info is not None:
                cached[path] = cached_info
            else:
                uncached.append(path)

        if not uncached:
            from pynixd.serde import QueryPathInfosResponse

            return QueryPathInfosResponse(infos=list(cached.values()))

        import json

        from nix_daemon_protocol.content_address import ContentAddress
        from nix_daemon_protocol.nar_hash import NARHash
        from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
        from nix_daemon_protocol.signature import Signature
        from nix_daemon_protocol.valid_path_info import ValidPathInfo
        from nix_daemon_protocol.wire_time import Time
        from pynixd.serde import QueryPathInfosResponse, StorePath

        from .queries import QUERY_PATH_INFOS_BATCH, QUERY_REFERENCES_BATCH

        paths_json = json.dumps([str(p) for p in uncached])
        async with self.db.execute(QUERY_PATH_INFOS_BATCH, (paths_json,)) as cursor:
            rows = await cursor.fetchall()
        async with self.db.execute(QUERY_REFERENCES_BATCH, (paths_json,)) as cursor:
            ref_rows = await cursor.fetchall()

        refs_map: dict[StorePath, set[StorePath]] = {}
        for referrer, reference in ref_rows:
            refs_map.setdefault(StorePath(path=referrer), set()).add(
                StorePath(path=reference),
            )

        infos: list[Any] = []
        for path, deriver, nar_hash, reg_time, nar_size, ultimate, sigs, ca in rows:
            sp = StorePath(path=path)
            sig_set: set[Signature] = set()
            if sigs:
                for s in sigs.split():
                    sig_set.add(Signature(**Signature.from_str(s)))
            uinfo = UnkeyedValidPathInfo(
                deriver=StorePath(path=deriver or ""),
                nar_hash=NARHash(hash=nar_hash),
                references=refs_map.get(sp, set()),
                registration_time=Time(ts=reg_time),
                nar_size=nar_size or 0,
                ultimate=bool(ultimate),
                sigs=sig_set,
                ca=ContentAddress(value=ca or ""),
            )
            infos.append(ValidPathInfo(path=sp, info=uinfo))

        return QueryPathInfosResponse(infos=[*cached.values(), *infos])

    async def query_derivation_output_map_batch(
        self, request: Any, client: Any = None, suppress_last: bool = False
    ) -> Any:
        """QueryDerivationOutputMapBatch — batch output map via SQLite, fallback to drv parse."""

        if not request.drv_paths:
            from pynixd.daemon_extensions.query_derivation_output_map_batch import DerivationOutputMapBatchResponse

            return DerivationOutputMapBatchResponse(outputs={})

        import json

        from pynixd.daemon_extensions.query_derivation_output_map_batch import DerivationOutputMapBatchResponse
        from pynixd.serde import StorePath

        from .queries import QUERY_DERIVATION_OUTPUT_MAP_BATCH

        paths_json = json.dumps([str(p) for p in request.drv_paths])
        async with self.db.execute(QUERY_DERIVATION_OUTPUT_MAP_BATCH, (paths_json,)) as cursor:
            rows = await cursor.fetchall()

        result: dict[StorePath, dict[str, StorePath]] = {}
        # `DerivationOutputs.path` is NOT NULL in Nix's schema.
        for drv_path, output_name, output_path in rows:
            result.setdefault(StorePath(path=drv_path), {})[output_name] = StorePath(path=output_path)

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
