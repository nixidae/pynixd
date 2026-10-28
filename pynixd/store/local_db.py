"""LocalDBStore — LocalStore with SQLite database for fast-path queries."""

from __future__ import annotations

import functools
import types
from enum import Enum
from typing import Any, Union, get_args, get_origin

import structlog
from pydantic import BaseModel

from nix_daemon_protocol.store_dir import store_prefix

from ..local_store_db import LocalStoreDB
from ..serde import StorePath
from .local_daemon import LocalStore

log = structlog.get_logger(__name__)

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


def _path_info_response(row: tuple, ref_paths: list[str] | set[str]) -> Any:
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

    sig_set: set = set()
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
    names = _path_field_names(type(request))
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

    db: LocalStoreDB

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
        await super().start(sync_paths=sync_paths)

    async def close(self) -> None:
        """Close the SQLite database and the daemon store."""
        await self.db.close()
        await super().close()

    async def execute(self, request, client=None, suppress_last=False, skip_probe=False, mark=True):  # type: ignore[no-untyped-def] -- the parent is untyped
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
        """
        if mark:
            self.db.mark_paths(referenced_paths(request))
        return await super().execute(request, client=client, suppress_last=suppress_last, skip_probe=skip_probe)

    # ── Fast-path overrides ────────────────────────────────────────

    async def is_valid_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
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

    async def query_path_info(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
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

    async def query_all_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryAllValidPaths — fast-path via SQLite."""

        from pynixd.serde import QueryAllValidPathsResponse, StorePath

        from .queries import QUERY_ALL_VALID_PATHS

        async with self.db.execute(QUERY_ALL_VALID_PATHS) as cursor:
            rows = await cursor.fetchall()
        paths: set = {StorePath(path=r[0]) for r in rows}  # type: ignore[arg-type]
        return QueryAllValidPathsResponse(paths=paths)

    async def query_valid_paths(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
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

        paths: set = {StorePath(path=r[0]) for r in rows}  # type: ignore[arg-type]
        return QueryValidPathsResponse(paths=paths)

    async def query_path_from_hash_part(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryPathFromHashPart — fast-path via SQLite."""

        from pynixd.serde import QueryPathFromHashPartResponse, StorePath

        from .queries import QUERY_PATH_FROM_HASH_PART

        prefix = f"{store_prefix()}{request.path}"
        upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        async with self.db.execute(QUERY_PATH_FROM_HASH_PART, (prefix, upper)) as cursor:
            row = await cursor.fetchone()
        if row:
            return QueryPathFromHashPartResponse(value=StorePath(path=row[0]))

        return None  # fall through

    async def query_closure(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        """QueryClosure — fast-path via SQLite recursive CTE."""

        import json

        from pynixd.serde import QueryClosureResponse, StorePath

        from .queries import QUERY_CLOSURE

        seeds_json = json.dumps([str(p) for p in request.paths])
        async with self.db.execute(QUERY_CLOSURE, (seeds_json,)) as cursor:
            rows = await cursor.fetchall()
        paths: set = {StorePath(path=row[0]) for row in rows}  # type: ignore[arg-type]
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

        sorted_infos: list = []
        for path, deriver, nar_hash, reg_time, nar_size, ultimate, sigs, ca, refs_str in rows:
            sp = StorePath(path=path)
            references: set = {StorePath(path=r) for r in refs_str.split()} if refs_str else set()  # type: ignore[arg-type]
            sig_set: set = set()
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

        cached: dict = {}
        uncached: list = []
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

        refs_map: dict = {}
        for referrer, reference in ref_rows:
            refs_map.setdefault(StorePath(path=referrer), set()).add(  # type: ignore[arg-type]
                StorePath(path=reference),
            )

        infos: list = []
        for path, deriver, nar_hash, reg_time, nar_size, ultimate, sigs, ca in rows:
            sp = StorePath(path=path)
            sig_set: set = set()
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

        result: dict = {}
        for drv_path, output_name, output_path in rows:
            sp = StorePath(path=drv_path)
            val: StorePath | None = StorePath(path=output_path) if output_path else None
            result.setdefault(sp, {})[output_name] = val

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
