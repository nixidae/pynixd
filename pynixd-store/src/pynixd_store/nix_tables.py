"""The reads of Nix's own tables in a local store's SQLite database: `ValidPaths`, `Refs`, `DerivationOutputs` and the realisations.

Each method answers with store paths and `ValidPathInfo`, never with a row or
a cursor. The SQL is in `queries.py`. pynixd's own tables are `LocalStoreDB`'s.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from .queries import (
    IS_VALID_PATH,
    QUERY_ALL_VALID_PATHS,
    QUERY_CLOSURE,
    QUERY_DERIVATION_OUTPUT_MAP_BATCH,
    QUERY_DERIVATION_OUTPUTS,
    QUERY_PATH_FROM_HASH_PART,
    QUERY_PATH_FROM_NAR_HASH,
    QUERY_REALISATION_BY_HASH,
    QUERY_REALISATION_BY_PATH,
    QUERY_REFERENCES_BATCH,
    QUERY_REFERRERS,
    QUERY_VALID_DERIVERS,
    QUERY_VALID_PATHS,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from typing import Any

    from .local_store_db import LocalStoreDB


def path_info(row: tuple[Any, ...], references: Iterable[str]) -> ValidPathInfo:
    """A `ValidPaths` row and its references, as `LocalStore::queryPathInfoInternal` reads them (`local-store.cc:667`)."""
    path, deriver, nar_hash, registration_time, nar_size, ultimate, sigs, ca, provenance = row
    return ValidPathInfo(
        path=StorePath(path=path),
        info=UnkeyedValidPathInfo(
            deriver=StorePath(path=deriver or ""),
            nar_hash=NARHash(hash=nar_hash),
            references={StorePath(path=reference) for reference in references},
            registration_time=Time(ts=registration_time),
            nar_size=nar_size or 0,
            ultimate=bool(ultimate),
            sigs={Signature(**Signature.from_str(sig)) for sig in (sigs or "").split()},
            ca=ContentAddress(value=ca or ""),
            provenance=provenance or "",
        ),
    )


class NixTables:
    """The queries of Nix's `LocalStore` over its own tables, on the pool of *db*."""

    def __init__(self, db: LocalStoreDB) -> None:
        self._db = db

    async def is_valid_path(self, path: str) -> bool:
        async with self._db.execute(IS_VALID_PATH, (path,)) as cursor:
            return await cursor.fetchone() is not None

    async def path_info(self, path: str) -> ValidPathInfo | None:
        async with self._db.execute(self._db.statements.path_info_with_refs, (path,)) as cursor:
            rows = [tuple(r) for r in await cursor.fetchall()]
        if not rows:
            return None
        return path_info(rows[0][:9], {r[9] for r in rows if r[9] is not None})

    async def _paths(self, query: str, params: tuple[Any, ...] = (), column: int = 0) -> set[StorePath]:
        async with self._db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return {StorePath(path=row[column]) for row in rows}

    async def all_valid_paths(self) -> set[StorePath]:
        return await self._paths(QUERY_ALL_VALID_PATHS)

    async def valid_paths(self, paths: Iterable[str]) -> set[StorePath]:
        return await self._paths(QUERY_VALID_PATHS, (json.dumps(list(paths)),))

    async def referrers(self, path: str) -> set[StorePath]:
        return await self._paths(QUERY_REFERRERS, (path,))

    async def valid_derivers(self, path: str) -> set[StorePath]:
        return await self._paths(QUERY_VALID_DERIVERS, (path,), column=1)

    async def closure(self, paths: Iterable[str]) -> set[StorePath]:
        return await self._paths(QUERY_CLOSURE, (json.dumps(list(paths)),))

    async def path_in_range(self, lower: str, upper: str) -> StorePath | None:
        """The first valid path *p* with `lower <= p < upper`: the lookup of a hash part."""
        async with self._db.execute(QUERY_PATH_FROM_HASH_PART, (lower, upper)) as cursor:
            row = await cursor.fetchone()
        return None if row is None else StorePath(path=row[0])

    async def path_from_nar_hash(self, nar_hash: str) -> StorePath | None:
        """A valid path whose `ValidPaths.hash` is *nar_hash*, as Nix writes it (`sha256:<base16>`)."""
        async with self._db.execute(QUERY_PATH_FROM_NAR_HASH, (nar_hash,)) as cursor:
            row = await cursor.fetchone()
        return None if row is None else StorePath(path=row[0])

    async def closure_with_info(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        async with self._db.execute(self._db.statements.closure_with_info, (json.dumps(list(paths)),)) as cursor:
            rows = await cursor.fetchall()
        return [path_info(row[:9], (row[9] or "").split()) for row in rows]

    async def path_infos(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        paths_json = json.dumps(list(paths))
        async with self._db.execute(self._db.statements.path_infos_batch, (paths_json,)) as cursor:
            rows = await cursor.fetchall()
        async with self._db.execute(QUERY_REFERENCES_BATCH, (paths_json,)) as cursor:
            reference_rows = await cursor.fetchall()
        references: dict[str, set[str]] = {}
        for referrer, reference in reference_rows:
            references.setdefault(referrer, set()).add(reference)
        return [path_info(tuple(row), references.get(row[0], ())) for row in rows]

    async def realisation(self, drv: str, output_name: str, *, by_path: bool) -> tuple[StorePath, list[str]] | None:
        """One realisation's output path and its signatures as Nix stored them; *drv* keys the table *by_path* picks."""
        query = QUERY_REALISATION_BY_PATH if by_path else QUERY_REALISATION_BY_HASH
        async with self._db.execute(query, (drv, output_name)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return StorePath(path=row[0]), (row[1] or "").split()

    async def derivation_outputs(self, drv_path: str) -> dict[str, StorePath] | None:
        """The static outputs of a derivation, or `None` when the derivation is not valid."""
        async with self._db.execute(QUERY_DERIVATION_OUTPUTS, (drv_path,)) as cursor:
            rows = await cursor.fetchall()
        if not rows:
            return None
        return {name: StorePath(path=path) for _drv, name, path in rows if name is not None}

    async def derivation_output_maps(self, drv_paths: Iterable[str]) -> dict[StorePath, dict[str, StorePath]]:
        async with self._db.execute(QUERY_DERIVATION_OUTPUT_MAP_BATCH, (json.dumps(list(drv_paths)),)) as cursor:
            rows = await cursor.fetchall()
        maps: dict[StorePath, dict[str, StorePath]] = {}
        # `DerivationOutputs.path` is NOT NULL in Nix's schema.
        for drv_path, output_name, output_path in rows:
            maps.setdefault(StorePath(path=drv_path), {})[output_name] = StorePath(path=output_path)
        return maps
