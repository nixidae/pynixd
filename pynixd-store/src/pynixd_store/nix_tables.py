"""Nix's own tables in a local store's SQLite database: `ValidPaths`, `Refs`, `DerivationOutputs` and the realisations.

Each method answers with store paths and `ValidPathInfo`, never with a row or
a cursor. The SQL is in `sql/nix.py`, and the writes in `register.py`. pynixd's own tables
are `LocalStoreDB`'s.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from . import register

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .database import Params, Row
    from .local_store_db import LocalStoreDB
    from .sql import Codec


def path_info(row: Row, references: Iterable[str], codec: Codec) -> ValidPathInfo:
    """A `ValidPaths` row and its references, as `LocalStore::queryPathInfoInternal` reads them (`local-store.cc:667`)."""
    path, deriver, nar_hash, registration_time, nar_size, ultimate, sigs, ca, provenance = row
    return ValidPathInfo(
        path=StorePath(path=path),
        info=UnkeyedValidPathInfo(
            deriver=StorePath(path=deriver or ""),
            nar_hash=NARHash(hash=codec.nar_hash_hex(nar_hash)),
            references={StorePath(path=reference) for reference in references},
            registration_time=Time(ts=registration_time),
            nar_size=nar_size or 0,
            ultimate=bool(ultimate),
            sigs={Signature(**Signature.from_str(sig)) for sig in codec.strings(sigs)},
            ca=ContentAddress(value=ca or ""),
            provenance=provenance or "",
        ),
    )


class NixTables:
    """The queries and writes of Nix's `LocalStore` over its own tables, on the pool of *db*."""

    def __init__(self, db: LocalStoreDB) -> None:
        self._db = db

    async def is_valid_path(self, path: str) -> bool:
        return await self._db.fetchone(self._db.sql.nix.is_valid_path, (path,)) is not None

    async def path_info(self, path: str) -> ValidPathInfo | None:
        rows = await self._db.fetchall(self._db.sql.nix.path_info_with_refs, (path,))
        if not rows:
            return None
        return path_info(rows[0][:9], {r[9] for r in rows if r[9] is not None}, self._db.sql.codec)

    async def _paths(self, query: str, params: Params = (), column: int = 0) -> set[StorePath]:
        return {StorePath(path=row[column]) for row in await self._db.fetchall(query, params)}

    async def all_valid_paths(self) -> set[StorePath]:
        return await self._paths(self._db.sql.nix.all_valid_paths)

    async def valid_paths(self, paths: Iterable[str]) -> set[StorePath]:
        return await self._paths(self._db.sql.nix.valid_paths, (self._db.sql.codec.path_set(paths),))

    async def referrers(self, path: str) -> set[StorePath]:
        return await self._paths(self._db.sql.nix.referrers, (path,))

    async def valid_derivers(self, path: str) -> set[StorePath]:
        return await self._paths(self._db.sql.nix.valid_derivers, (path,), column=1)

    async def closure(self, paths: Iterable[str]) -> set[StorePath]:
        row = await self._db.fetchone(self._db.sql.nix.closure, (self._db.sql.codec.path_set(paths),))
        return set() if row is None else {StorePath(path=path) for path in self._db.sql.codec.strings(row[0])}

    async def path_in_range(self, lower: str, upper: str) -> StorePath | None:
        """The first valid path *p* with `lower <= p < upper`: the lookup of a hash part."""
        row = await self._db.fetchone(self._db.sql.nix.path_from_hash_part, (lower, upper))
        return None if row is None else StorePath(path=row[0])

    async def path_from_nar_hash(self, nar_hash: bytes) -> StorePath | None:
        """A valid path whose NAR has the SHA-256 digest *nar_hash*."""
        row = await self._db.fetchone(self._db.sql.nix.path_from_nar_hash, (self._db.sql.codec.nar_hash(nar_hash),))
        return None if row is None else StorePath(path=row[0])

    async def closure_with_info(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        rows = await self._db.fetchall(self._db.sql.nix.closure_with_info, (self._db.sql.codec.path_set(paths),))
        codec = self._db.sql.codec
        return [path_info(row[:9], codec.strings(row[9]), codec) for row in rows]

    async def path_infos(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        path_set = self._db.sql.codec.path_set(list(paths))
        rows = await self._db.fetchall(self._db.sql.nix.path_infos_batch, (path_set,))
        references: dict[str, set[str]] = {}
        for referrer, reference in await self._db.fetchall(self._db.sql.nix.references_batch, (path_set,)):
            references.setdefault(referrer, set()).add(reference)
        return [path_info(row, references.get(row[0], ()), self._db.sql.codec) for row in rows]

    async def realisation(self, drv: str, output_name: str, *, by_path: bool) -> tuple[StorePath, list[str]] | None:
        """One realisation's output path and its signatures as Nix stored them; *drv* keys the table *by_path* picks."""
        query = self._db.sql.nix.realisation_by_path if by_path else self._db.sql.nix.realisation_by_hash
        row = await self._db.fetchone(query, (drv, output_name))
        if row is None:
            return None
        return StorePath(path=row[0]), self._db.sql.codec.strings(row[1])

    async def derivation_outputs(self, drv_path: str) -> dict[str, StorePath] | None:
        """The static outputs of a derivation, or `None` when the derivation is not valid."""
        rows = await self._db.fetchall(self._db.sql.nix.derivation_outputs, (drv_path,))
        if not rows:
            return None
        return {name: StorePath(path=path) for _drv, name, path in rows if name is not None}

    async def derivation_output_maps(self, drv_paths: Iterable[str]) -> dict[StorePath, dict[str, StorePath]]:
        rows = await self._db.fetchall(
            self._db.sql.nix.derivation_output_map_batch, (self._db.sql.codec.path_set(drv_paths),)
        )
        maps: dict[StorePath, dict[str, StorePath]] = {}
        # `DerivationOutputs.path` is NOT NULL in Nix's schema.
        for drv_path, output_name, output_path in rows:
            maps.setdefault(StorePath(path=drv_path), {})[output_name] = StorePath(path=output_path)
        return maps

    # Each write is one transaction of its own, in `register.py`. A caller never holds a connection.

    async def register_valid_paths(self, registrations: list[register.Registration]) -> None:
        async with self._db.transaction() as session:
            await register.register_valid_paths(session, registrations)

    async def add_signatures(self, path: str, sigs: Iterable[Signature]) -> bool:
        """Merge *sigs* into the path's signatures; `False` when the path is not valid."""
        async with self._db.transaction() as session:
            return await register.add_signatures(session, path, sigs)

    async def register_drv_output(
        self, drv: str, output_name: str, out_path: str, sigs: Iterable[Signature], *, by_path: bool
    ) -> None:
        async with self._db.transaction() as session:
            await register.register_drv_output(session, drv, output_name, out_path, sigs, by_path=by_path)

    async def invalidate_path(self, path: str) -> None:
        async with self._db.transaction() as session:
            await register.invalidate_path(session, path)

    async def update_path_hashes(self, path: str, *, nar_hash: bytes, nar_size: int) -> None:
        async with self._db.transaction() as session:
            await register.update_path_hashes(session, path, nar_hash=nar_hash, nar_size=nar_size)
