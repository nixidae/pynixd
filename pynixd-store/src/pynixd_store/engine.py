"""pynixd's local store, apart from the daemon: Nix's `LocalStore` (`docs/design/store-engine.md`).

The engine owns the store's layout, its database, the settings it reads and
its temporary roots. Its methods take plain types: a store path, a
`ValidPathInfo`, a `Source`. It reads no wire request and holds no daemon
connection; what it needs of the daemon (its settings) and of the cache
above it (a path that changed) come in as callables.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import anyio.to_thread

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from . import add, build_log, register, roots
from .drv_parser import read_drv_file
from .hintfmt import magenta
from .liveness import unlink_stale_auto_roots, walk_roots
from .local_store_db import LocalStoreDB
from .queries import (
    IS_VALID_PATH,
    QUERY_ALL_VALID_PATHS,
    QUERY_CLOSURE,
    QUERY_CLOSURE_WITH_INFO,
    QUERY_DERIVATION_OUTPUT_MAP_BATCH,
    QUERY_DERIVATION_OUTPUTS,
    QUERY_PATH_FROM_HASH_PART,
    QUERY_PATH_INFO_WITH_REFS,
    QUERY_PATH_INFOS_BATCH,
    QUERY_REALISATION_BY_HASH,
    QUERY_REALISATION_BY_PATH,
    QUERY_REFERENCES_BATCH,
    QUERY_REFERRERS,
    QUERY_VALID_DERIVERS,
    QUERY_VALID_PATHS,
)
from .temp_roots import TempRoots

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping
    from typing import Any

    from .content_address import Method
    from .drv_parser import Derivation
    from .local_store_db import SyncReader
    from .source import Source
    from .store_layout import StoreLayout


def path_info(row: tuple[Any, ...], references: Iterable[str]) -> ValidPathInfo:
    """A `ValidPaths` row and its references, as `LocalStore::queryPathInfoInternal` reads them (`local-store.cc:667`)."""
    path, deriver, nar_hash, registration_time, nar_size, ultimate, sigs, ca = row
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
        ),
    )


class StoreEngine:
    """The operations of a local store that pynixd runs itself, with Nix's names."""

    def __init__(
        self,
        layout: StoreLayout,
        *,
        writes: bool,
        read_config: Callable[[], Awaitable[Mapping[str, Any]]],
        on_change: Callable[[StorePath], None],
        name: str,
    ) -> None:
        self.layout = layout
        #: Whether pynixd itself may change the store (`LocalSocketStoreSpec.pynixd_writes`).
        self.writes = writes
        # The owner replaces it at start. A start that fails before then still reaches `close`.
        self.db = LocalStoreDB.inactive(layout)
        self._read_config = read_config
        self._on_change = on_change
        self._name = name
        self._settings: add.ImportSettings | None = None

    async def settings(self) -> add.ImportSettings:
        """What `addToStore` reads of the store's Nix settings. Read once, as the daemon reads them."""
        if self._settings is None:
            self._settings = add.ImportSettings.from_nix_config(await self._read_config())
        return self._settings

    @property
    def writable(self) -> bool:
        return self.db.active and not self.db.read_only

    async def is_valid_path_uncached(self, path: str) -> bool:
        """`isValidPathUncached`: the table itself, never the path-info cache."""
        async with self.db.acquire_conn() as conn, conn.execute(IS_VALID_PATH, (path,)) as cursor:
            return await cursor.fetchone() is not None

    async def query_path_info(self, path: str, *, reader: SyncReader | None = None) -> ValidPathInfo | None:
        """`queryPathInfoUncached`: the info of a valid path, or `None`.

        A session's *reader* answers first when it can. It skips a thread hop
        per query, and a closure query sends thousands of these.
        """
        found = reader.query_path_info(path) if reader is not None else None
        if found is not None:
            row, references = found
            return None if row is None else path_info(row, references)
        async with self.db.execute(QUERY_PATH_INFO_WITH_REFS, (path,)) as cursor:
            rows = [tuple(r) for r in await cursor.fetchall()]
        if not rows:
            return None
        return path_info(rows[0][:8], {r[8] for r in rows if r[8] is not None})

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        """`Store::readDerivation`: the parsed `.drv` file, or `None` when it is not on disk."""
        return await read_drv_file(drv_path, self.layout)

    async def is_valid_path(self, path: str, *, reader: SyncReader | None = None) -> bool:
        """`isValidPathUncached`, through a session's *reader* when it can answer."""
        valid = reader.is_valid_path(path) if reader is not None else None
        if valid is not None:
            return valid
        async with self.db.execute(IS_VALID_PATH, (path,)) as cursor:
            return await cursor.fetchone() is not None

    async def _paths(self, query: str, params: tuple[Any, ...] = (), column: int = 0) -> set[StorePath]:
        async with self.db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
        return {StorePath(path=row[column]) for row in rows}

    async def query_all_valid_paths(self) -> set[StorePath]:
        """`LocalStore::queryAllValidPaths` (`local-store.cc:822`)."""
        return await self._paths(QUERY_ALL_VALID_PATHS)

    async def query_valid_paths(self, paths: Iterable[str]) -> set[StorePath]:
        """The valid ones of *paths*, without substitution (`Store::queryValidPaths`)."""
        return await self._paths(QUERY_VALID_PATHS, (json.dumps(list(paths)),))

    async def query_referrers(self, path: str) -> set[StorePath]:
        """`LocalStore::queryReferrers`: the paths whose references name *path*."""
        return await self._paths(QUERY_REFERRERS, (path,))

    async def query_valid_derivers(self, path: str) -> set[StorePath]:
        """`LocalStore::queryValidDerivers`: the valid derivations that name *path* as an output."""
        return await self._paths(QUERY_VALID_DERIVERS, (path,), column=1)

    async def query_path_from_hash_part(self, hash_part: str) -> StorePath | None:
        """`LocalStore::queryPathFromHashPart` (`local-store.cc:958`): the valid path with this hash part."""
        prefix = f"{self.layout.store_dir}/{hash_part}"
        upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        async with self.db.execute(QUERY_PATH_FROM_HASH_PART, (prefix, upper)) as cursor:
            row = await cursor.fetchone()
        return None if row is None else StorePath(path=row[0])

    async def query_closure(self, paths: Iterable[str]) -> set[StorePath]:
        """`computeFSClosure` of *paths* over the references."""
        return await self._paths(QUERY_CLOSURE, (json.dumps(list(paths)),))

    async def query_closure_with_info(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        """The closure of *paths*, each with its info."""
        async with self.db.execute(QUERY_CLOSURE_WITH_INFO, (json.dumps(list(paths)),)) as cursor:
            rows = await cursor.fetchall()
        return [path_info(row[:8], (row[8] or "").split()) for row in rows]

    async def query_path_infos(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        """The info of each valid one of *paths*."""
        paths_json = json.dumps(list(paths))
        async with self.db.execute(QUERY_PATH_INFOS_BATCH, (paths_json,)) as cursor:
            rows = await cursor.fetchall()
        async with self.db.execute(QUERY_REFERENCES_BATCH, (paths_json,)) as cursor:
            reference_rows = await cursor.fetchall()
        references: dict[str, set[str]] = {}
        for referrer, reference in reference_rows:
            references.setdefault(referrer, set()).add(reference)
        return [path_info(tuple(row), references.get(row[0], ())) for row in rows]

    async def query_realisation(
        self, drv: str, output_name: str, *, by_path: bool
    ) -> tuple[StorePath, list[Signature]] | None:
        """One realisation: its output path, and its signatures in Nix's order.

        *drv* keys the table this store's Nix reads: with *by_path*
        (`realisation-with-path-not-hash`) the derivation's base name, else
        the derivation's hash (`sha256:<hex>`).
        """
        query = QUERY_REALISATION_BY_PATH if by_path else QUERY_REALISATION_BY_HASH
        async with self.db.execute(query, (drv, output_name)) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        # Nix orders a `std::set<Signature>` by key name, then signature.
        signatures = sorted((Signature(text) for text in (row[1] or "").split()), key=lambda s: (s.name, s.signature))
        return StorePath(path=row[0]), signatures

    async def query_static_derivation_outputs(self, drv_path: str) -> dict[str, StorePath] | None:
        """The outputs of a derivation whose path is known statically, or `None` when the derivation is not valid."""
        async with self.db.execute(QUERY_DERIVATION_OUTPUTS, (drv_path,)) as cursor:
            rows = await cursor.fetchall()
        if not rows:
            return None
        return {name: StorePath(path=path) for _drv, name, path in rows if name is not None}

    async def query_derivation_output_maps(self, drv_paths: Iterable[str]) -> dict[StorePath, dict[str, StorePath]]:
        """The static outputs of each valid one of *drv_paths*."""
        async with self.db.execute(QUERY_DERIVATION_OUTPUT_MAP_BATCH, (json.dumps(list(drv_paths)),)) as cursor:
            rows = await cursor.fetchall()
        maps: dict[StorePath, dict[str, StorePath]] = {}
        # `DerivationOutputs.path` is NOT NULL in Nix's schema.
        for drv_path, output_name, output_path in rows:
            maps.setdefault(StorePath(path=drv_path), {})[output_name] = StorePath(path=output_path)
        return maps

    async def find_roots(self, *, tidy: bool) -> list[tuple[str, str]]:
        """`LocalStore::findRoots` (`gc.cc:319`): each `(link, target)`, ordered by target, then link.

        With *tidy*, the walk removes stale automatic roots and dead temporary
        roots files, as Nix does. The answer is the same either way.
        """
        state_dir, store_dir = self.layout.state_dir, str(self.layout.store_dir)

        def walk() -> tuple[dict[str, set[str]], dict[str, set[str]]]:
            if tidy:
                unlink_stale_auto_roots(state_dir, store_dir)
            return walk_roots(state_dir, store_dir, reap=tidy)

        checked, temporary = await anyio.to_thread.run_sync(walk)
        valid = await self.query_valid_paths(sorted(checked))
        entries = {(link, str(path)) for path in valid for link in checked[str(path)]}
        entries |= {(link, path) for path, links in temporary.items() for link in links}
        return sorted(entries, key=lambda entry: (entry[1], entry[0]))

    def invalidate_path_info_cache_for(self, path: StorePath | str) -> None:
        """`Store::invalidatePathInfoCacheFor`: the cache above the engine forgets *path*."""
        self._on_change(StorePath(path=str(path)))

    async def add_indirect_root(self, path: str) -> None:
        """`LocalStore::addIndirectRoot` of the client's path, as `daemon.cc` passes it through `absPath`."""
        await anyio.to_thread.run_sync(roots.add_indirect_root, self.layout.state_dir, roots.abs_path(path))

    async def add_perm_root(self, store_path: str, gc_root: str, *, hold: Callable[[str], Awaitable[None]]) -> str:
        """`IndirectRootStore::addPermRoot`: link *gc_root* to *store_path*, and answer the root it made.

        *hold* is the session's `addTempRoot`, which Nix takes before it links.
        """
        root = roots.abs_path(gc_root)
        roots.check_perm_root(root, self.layout.store_dir)
        await hold(store_path)
        await anyio.to_thread.run_sync(
            roots.add_perm_root, store_path, root, self.layout.store_dir, self.layout.state_dir
        )
        return root

    def temp_roots(self) -> TempRoots:
        """A roots file of its own, for an import no client session holds."""
        return TempRoots(self.layout.state_dir)

    def check_signatures(
        self, info: ValidPathInfo, settings: add.ImportSettings, overrides: Mapping[str, str], warnings: list[str]
    ) -> None:
        """`pathInfoIsUntrusted` and its refusal (`local-store.cc:1048`)."""
        add.check_import_signatures(self, info, settings, overrides, warnings)

    async def add_to_store(
        self, info: ValidPathInfo, source: Source, *, repair: bool, ignored_acls: frozenset[str] = add.IGNORED_ACLS
    ) -> None:
        """`LocalStore::addToStore` (`local-store.cc:1046`) after the signature check."""
        await add.add_nar(self, info, source, repair=repair, ignored_acls=ignored_acls)

    async def import_nar(self, info: ValidPathInfo, source: Source, *, dont_check_sigs: bool) -> None:
        """`addToStore` for pynixd's own callers, with its own temporary root."""
        await add.import_nar(self, info, source, dont_check_sigs=dont_check_sigs)

    async def add_to_store_from_dump(
        self,
        name: str,
        method: Method,
        algo: str,
        references: frozenset[str],
        source: Source,
        *,
        root: Callable[[str], Awaitable[None]],
        ignored_acls: frozenset[str] = add.IGNORED_ACLS,
    ) -> str:
        """`LocalStore::addToStoreFromDump` (`local-store.cc:1168`), without repair."""
        return await add.add_dump(self, name, method, algo, references, source, root=root, ignored_acls=ignored_acls)

    async def register_valid_paths(self, infos: Iterable[ValidPathInfo]) -> None:
        """Make *infos* valid in Nix's own tables, as `LocalStore::registerValidPaths` does.

        The files must already be in the store. `RegistrationError` carries
        Nix's reason for a path Nix would refuse.
        """
        if not self.writes:
            raise RuntimeError(f"store {self._name}: pynixd_writes is off, so pynixd registers no path")
        if not self.writable:
            raise RuntimeError(f"store {self._name}: pynixd cannot write the store database")
        infos = list(infos)
        registrations = [await register.prepare(info, self.layout) for info in infos]
        async with self.db.acquire_conn() as conn:
            await register.register_valid_paths(conn, registrations)
        for info in infos:
            self._on_change(info.path)

    async def add_signatures(self, path: str, sigs: Iterable[Signature]) -> None:
        """`LocalStore::addSignatures` (`local-store.cc:1583`): merge *sigs* into the path's signature set.

        NIX-DEFECT (#23): Nix reads the path with `queryPathInfoInternal`, which
        returns a null pointer for an invalid path (`local-store.cc:772`), and
        `addSignatures` dereferences it at `local-store.cc:1592`. The daemon
        worker segfaults and the connection dies with no error. pynixd raises
        `RegistrationError` instead; Lillecarl/nix holds the report.
        """
        if not self.writes:
            raise RuntimeError(f"store {self._name}: pynixd_writes is off, so pynixd signs no path")
        if not self.writable:
            raise RuntimeError(f"store {self._name}: pynixd cannot write the store database")
        async with self.db.acquire_conn() as conn:
            valid = await register.add_signatures(conn, path, sigs)
        if not valid:
            raise register.RegistrationError(
                f"cannot add signatures to '{magenta(path)}': it is not a valid store path"
            )
        self._on_change(StorePath(path=path))

    async def register_drv_output(
        self, drv: str, output_name: str, out_path: str, sigs: Iterable[Signature], *, by_path: bool
    ) -> None:
        """`LocalStore::registerDrvOutput` (`local-store.cc:664`): record one build-trace entry.

        *drv* keys the table this store's Nix reads: with *by_path* the
        derivation's base name in `BuildTraceV3`, else the derivation's hash in
        `Realisations`. An id already present with another output path raises,
        as Nix does. A realisation carries no path info, so nothing is forgotten.
        """
        if not self.writes:
            raise RuntimeError(f"store {self._name}: pynixd_writes is off, so pynixd registers no realisation")
        if not self.writable:
            raise RuntimeError(f"store {self._name}: pynixd cannot write the store database")
        async with self.db.acquire_conn() as conn:
            await register.register_drv_output(conn, drv, output_name, out_path, sigs, by_path=by_path)

    async def add_build_log(self, drv: str, log: bytes) -> None:
        """`LocalStore::addBuildLog` (`local-store.cc:1645`): record *drv*'s build log under `logDir`.

        *drv* is the derivation's base name, as the wire carries it. The write
        touches no database, so it reads no `writable`: like a root, it needs
        the file system, and the router gates the route.
        """
        await anyio.to_thread.run_sync(build_log.add_build_log, self.layout.log_dir, drv, log)
