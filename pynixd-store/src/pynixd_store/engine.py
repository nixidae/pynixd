"""pynixd's local store, apart from the daemon: Nix's `LocalStore` (`docs/design/store-engine.md`).

The engine owns the store's layout, its database, the settings it reads and
its temporary roots. Its methods take plain types: a store path, a
`ValidPathInfo`, a `Source`. It reads no wire request and holds no daemon
connection; what it needs of the daemon (its settings) and of the cache
above it (a path that changed) come in as callables.
"""

from __future__ import annotations

import json
import os
from functools import partial
from typing import TYPE_CHECKING

import anyio.to_thread

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.protocol import GCAction, Verbosity
from nix_daemon_protocol.signature import Signature
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from . import add, build_log, collect, optimise, register, roots, verify
from .active_builds import ActiveBuilds
from .collector import Collection
from .content_address import parse_any_sha256
from .drv_parser import read_drv_file
from .hintfmt import WARNING_PREFIX, magenta, show_error
from .liveness import unlink_stale_auto_roots, walk_roots
from .local_store_db import LocalStoreDB
from .nix_config import nix_at_least
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
from .temp_roots import GC_SOCKET_PATH, TempRoots
from .utils import nix32_encode

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping
    from pathlib import Path
    from typing import Any

    from .content_address import Method
    from .drv_parser import Derivation
    from .local_store_db import SyncReader
    from .source import Source
    from .store_layout import StoreLayout

    Report = Callable[[Verbosity, str], Awaitable[None]]
    Repair = Callable[[str], Awaitable[None]]


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


def hashes_self_reference_positions(nix_version: str) -> bool:
    """Whether a Nix of *nix_version* hashes the offset of each self-reference: from 2.35 (`add.HashModulo`).

    A version that does not parse counts as a new one.
    """
    return nix_at_least(nix_version, 2, 35)


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
        self._config: Mapping[str, Any] | None = None
        # The owner replaces it at start. A start that fails before then still reaches `close`.
        self.db = LocalStoreDB.inactive(layout)
        self._read_config = read_config
        self._on_change = on_change
        self._name = name
        self._settings: add.ImportSettings | None = None
        #: Whether the store's Nix hashes the offset of each self-reference (`add.HashModulo`); not Nix 2.34.
        self.hashes_self_reference_positions = True
        #: The builds of pynixd's own builder that run now (`QueryActiveBuilds`, op 48).
        self.active_builds = ActiveBuilds()

    @property
    def db(self) -> LocalStoreDB:
        """The store's database. Settings read before it opens reach it when it is set: a store with no daemon reads them first."""
        return self._db

    @db.setter
    def db(self, db: LocalStoreDB) -> None:
        self._db = db
        if self._config is not None:
            self._apply_db_settings(self._config)

    async def nix_config(self) -> Mapping[str, Any]:
        """The store's Nix settings, `nix config show --json`. Read once, as the daemon reads them."""
        if self._config is None:
            self._config = await self._read_config()
            self._apply_db_settings(self._config)
        return self._config

    def _apply_db_settings(self, config: Mapping[str, Any]) -> None:
        if "fsync-metadata" in config:
            self.db.synchronous = "NORMAL" if config["fsync-metadata"]["value"] else "OFF"

    async def settings(self) -> add.ImportSettings:
        """What `addToStore` reads of the store's Nix settings."""
        if self._settings is None:
            self._settings = add.ImportSettings.from_nix_config(await self.nix_config())
        return self._settings

    async def gc_settings(self, overrides: Mapping[str, str]) -> collect.GCSettings:
        """What `collectGarbage` reads of the settings, the session's overrides applied. Read per call: a collection is rare."""
        config = await self._read_config()
        self._apply_db_settings(config)
        return collect.GCSettings.from_nix_config(config, overrides)

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
        async with self.db.execute(self.db.statements.path_info_with_refs, (path,)) as cursor:
            rows = [tuple(r) for r in await cursor.fetchall()]
        if not rows:
            return None
        return path_info(rows[0][:9], {r[9] for r in rows if r[9] is not None})

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

    async def query_gc_referrers(self, path: str) -> set[StorePath]:
        """`queryGCReferrers`, and the referrers `invalidatePathChecked` reads: this store's own database alone.

        An overlay store's `query_referrers` adds the lower store's; this never does (`local-overlay-store.cc:167`).
        """
        return await StoreEngine.query_referrers(self, path)

    async def delete_store_path(self, base_name: str) -> int:
        """`LocalStore::deleteStorePath` (`local-store.cc:408`): delete one name of the store directory; the bytes freed."""
        return await anyio.to_thread.run_sync(collect.delete_path, self.layout.real_store_dir / base_name)

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

    async def query_path_from_nar_hash(self, nar_hash: str) -> StorePath | None:
        """A valid path whose NAR has this base-16 SHA-256 hash. Nix writes `ValidPaths.hash` as `sha256:<base16>`."""
        async with self.db.execute(QUERY_PATH_FROM_NAR_HASH, (f"sha256:{nar_hash}",)) as cursor:
            row = await cursor.fetchone()
        return None if row is None else StorePath(path=row[0])

    async def query_closure(self, paths: Iterable[str]) -> set[StorePath]:
        """`computeFSClosure` of *paths* over the references."""
        return await self._paths(QUERY_CLOSURE, (json.dumps(list(paths)),))

    async def query_closure_with_info(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        """The closure of *paths*, each with its info."""
        async with self.db.execute(self.db.statements.closure_with_info, (json.dumps(list(paths)),)) as cursor:
            rows = await cursor.fetchall()
        return [path_info(row[:9], (row[9] or "").split()) for row in rows]

    async def query_path_infos(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        """The info of each valid one of *paths*."""
        paths_json = json.dumps(list(paths))
        async with self.db.execute(self.db.statements.path_infos_batch, (paths_json,)) as cursor:
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
        self,
        info: ValidPathInfo,
        source: Source,
        *,
        repair: bool,
        ignored_acls: frozenset[str] = add.IGNORED_ACLS,
        auto_optimise: bool | None = None,
        fsync: bool | None = None,
        report: Report | None = None,
    ) -> None:
        """`LocalStore::addToStore` (`local-store.cc:1046`) after the signature check.

        *auto_optimise* and *fsync* are the session's overrides; `None` reads the store's setting. *report* gets
        each warning Nix prints while it registers the path.
        """
        await add.add_nar(
            self,
            info,
            source,
            repair=repair,
            ignored_acls=ignored_acls,
            auto_optimise=auto_optimise,
            fsync=fsync,
            report=report,
        )

    async def import_nar(
        self, info: ValidPathInfo, source: Source, *, dont_check_sigs: bool, repair: bool = False
    ) -> None:
        """`addToStore` for pynixd's own callers, with its own temporary root."""
        await add.import_nar(self, info, source, dont_check_sigs=dont_check_sigs, repair=repair)

    async def add_to_store_from_dump(
        self,
        name: str,
        method: Method,
        algo: str,
        references: frozenset[str],
        source: Source,
        *,
        root: Callable[[str], Awaitable[None]],
        repair: bool = False,
        provenance: str = "",
        ignored_acls: frozenset[str] = add.IGNORED_ACLS,
        scan: bool = False,
        auto_optimise: bool | None = None,
        fsync: bool | None = None,
        report: Report | None = None,
    ) -> str:
        """`LocalStore::addToStoreFromDump` (`local-store.cc:1151`); *scan* is `filterReferences`."""
        return await add.add_dump(
            self,
            name,
            method,
            algo,
            references,
            source,
            root=root,
            repair=repair,
            provenance=provenance,
            ignored_acls=ignored_acls,
            scan=scan,
            auto_optimise=auto_optimise,
            fsync=fsync,
            report=report,
        )

    async def register_valid_paths(self, infos: Iterable[ValidPathInfo], report: Report | None = None) -> None:
        """Make *infos* valid in Nix's own tables, as `LocalStore::registerValidPaths` does.

        The files must already be in the store. `RegistrationError` carries
        Nix's reason for a path Nix would refuse. *report* gets each warning of
        `checkInvariants`, before the write, where Nix prints it.
        """
        if not self.writes:
            raise RuntimeError(f"store {self._name}: pynixd_writes is off, so pynixd registers no path")
        if not self.writable:
            raise RuntimeError(f"store {self._name}: pynixd cannot write the store database")
        infos = list(infos)
        settings = await self.settings()
        if settings.sync_before_registering:
            # `registerValidPaths` (`local-store.cc:964`): the files reach the disk before SQLite says they are valid.
            await anyio.to_thread.run_sync(os.sync)
        features = settings.experimental_features
        registrations = [await register.prepare(info, self.layout, features) for info in infos]
        if report is not None:
            for registration in registrations:
                for warning in registration.warnings:
                    await report(Verbosity.WARN, warning)
        async with self.db.acquire_conn() as conn:
            await register.register_valid_paths(conn, registrations, self.db.statements)
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

    async def optimise_store(
        self, *, hold: Callable[[str], Awaitable[None]], report: Report | None = None
    ) -> optimise.OptimiseStats:
        """`LocalStore::optimiseStore` (`optimise-store.cc`): hard-link identical store files into `.links`.

        *hold* is the session's `addTempRoot`. Nix takes a temporary root on
        each path before it reads it, so a concurrent collector does not remove
        a path mid-walk. The daemon's collector (op 20) reads the same temproots
        files, so the hold protects against it too. *report* gets the summary
        line Nix prints (`optimise-store.cc:328`).
        """
        if not self.writes:
            raise RuntimeError(f"store {self._name}: pynixd_writes is off, so pynixd optimises no store")
        if not self.writable:
            raise RuntimeError(f"store {self._name}: pynixd cannot write the store")
        real_store_dir, links_dir = self.layout.real_store_dir, self.layout.links_dir
        # `LocalStore`'s constructor makes `.links`; a bare chroot store has none yet.
        await anyio.to_thread.run_sync(lambda: links_dir.mkdir(parents=True, exist_ok=True))
        stats = optimise.OptimiseStats()
        inode_hash = await anyio.to_thread.run_sync(optimise.load_inode_hash, links_dir)
        for path in await self.query_all_valid_paths():
            await hold(path.path)
            if not await self.is_valid_path_uncached(path.path):
                continue  # a collector took the path before the hold landed
            await anyio.to_thread.run_sync(
                partial(
                    optimise.optimise_path,
                    real_store_dir / path.name,
                    inode_hash,
                    real_store_dir=real_store_dir,
                    links_dir=links_dir,
                    stats=stats,
                )
            )
        if report is not None:
            await report(
                Verbosity.INFO,
                f"{optimise.render_size(stats.bytes_freed)} freed by hard-linking {stats.files_linked} files",
            )
        return stats

    async def contents_good(self, path: str) -> bool:
        """`Worker::pathContentsGood` (`worker.cc:553`) without its cache and its lines.

        A path whose files are gone answers `False`, as the null accessor does there. A null `narHash` is good.
        """
        info = await self.query_path_info(path)
        if info is None:
            return False
        real = self.layout.real_store_dir / path.rsplit("/", 1)[-1]
        if not await anyio.to_thread.run_sync(os.path.lexists, real):
            return False
        expected = parse_any_sha256(str(info.info.nar_hash))
        if expected == bytes(32):
            return True
        current, _ = await anyio.to_thread.run_sync(verify.nar_hash_and_size, real)
        return bytes.fromhex(current) == expected

    async def verify_store(self, *, check_contents: bool, report: Report, repair: Repair | None = None) -> bool:
        """`LocalStore::verifyStore` (`local-store.cc:1380`): cross-check the database and the store directory.

        It holds the big garbage-collector lock for reading, so the collector
        cannot remove a path mid-walk. A valid path the directory lost is
        removed from the database when its referrers can go too. With
        *check_contents* each `.links` entry and each valid path is re-hashed.
        It returns whether it found an error. *report* gets each line Nix
        prints, at Nix's level, in Nix's order.

        It writes without *check_contents* (it invalidates a lost path), so it
        needs a writable store. *repair* is `Store::repairPath`, which raises
        `RepairError`; with it, a modified link goes, and a lost or modified
        path is repaired.
        """
        if not self.writes:
            raise RuntimeError(f"store {self._name}: pynixd_writes is off, so pynixd verifies no store")
        if not self.writable:
            raise RuntimeError(f"store {self._name}: pynixd cannot write the store database")
        real_store_dir = self.layout.real_store_dir
        await report(Verbosity.INFO, "reading the Nix store...")
        gc_lock = await anyio.to_thread.run_sync(verify.open_gc_lock, self.layout.state_dir)
        try:
            if not await anyio.to_thread.run_sync(verify.try_gc_lock_shared, gc_lock):
                await report(Verbosity.INFO, "waiting for the big garbage collector lock...")
                await anyio.to_thread.run_sync(verify.wait_gc_lock_shared, gc_lock)
            in_dir = await anyio.to_thread.run_sync(verify.scan_store_dir, real_store_dir)
            await report(Verbosity.INFO, "checking path existence...")
            done: set[str] = set()
            valid: set[str] = set()
            errors = False
            for path in await self.query_all_valid_paths():
                errors = await self._verify_path(path, in_dir, done, valid, report, repair) or errors
            if check_contents:
                errors = await self._check_link_hashes(report, repair=repair is not None) or errors
                errors = await self._check_store_hashes(real_store_dir, valid, report, repair) or errors
            return errors
        finally:
            await anyio.to_thread.run_sync(verify.release_gc_lock, gc_lock)

    async def collect_garbage(
        self, options: collect.GCOptions, *, settings: collect.GCSettings, report: Report
    ) -> collect.GCResults:
        """`LocalStore::collectGarbage` (`gc.cc:356`): the lock, the roots socket, then the walk.

        A process that adds a temporary root while the lock is held gives it
        to the socket, pynixd's own sessions included, so the socket serves
        for the whole walk. *report* gets each line Nix prints.
        """
        if not self.writes:
            raise RuntimeError(f"store {self._name}: pynixd_writes is off, so pynixd collects no garbage")
        if not self.writable:
            raise RuntimeError(f"store {self._name}: pynixd cannot write the store database")
        if options.paths is not None and not options.paths:
            return collect.GCResults()
        state_dir = self.layout.state_dir
        if options.action in {GCAction.DELETE_DEAD, GCAction.DELETE_SPECIFIC}:
            await anyio.to_thread.run_sync(collect.delete_path, state_dir / "db" / "reserved")
        gc_lock = await anyio.to_thread.run_sync(verify.open_gc_lock, state_dir)
        try:
            if not await anyio.to_thread.run_sync(collect.try_gc_lock_exclusive, gc_lock):
                await report(Verbosity.INFO, "waiting for the big garbage collector lock...")
                await anyio.to_thread.run_sync(collect.wait_gc_lock_exclusive, gc_lock)
            server = collect.RootsServer(str(self.layout.store_dir), report)
            outcome: collect.GCResults | Exception
            async with anyio.create_task_group() as tg:
                await tg.start(server.serve, state_dir / GC_SOCKET_PATH)
                try:
                    outcome = await Collection(self, options, settings, server, report).run()
                # Raised outside the group, so a caller sees Nix's error and not a group around it.
                except Exception as exc:  # noqa: BLE001 -- re-raised below
                    outcome = exc
                tg.cancel_scope.cancel()
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        finally:
            await anyio.to_thread.run_sync(verify.release_gc_lock, gc_lock)

    async def _verify_path(
        self,
        path: StorePath,
        in_dir: set[str],
        done: set[str],
        valid: set[str],
        report: Report,
        repair: Repair | None = None,
    ) -> bool:
        """`LocalStore::verifyPath` (`local-store.cc:1518`): one path, its referrers first, then itself."""
        key = path.path
        if key in done:
            return False
        done.add(key)
        if path.name in in_dir:
            valid.add(key)
            return False
        # The path is gone from the directory. Invalidate it only when every
        # referrer can go too; a valid referrer keeps it, and that is an error.
        errors = False
        can_invalidate = True
        for referrer in await self.query_referrers(key):
            if referrer.path == key:
                continue
            errors = await self._verify_path(referrer, in_dir, done, valid, report, repair) or errors
            if referrer.path in valid:
                can_invalidate = False
        if can_invalidate:
            await report(Verbosity.INFO, f"path '{key}' disappeared, removing from database...")
            await self._invalidate_path(key)
            return errors
        await report(Verbosity.ERROR, f"path '{key}' disappeared, but it still has valid referrers!")
        if repair is None:
            return True
        try:
            await repair(key)
        except verify.RepairError as e:
            await report(Verbosity.WARN, f"{WARNING_PREFIX}{e}")
            return True
        return errors

    async def _check_link_hashes(self, report: Report, *, repair: bool = False) -> bool:
        """The link-hash pass of `verifyStore` (`local-store.cc:1394`): a modified `.links` entry is an error.

        With *repair* the entry goes instead, and that is no error.
        """
        await report(Verbosity.INFO, "checking link hashes...")
        links_dir = self.layout.links_dir
        errors = False
        for name in await anyio.to_thread.run_sync(verify.list_links, links_dir):
            await report(Verbosity.TALKATIVE, f'checking contents of "{name}"')
            got = await anyio.to_thread.run_sync(verify.link_hash, links_dir / name)
            if got != name:
                await report(
                    Verbosity.ERROR, f"link \"{links_dir / name}\" was modified! expected hash {name}, got '{got}'"
                )
                if repair:
                    await anyio.to_thread.run_sync(verify.unlink_if_exists, links_dir / name)
                    await report(Verbosity.INFO, f'removed link "{links_dir / name}"')
                else:
                    errors = True
        return errors

    async def _invalidate_path(self, path: str) -> None:
        async with self.db.acquire_conn() as conn:
            await register.invalidate_path(conn, path)
        self._on_change(StorePath(path=path))

    async def _check_store_hashes(
        self, real_store_dir: Path, valid: set[str], report: Report, repair: Repair | None = None
    ) -> bool:
        """The store-hash pass of `verifyStore` (`local-store.cc:1415`): a modified path is an error.

        A path whose hash matches gets a missing hash or a zero size filled in,
        as Nix does. A read that fails on a still-valid path is an error.
        """
        await report(Verbosity.INFO, "checking store hashes...")
        errors = False
        # `StorePathSet` order: by base name.
        for key in sorted(valid, key=lambda p: p.rsplit("/", 1)[-1]):
            name = StorePath(path=key).name
            try:
                info = await self.query_path_info(key)
                if info is None:
                    errors = True
                    continue
                await report(Verbosity.TALKATIVE, f"checking contents of '{key}'")
                current_hex, current_size = await anyio.to_thread.run_sync(
                    verify.nar_hash_and_size, real_store_dir / name
                )
            except OSError as exc:
                # Nix logs its own `Error` here (`logError`); an OSError's text
                # is not Nix's, so only the level and the prefix match.
                if await self.is_valid_path_uncached(key):
                    await report(Verbosity.ERROR, f"error: {exc}")
                else:
                    await report(Verbosity.WARN, f"warning: {exc}")
                errors = True
                continue
            stored = str(info.info.nar_hash)
            if stored != verify.NULL_SHA256_HEX and stored != current_hex:
                expected, got = (f"sha256:{nix32_encode(bytes.fromhex(h))}" for h in (stored, current_hex))
                await report(Verbosity.ERROR, f"path '{key}' was modified! expected hash '{expected}', got '{got}'")
                if repair is None:
                    errors = True
                    continue
                try:
                    await repair(key)
                except verify.RepairError as e:
                    # `logError` of the `Error` the store-hash loop catches (`local-store.cc:1468`).
                    await report(Verbosity.ERROR, show_error(str(e)))
                    errors = True
                continue
            final_hash, final_size = stored, info.info.nar_size
            if stored == verify.NULL_SHA256_HEX:
                await report(Verbosity.INFO, f"fixing missing hash on '{key}'")
                final_hash = current_hex
            if info.info.nar_size == 0:
                await report(Verbosity.INFO, f"updating size field on '{key}' to {current_size}")
                final_size = current_size
            if (final_hash, final_size) != (stored, info.info.nar_size):
                await self._fill_hashes(key, nar_hash=f"sha256:{final_hash}", nar_size=final_size)
        return errors

    async def _fill_hashes(self, path: str, *, nar_hash: str, nar_size: int) -> None:
        async with self.db.acquire_conn() as conn:
            await register.update_path_hashes(conn, path, nar_hash=nar_hash, nar_size=nar_size)
        self._on_change(StorePath(path=path))
