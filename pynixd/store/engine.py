"""pynixd's local store, apart from the daemon: Nix's `LocalStore` (`docs/design/store-engine.md`).

The engine owns the store's layout, its database, the settings it reads and
its temporary roots. Its methods take plain types: a store path, a
`ValidPathInfo`, a `Source`. It reads no wire request and holds no daemon
connection; what it needs of the daemon (its settings) and of the cache
above it (a path that changed) come in as callables.
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

from ..local_store_db import LocalStoreDB
from ..temp_roots import TempRoots
from . import add, register
from .queries import IS_VALID_PATH, QUERY_PATH_INFO_WITH_REFS

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping
    from typing import Any

    from ..content_address import Method
    from ..local_store_db import SyncReader
    from ..store_layout import StoreLayout
    from .source import Source


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

    def invalidate_path_info_cache_for(self, path: StorePath | str) -> None:
        """`Store::invalidatePathInfoCacheFor`: the cache above the engine forgets *path*."""
        self._on_change(StorePath(path=str(path)))

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
