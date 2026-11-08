"""pynixd's `local-overlay://` store: Nix's `LocalOverlayStore` (`local-overlay-store.cc`).

The store is a plain local store whose database is the upper one, over a lower
store that it never writes. Each read Nix overrides asks the upper database,
then the lower one. `isValidPathUncached` alone writes: it copies a lower
path's info, with its closure, into the upper database, because
`LocalStore::registerValidPaths` (`local-store.cc:985`) refuses a reference the
upper database does not hold.

`Refs.reference` is a foreign key into the upper `ValidPaths`, so the upper
database holds the closure of each path it holds. A closure query therefore
takes the upper closure of the paths the upper database holds and the lower
closure of the rest, and walks no path across the two.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.store_path import StorePath

from .engine import StoreEngine

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping
    from typing import Any

    from nix_daemon_protocol.signature import Signature
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from .local_store_db import SyncReader
    from .store_layout import StoreLayout


class LowerPathNeedsSync(Exception):
    """The lower store holds the path, and the engine may not copy its info up.

    Nix answers such a query and writes the upper database in the same call.
    pynixd without `pynixd_writes` leaves that write, and so the answer, to
    the daemon.
    """


def _forget_nothing(_path: StorePath) -> None:
    """The lower store changes no path the cache above the engine holds."""


class OverlayEngine(StoreEngine):
    """The operations of a `local-overlay://` store, with Nix's names. `lower` is the lower store."""

    def __init__(
        self,
        layout: StoreLayout,
        *,
        writes: bool,
        read_config: Callable[[], Awaitable[Mapping[str, Any]]],
        on_change: Callable[[StorePath], None],
        name: str,
    ) -> None:
        super().__init__(layout, writes=writes, read_config=read_config, on_change=on_change, name=name)
        if layout.overlay is None:
            raise ValueError(f"store {name}: an overlay engine needs an overlay layout")
        self.lower = StoreEngine(
            layout.overlay.lower, writes=False, read_config=read_config, on_change=_forget_nothing, name=f"{name} lower"
        )

    async def is_valid_path_uncached(self, path: str) -> bool:
        """`LocalOverlayStore::isValidPathUncached` (`local-overlay-store.cc:157`)."""
        return await super().is_valid_path_uncached(path) or await self._sync_from_lower([path]) == {path}

    async def is_valid_path(self, path: str, *, reader: SyncReader | None = None) -> bool:
        """`isValidPathUncached`, the upper database through a session's *reader* when it can answer."""
        return await super().is_valid_path(path, reader=reader) or await self._sync_from_lower([path]) == {path}

    async def query_valid_paths(self, paths: Iterable[str]) -> set[StorePath]:
        """`LocalStore::queryValidPaths` (`local-store.cc:844`): `isValidPath` of each, so it syncs too."""
        paths = list(paths)
        upper = await super().query_valid_paths(paths)
        rest = [path for path in paths if StorePath(path=path) not in upper]
        return upper | {StorePath(path=path) for path in await self._sync_from_lower(rest)}

    async def _sync_from_lower(self, paths: list[str]) -> set[str]:
        """Copy the info of each of *paths* the lower store holds, with its closure, into the upper database.

        Answers the paths the lower store holds. Nix registers one path at a
        time, references first; pynixd registers the closure in one
        transaction, which leaves the same rows.

        Nix skips the sync when its path-info cache already holds the path
        (`Store::isValidPath`, `store-api.cc:520`). pynixd syncs whenever the
        upper database lacks the path.
        """
        if not paths:
            return set()
        held = {str(path) for path in await self.lower.query_valid_paths(paths)}
        if not held:
            return held
        if not (self.writes and self.writable):
            raise LowerPathNeedsSync(sorted(held)[0])
        closure = await self.lower.query_closure_with_info(sorted(held))
        upper = await StoreEngine.query_valid_paths(self, (str(info.path) for info in closure))
        if missing := [info for info in closure if info.path not in upper]:
            await StoreEngine.register_valid_paths(self, missing)
        return held

    async def query_path_info(self, path: str, *, reader: SyncReader | None = None) -> ValidPathInfo | None:
        """`LocalOverlayStore::queryPathInfoUncached` (`local-overlay-store.cc:93`): the upper store, then the lower."""
        found = await super().query_path_info(path, reader=reader)
        return found if found is not None else await self.lower.query_path_info(path)

    async def query_referrers(self, path: str) -> set[StorePath]:
        """`LocalOverlayStore::queryReferrers` (`local-overlay-store.cc:174`): both stores."""
        return await super().query_referrers(path) | await self.lower.query_referrers(path)

    async def query_valid_derivers(self, path: str) -> set[StorePath]:
        """`LocalOverlayStore::queryValidDerivers` (`local-overlay-store.cc:185`): both stores."""
        return await super().query_valid_derivers(path) | await self.lower.query_valid_derivers(path)

    async def query_path_from_hash_part(self, hash_part: str) -> StorePath | None:
        """`LocalOverlayStore::queryPathFromHashPart` (`local-overlay-store.cc:193`): the upper store, then the lower."""
        found = await super().query_path_from_hash_part(hash_part)
        return found if found is not None else await self.lower.query_path_from_hash_part(hash_part)

    async def query_realisation(
        self, drv: str, output_name: str, *, by_path: bool
    ) -> tuple[StorePath, list[Signature]] | None:
        """`LocalOverlayStore::queryRealisationUncached` (`local-overlay-store.cc:120`): the upper store, then the lower."""
        found = await super().query_realisation(drv, output_name, by_path=by_path)
        return found if found is not None else await self.lower.query_realisation(drv, output_name, by_path=by_path)

    async def _split(self, paths: Iterable[str]) -> tuple[list[str], list[str]]:
        """*paths* the upper database holds, and the rest."""
        paths = list(paths)
        upper = {str(path) for path in await StoreEngine.query_valid_paths(self, paths)}
        return [path for path in paths if path in upper], [path for path in paths if path not in upper]

    async def query_closure(self, paths: Iterable[str]) -> set[StorePath]:
        upper, rest = await self._split(paths)
        return await super().query_closure(upper) | await self.lower.query_closure(rest)

    async def query_closure_with_info(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        """The upper closure, then the rest of the lower closure, each part in registration order.

        A lower path refers to lower paths only, and an upper one to upper
        paths only, so the order keeps each reference ahead of its referrer.
        """
        upper, rest = await self._split(paths)
        found = await super().query_closure_with_info(upper)
        held = {info.path for info in found}
        lower = await self.lower.query_closure_with_info(rest)
        upper_lower = await StoreEngine.query_valid_paths(self, (str(info.path) for info in lower))
        # The upper database holds the closure of each of these, so its order keeps references first too.
        merged = await super().query_closure_with_info(sorted(str(path) for path in upper_lower - held))
        return [
            *found,
            *(info for info in merged if info.path not in held),
            *(info for info in lower if info.path not in upper_lower),
        ]

    async def query_path_infos(self, paths: Iterable[str]) -> list[ValidPathInfo]:
        upper, rest = await self._split(paths)
        return [*await super().query_path_infos(upper), *await self.lower.query_path_infos(rest)]

    async def register_valid_paths(self, infos: Iterable[ValidPathInfo]) -> None:
        """`LocalOverlayStore::registerValidPaths` (`local-overlay-store.cc:203`): the lower info first, then *infos*.

        A path the lower store holds and the upper does not takes the lower
        info first, so the request updates that row.
        """
        infos = list(infos)
        _upper, rest = await self._split(str(info.path) for info in infos)
        held = sorted(str(path) for path in await self.lower.query_valid_paths(rest))
        if held:
            await super().register_valid_paths(await self.lower.query_path_infos(held))
        await super().register_valid_paths(infos)

    async def register_drv_output(
        self, drv: str, output_name: str, out_path: str, sigs: Iterable[Signature], *, by_path: bool
    ) -> None:
        """`LocalOverlayStore::registerDrvOutput` (`local-overlay-store.cc:82`): the lower realisation first."""
        found = await self.lower.query_realisation(drv, output_name, by_path=by_path)
        if found is not None:
            lower_path, lower_sigs = found
            await super().register_drv_output(drv, output_name, str(lower_path), lower_sigs, by_path=by_path)
        await super().register_drv_output(drv, output_name, out_path, sigs, by_path=by_path)
