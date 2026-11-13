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

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread

from nix_daemon_protocol.protocol import Verbosity
from nix_daemon_protocol.store_path import StorePath

from . import collect, optimise
from .builder.hooks import status_text
from .engine import StoreEngine
from .errors import PynixdError
from .hintfmt import WARNING_PREFIX

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping
    from typing import Any

    from nix_daemon_protocol.signature import Signature
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from .engine import Report
    from .local_store_db import SyncReader
    from .store_layout import Overlay, StoreLayout


class LowerPathNeedsSync(Exception):
    """The lower store holds the path, and the engine may not copy its info up.

    Nix answers such a query and writes the upper database in the same call.
    pynixd without `pynixd_writes` leaves that write, and so the answer, to
    the daemon.
    """


def _unescape_mount_field(field: str) -> str:
    """A field of `/proc/self/mounts`: the kernel writes a space, a tab, a newline and `\\` as octal escapes."""
    return re.sub(r"\\([0-7]{3})", lambda match: chr(int(match.group(1), 8)), field)


def mounted_correctly(mounts: str, overlay: Overlay, real_store_dir: Path) -> bool:
    """`check-mount` (`local-overlay-store.cc:49`): the last mount at *real_store_dir* is OverlayFS over these layers.

    Mounts stack, so the last entry at the directory is the mount in effect. It must be OverlayFS, its
    `lowerdir` the lower store's real store directory, alone, and its `upperdir` the upper layer.

    NIX-DEFECT (#23): Nix matches `/proc/self/mounts` with the regex `(^|\\n)overlay <dir> `
    (`local-overlay-store.cc:54`). That reads the mount's source, which `mount` takes from its caller, and
    not the file system type, and it skips a later mount of another type over the overlay. pynixd splits the
    fields and compares the type of the last mount; Lillecarl/nix#431.
    """
    options: str | None = None
    for line in mounts.splitlines():
        fields = line.split(" ")
        if len(fields) >= 4 and Path(_unescape_mount_field(fields[1])) == real_store_dir:
            options = _unescape_mount_field(fields[3]) if fields[2] == "overlay" else None
    if options is None:
        return False
    found = dict(option.partition("=")[::2] for option in options.split(","))
    return found.get("lowerdir") == str(overlay.lower.real_store_dir) and found.get("upperdir") == str(
        overlay.upper_layer
    )


def check_mount(layout: StoreLayout) -> None:
    """Refuse an overlay store whose mount `mounted_correctly` rejects, with Nix's text (`local-overlay-store.cc:71`).

    It blocks: run it in a thread.
    """
    if layout.overlay is None or not layout.overlay.check_mount:
        return
    mounts = Path("/proc/self/mounts").read_text()
    if not mounted_correctly(mounts, layout.overlay, layout.real_store_dir):
        raise PynixdError(f"overlay filesystem {collect.path_fmt(layout.real_store_dir)} mounted incorrectly")


def _forget_nothing(_path: StorePath) -> None:
    """The lower store changes no path the cache above the engine holds."""


async def _say_nothing(_level: Verbosity, _text: str) -> None:
    """A caller that takes no lines."""


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
        self.overlay: Overlay = layout.overlay
        self.lower = StoreEngine(
            layout.overlay.lower, writes=False, read_config=read_config, on_change=_forget_nothing, name=f"{name} lower"
        )
        #: `_remountRequired`: a delete went through the upper layer, and the mount does not see it yet.
        self.remount_required = False

    async def is_valid_path_uncached(self, path: str) -> bool:
        """`LocalOverlayStore::isValidPathUncached` (`local-overlay-store.cc:143`)."""
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
        """`LocalOverlayStore::queryPathInfoUncached` (`local-overlay-store.cc:92`): the upper store, then the lower."""
        found = await super().query_path_info(path, reader=reader)
        return found if found is not None else await self.lower.query_path_info(path)

    async def query_referrers(self, path: str) -> set[StorePath]:
        """`LocalOverlayStore::queryReferrers` (`local-overlay-store.cc:161`): both stores."""
        return await super().query_referrers(path) | await self.lower.query_referrers(path)

    async def query_valid_derivers(self, path: str) -> set[StorePath]:
        """`LocalOverlayStore::queryValidDerivers` (`local-overlay-store.cc:172`): both stores."""
        return await super().query_valid_derivers(path) | await self.lower.query_valid_derivers(path)

    async def query_path_from_hash_part(self, hash_part: str) -> StorePath | None:
        """`LocalOverlayStore::queryPathFromHashPart` (`local-overlay-store.cc:180`): the upper store, then the lower."""
        found = await super().query_path_from_hash_part(hash_part)
        return found if found is not None else await self.lower.query_path_from_hash_part(hash_part)

    async def query_path_from_nar_hash(self, nar_hash: str) -> StorePath | None:
        """The upper store, then the lower, as `query_path_from_hash_part`."""
        found = await super().query_path_from_nar_hash(nar_hash)
        return found if found is not None else await self.lower.query_path_from_nar_hash(nar_hash)

    async def query_realisation(
        self, drv: str, output_name: str, *, by_path: bool
    ) -> tuple[StorePath, list[Signature]] | None:
        """`LocalOverlayStore::queryRealisationUncached` (`local-overlay-store.cc:117`): the upper store, then the lower."""
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

    async def register_valid_paths(self, infos: Iterable[ValidPathInfo], report: Report | None = None) -> None:
        """`LocalOverlayStore::registerValidPaths` (`local-overlay-store.cc:189`): the lower info first, then *infos*.

        A path the lower store holds and the upper does not takes the lower
        info first, so the request updates that row.
        """
        infos = list(infos)
        _upper, rest = await self._split(str(info.path) for info in infos)
        held = sorted(str(path) for path in await self.lower.query_valid_paths(rest))
        if held:
            await super().register_valid_paths(await self.lower.query_path_infos(held))
        await super().register_valid_paths(infos, report)

    async def register_drv_output(
        self, drv: str, output_name: str, out_path: str, sigs: Iterable[Signature], *, by_path: bool
    ) -> None:
        """`LocalOverlayStore::registerDrvOutput` (`local-overlay-store.cc:82`): the lower realisation first."""
        found = await self.lower.query_realisation(drv, output_name, by_path=by_path)
        if found is not None:
            lower_path, lower_sigs = found
            await super().register_drv_output(drv, output_name, str(lower_path), lower_sigs, by_path=by_path)
        await super().register_drv_output(drv, output_name, out_path, sigs, by_path=by_path)

    async def delete_store_path(self, base_name: str) -> int:
        """`LocalOverlayStore::deleteStorePath` (`local-overlay-store.cc:214`).

        A path the upper layer lacks keeps its files. A path both layers hold
        goes from the upper layer, so OverlayFS writes no whiteout, and the
        store then needs a remount. Any other path goes through the mount.

        NIX-DEFECT (#23): with no upper copy, Nix sets no `bytesFreed`, and
        `deleteFromStore` (`gc.cc:564`) declares it without a value, so the
        collector adds an undefined number to the bytes freed. Setting it to
        0 there would fix it. pynixd counts 0; Lillecarl/nix#407.
        """
        if not await anyio.to_thread.run_sync(os.path.lexists, self.overlay.upper_path(base_name)):
            return 0
        if await self.lower.is_valid_path_uncached(f"{self.layout.store_dir}/{base_name}"):
            freed = await anyio.to_thread.run_sync(collect.delete_path, self.overlay.upper_path(base_name))
            self.remount_required = True
            return freed
        return await super().delete_store_path(base_name)

    async def remount_if_necessary(self, report: Report) -> None:
        """`LocalOverlayStore::remountIfNecessary` (`local-overlay-store.cc:284`): run `remount-hook` with the store directory.

        A hook that fails keeps the flag, as Nix's exception does.
        """
        if not self.remount_required:
            return
        real_store_dir = self.layout.real_store_dir
        hook = self.overlay.remount_hook
        if hook is None:
            await report(
                Verbosity.WARN,
                f"{WARNING_PREFIX}{collect.path_fmt(real_store_dir)} needs remounting, "
                "set remount-hook to do this automatically",
            )
        else:
            # `runProgram` reads the hook's output and passes its errors through.
            done = await anyio.run_process([str(hook), str(real_store_dir)], check=False, stderr=None)
            if done.returncode != 0:
                raise PynixdError(f"program {collect.path_fmt(hook)} {status_text(done.returncode)}")
        self.remount_required = False

    async def optimise_store(
        self, *, hold: Callable[[str], Awaitable[None]], report: Report | None = None
    ) -> optimise.OptimiseStats:
        """`LocalOverlayStore::optimiseStore` (`local-overlay-store.cc:240`): drop each upper copy of a lower path.

        It hard-links nothing, takes no temporary root and prints no summary.
        """
        if not self.writes:
            raise RuntimeError(f"store {self._name}: pynixd_writes is off, so pynixd optimises no store")
        if not self.writable:
            raise RuntimeError(f"store {self._name}: pynixd cannot write the store")
        stats = optimise.OptimiseStats()
        for path in await self.query_all_valid_paths():
            if await self.lower.is_valid_path_uncached(path.path):
                await self.delete_store_path(path.name)
        await self.remount_if_necessary(report or _say_nothing)
        return stats

    async def collect_garbage(
        self, options: collect.GCOptions, *, settings: collect.GCSettings, report: Report
    ) -> collect.GCResults:
        """`LocalOverlayStore::collectGarbage` (`local-overlay-store.cc:207`): collect, then remount."""
        results = await super().collect_garbage(options, settings=settings, report=report)
        await self.remount_if_necessary(report)
        return results
