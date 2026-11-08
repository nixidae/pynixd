"""`LocalStore::collectGarbage` of Nix 2.35 (`gc.cc:356`) over the engine.

One collection holds the big lock for writing and serves the roots socket
while it walks. From each candidate it walks the referrers: a root, a
temporary root or a live path anywhere in that closure keeps the candidate,
and otherwise the whole closure goes, referrers first. `keep-outputs` and
`keep-derivations` widen the walk for the whole store only; Nix 2.34 also
widens it for named paths, and 2.35 stopped (`gc.cc:598`, `:691`).
"""

from __future__ import annotations

import os
from collections import deque
from dataclasses import dataclass, field
from functools import partial
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
from anyio.lowlevel import checkpoint

from nix_daemon_protocol.protocol import GCAction, Verbosity
from nix_daemon_protocol.store_path import StorePath

from . import collect, register
from .errors import PynixdError
from .hintfmt import ANSI_NORMAL, ANSI_RED, magenta
from .liveness import unlink_stale_auto_roots, walk_root_sources
from .optimise import render_size

if TYPE_CHECKING:
    from collections.abc import Callable

    from .collect import Report
    from .engine import StoreEngine


class PathInUse(PynixdError):
    """`invalidatePathChecked` refuses a path a valid path still refers to (`local-store.cc:1366`)."""


class _LimitReached(Exception):
    """`GCLimitReached`: the collection freed more than `max-freed`."""


@dataclass
class Collection:
    """The state of one `collectGarbage` call: Nix's `roots`, `alive`, `dead` and `referrersCache`."""

    engine: StoreEngine
    options: collect.GCOptions
    settings: collect.GCSettings
    server: collect.RootsServer
    report: Report
    results: collect.GCResults = field(default_factory=collect.GCResults)
    roots: set[str] = field(default_factory=set)
    alive: set[str] = field(default_factory=set)
    dead: set[str] = field(default_factory=set)
    #: Paths whose whole closure is in `alive`, so marking them again adds nothing.
    closed: set[str] = field(default_factory=set)
    referrers: dict[str, set[str]] = field(default_factory=dict)

    @property
    def whole_store(self) -> bool:
        return self.options.paths is None

    @property
    def deletes(self) -> bool:
        return self.options.action in {GCAction.DELETE_DEAD, GCAction.DELETE_SPECIFIC}

    async def run(self) -> collect.GCResults:
        await self._find_roots()
        try:
            if self.options.paths is None:
                await self._whole_store()
            else:
                await self._specific(self.options.paths)
        except _LimitReached:
            pass
        if self.options.action == GCAction.RETURN_LIVE:
            self.results.paths = set(self.alive)
        elif self.options.action == GCAction.RETURN_DEAD:
            self.results.paths = set(self.dead)
        else:
            await self._sweep_links()
        return self.results

    async def _find_roots(self) -> None:
        """`findRootsNoTemp` then `findTempRoots` (`gc.cc:520`). The lock is held, so no permanent root appears."""
        await self.report(Verbosity.INFO, "finding garbage collector roots...")
        layout = self.engine.layout
        state_dir, store_dir = layout.state_dir, str(layout.store_dir)
        for link, target in await anyio.to_thread.run_sync(partial(unlink_stale_auto_roots, state_dir, store_dir)):
            await self.report(Verbosity.INFO, f'removing stale link from "{link}" to "{target}"')
        walk = await anyio.to_thread.run_sync(partial(walk_root_sources, state_dir, store_dir, reap=True))
        for path in walk.unreadable:
            await self.report(Verbosity.INFO, f'cannot read potential root "{path}"')
        named = set(walk.stable.values()) | set(walk.runtime)
        self.roots = {path.path for path in await self.engine.query_valid_paths(named)}
        for link, path in sorted(walk.stable.items()):
            # A plain file under `gcroots` that names an invalid path is skipped without a word (`gc.cc:280`).
            if path not in self.roots and os.path.islink(link):
                try:
                    target = await anyio.to_thread.run_sync(os.readlink, link)
                except OSError:
                    # Gone since the walk read it: Nix read it once, and this line is all that is lost.
                    continue
                await self.report(Verbosity.INFO, f'skipping invalid root from "{link}" to "{target}"')
        for reaped in walk.reaped:
            await self.report(Verbosity.INFO, f'removing stale temporary roots file "{reaped}"')
        for path in walk.temporary:
            self.server.temp_roots.add(_hash_part(path))
            self.roots.add(path)

    async def _specific(self, paths: frozenset[str]) -> None:
        action = self.options.action
        if action == GCAction.DELETE_DEAD:
            await self.report(Verbosity.INFO, "deleting garbage within specified paths...")
        elif action == GCAction.DELETE_SPECIFIC:
            await self.report(Verbosity.INFO, "deleting specified paths...")
        else:
            await self.report(Verbosity.INFO, "determining live/dead paths...")
        for path in sorted(paths, key=_base_name):
            await self._maybe_delete_referrers_closure(path)
            if path in self.dead:
                continue
            if action == GCAction.DELETE_SPECIFIC:
                raise PynixdError(
                    f"Cannot delete path '{magenta(path)}' since it is still alive. To find out why, use: "
                    "nix-store --query --roots and nix-store --query --referrers"
                )
            await self.report(Verbosity.DEBUG, f"cannot delete '{path}' because it's still alive")

    async def _whole_store(self) -> None:
        if self.options.max_freed == 0:
            return
        action = self.options.action
        if action == GCAction.DELETE_DEAD:
            await self.report(Verbosity.INFO, "deleting garbage...")
        elif action == GCAction.DELETE_SPECIFIC:
            raise PynixdError("Cannot delete the entire store")
        else:
            await self.report(Verbosity.INFO, "determining live/dead paths...")
        layout = self.engine.layout
        store_dir = str(layout.store_dir)
        names = await anyio.to_thread.run_sync(collect.store_dir_entries, layout.real_store_dir, layout.links_dir.name)
        for name in names:
            await checkpoint()
            if collect.store_base_name(f"{store_dir}/{name}", store_dir) is not None:
                await self._maybe_delete_referrers_closure(f"{store_dir}/{name}")
            else:
                await self._delete_from_store(name, known=False)

    async def _maybe_delete_referrers_closure(self, start: str) -> None:
        """`maybeDeleteReferrersClosure` (`gc.cc:581`): delete *start* and its referrers, or none of them."""
        visited: dict[str, None] = {}
        todo: deque[str] = deque()

        def enqueue(path: str) -> None:
            if path not in visited:
                visited[path] = None
                todo.append(path)

        try:
            enqueue(start)
            while todo:
                await checkpoint()
                path = todo.popleft()
                if path in self.alive:
                    await self.report(Verbosity.DEBUG, f"cannot delete '{start}' because '{path}' is alive")
                    self.alive.add(start)
                    return
                if path in self.dead:
                    continue
                if path in self.roots:
                    await self.report(Verbosity.DEBUG, f"cannot delete '{path}' because it's a root")
                    self.alive.add(start)
                    await self._mark_alive(path)
                    return
                if (
                    self.options.paths is not None
                    and not self.options.delete_referrers
                    and path not in self.options.paths
                ):
                    await self.report(
                        Verbosity.DEBUG,
                        f"cannot delete '{start}' because '{path}' is not in the specified paths to delete",
                    )
                    return
                if not self.server.hold(_hash_part(path)):
                    await self.report(Verbosity.DEBUG, f"cannot delete '{path}' because it's a temporary root")
                    self.alive.add(start)
                    await self._mark_alive(path)
                    return
                if await self.engine.is_valid_path_uncached(path):
                    await self._visit_edges(path, enqueue)
            await self._delete_closure(set(visited))
        finally:
            await self.server.release()

    async def _visit_edges(self, path: str, enqueue: Callable[[str], None]) -> None:
        if path not in self.referrers:
            self.referrers[path] = {referrer.path for referrer in await self.engine.query_gc_referrers(path)}
        for referrer in sorted(self.referrers[path], key=_base_name):
            enqueue(referrer)
        if not self.whole_store:
            return
        # With keep-derivations a derivation goes only with the outputs it made.
        if self.settings.keep_derivations and path.endswith(".drv"):
            outputs = (await self.engine.query_derivation_output_maps([path])).get(StorePath(path=path), {})
            for output in outputs.values():
                info = await self.engine.query_path_info(output.path)
                if info is not None and info.info.deriver is not None and info.info.deriver.path == path:
                    enqueue(output.path)
        # With keep-outputs an output goes only with its derivers.
        if self.settings.keep_outputs:
            for deriver in sorted(await self.engine.query_valid_derivers(path), key=lambda p: p.name):
                enqueue(deriver.path)

    async def _delete_closure(self, visited: set[str]) -> None:
        """The deletion loop of `maybeDeleteReferrersClosure` (`gc.cc:715`): referrers before references."""
        infos = await self.engine.query_path_infos(visited)
        references = {info.path.path: {ref.path for ref in info.info.references} for info in infos}
        for path in collect.topo_sort_paths(visited, references):
            if path in self.dead:
                continue
            self.dead.add(path)
            if not self.deletes:
                continue
            # A root may have arrived over the socket since the walk saw this path.
            if not self.server.hold(_hash_part(path)):
                await self.report(
                    Verbosity.DEBUG, f"not deleting '{path}' because it became a temporary root after initial scan"
                )
                await self._mark_alive(path)
                continue
            try:
                try:
                    await self._invalidate_path_checked(path)
                    await self._delete_from_store(_base_name(path), known=True)
                    self.referrers.pop(path, None)
                except PathInUse as exc:
                    # `printError("BUG: %s", e.what())`: `what()` carries the `error:` prefix (`error.cc:276`).
                    await self.report(Verbosity.ERROR, f"BUG: {ANSI_RED}error:{ANSI_NORMAL} {exc}")
            finally:
                await self.server.release()

    async def _mark_alive(self, path: str) -> None:
        """`markAlive` (`gc.cc:598`): *path* and its closure. An invalid path keeps only itself, as Nix's catch does."""
        self.alive.add(path)
        if path in self.closed:
            return
        outputs = self.whole_store and self.settings.keep_outputs
        derivers = self.whole_store and self.settings.keep_derivations
        seen: set[str] = set()
        todo = [path]
        while todo:
            current = todo.pop()
            if current in seen:
                continue
            seen.add(current)
            if current in self.closed:
                continue
            info = await self.engine.query_path_info(current)
            if info is None:
                return
            todo.extend(ref.path for ref in info.info.references if ref.path != current)
            if outputs and current.endswith(".drv"):
                maps = await self.engine.query_derivation_output_maps([current])
                for output in maps.get(StorePath(path=current), {}).values():
                    if await self.engine.is_valid_path_uncached(output.path):
                        todo.append(output.path)
            deriver = info.info.deriver.path if info.info.deriver is not None else ""
            if derivers and deriver and await self.engine.is_valid_path_uncached(deriver):
                todo.append(deriver)
        self.alive |= seen
        self.closed |= seen

    async def _invalidate_path_checked(self, path: str) -> None:
        """`invalidatePathChecked` (`local-store.cc:1357`): no valid referrer but itself, then invalidate."""
        if not await self.engine.is_valid_path_uncached(path):
            return
        referrers = {referrer.path for referrer in await self.engine.query_gc_referrers(path)} - {path}
        if referrers:
            names = ", ".join(f"'{referrer}'" for referrer in sorted(referrers, key=_base_name))
            raise PathInUse(f"cannot delete path '{magenta(path)}' because it is in use by {magenta(names)}")
        async with self.engine.db.acquire_conn() as conn:
            await register.invalidate_path(conn, path)
        self.engine.invalidate_path_info_cache_for(path)

    async def _delete_from_store(self, base_name: str, *, known: bool) -> None:
        """`deleteFromStore` (`gc.cc:540`): delete one name of the store directory and count it."""
        layout = self.engine.layout
        path = f"{layout.store_dir}/{base_name}"
        real_path = layout.real_store_dir / base_name
        held: int | None = None
        if base_name.startswith("tmp-"):
            held = await anyio.to_thread.run_sync(collect.lock_tmp_dir, real_path)
            if held is None:
                await self.report(Verbosity.DEBUG, f'skipping locked tempdir "{real_path}"')
                return
        try:
            await self.report(Verbosity.INFO, f"deleting '{path}'")
            self.results.paths.add(path)
            try:
                freed = await self.engine.delete_store_path(base_name)
            except collect.DeleteError as exc:
                what = "store path" if known else "garbage in store directory"
                exc.with_trace(f"While deleting {what} {collect.path_fmt(real_path)}")
                raise
        finally:
            if held is not None:
                os.close(held)
        self.results.bytes_freed += freed
        if self.results.bytes_freed > self.options.max_freed:
            await self.report(Verbosity.INFO, f"deleted more than {self.options.max_freed} bytes; stopping")
            raise _LimitReached

    async def _sweep_links(self) -> None:
        """The `.links` sweep (`gc.cc:896`): an entry with one link is no file's any more."""
        await self.report(Verbosity.INFO, "deleting unused links...")
        sweep = await anyio.to_thread.run_sync(collect.sweep_unused_links, self.engine.layout.links_dir)
        for link in sweep.unlinked:
            await self.report(Verbosity.TALKATIVE, f'deleting unused link "{link}"')
        await self.report(Verbosity.INFO, f"note: hard linking is currently saving {render_size(sweep.saving)}")


def _base_name(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def _hash_part(path: str) -> str:
    return _base_name(path)[: collect.HASH_LEN]
