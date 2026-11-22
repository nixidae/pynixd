"""A store over a lower store whose files stay where they are: a build pod's store over its nixkube farm.

nixidae/nixidae#1560, spike. The farm is a read-only volume with its own
database. No OverlayFS merges it with the pod's store: OverlayFS follows no
submount of a lower layer, and a farm is one bind mount per path
(`nixkube/pkgs/nixkube/src/nri/farm.py`). So each path's files are in one of
the two directories.

The database half is `LayeredEngine`'s. It copies a lower path's info into
this store's database once a reference needs it, so this database cannot say
where the files are. The lower store cannot change (`StoreLayout.immutable`),
so `load_lower` reads its path names once, and the names decide.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.store_path import StorePath

from .database import DatabaseError
from .drv_parser import read_drv_file
from .engine import StoreEngine
from .overlay_engine import LayeredEngine

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Mapping
    from pathlib import Path
    from typing import Any

    from nix_daemon_protocol.signature import Signature

    from .drv_parser import Derivation
    from .store_layout import StoreLayout
    from .temp_roots import RootWriter

# Any derivation name: `load_lower` asks only whether the table answers.
_PROBE_DRV = "00000000000000000000000000000000-probe.drv"


def _name(path: StorePath | str) -> str:
    return path.name if isinstance(path, StorePath) else StorePath(path=str(path)).name


class LowerStoreEngine(LayeredEngine):
    """The operations of a chroot store over an immutable lower store (`StoreLayout.lower`)."""

    def __init__(
        self,
        layout: StoreLayout,
        *,
        writes: bool,
        read_config: Callable[[], Awaitable[Mapping[str, Any]]],
        on_change: Callable[[StorePath], None],
        name: str,
        roots: RootWriter | None = None,
    ) -> None:
        if layout.lower is None:
            raise ValueError(f"store {name}: a lower store engine needs a lower store")
        super().__init__(
            layout,
            lower=layout.lower,
            writes=writes,
            read_config=read_config,
            on_change=on_change,
            name=name,
            roots=roots,
        )
        self.lower_names: frozenset[str] = frozenset()
        self._lower_realisations: frozenset[bool] = frozenset()
        """The keyings (`by_path`) whose realisation table the lower store holds."""

    async def load_lower(self) -> None:
        """Read the lower store's path names, and which realisation tables it holds. Its database is open.

        Nix makes a table only with `ca-derivations` on, and 2.34 keys it by hash (`Realisations`) where
        2.35 keys it by path (`BuildTraceV3`). Measured: a farm the node's 2.35 filled held only the
        second, and a pod's 2.34 asked the first.
        """
        self.lower_names = frozenset(path.name for path in await self.lower.query_all_valid_paths())
        held: set[bool] = set()
        for by_path in (True, False):
            try:
                await self.lower.query_realisation(_PROBE_DRV, "out", by_path=by_path)
            except DatabaseError:
                continue
            held.add(by_path)
        self._lower_realisations = frozenset(held)

    def files_of(self, path: StorePath | str) -> Path:
        if _name(path) in self.lower_names:
            return self.lower.layout.real_path(path)
        return super().files_of(path)

    async def read_derivation(self, drv_path: str) -> Derivation | None:
        if _name(drv_path) in self.lower_names:
            return await read_drv_file(drv_path, self.lower.layout)
        return await super().read_derivation(drv_path)

    async def query_realisation(
        self, drv: str, output_name: str, *, by_path: bool
    ) -> tuple[StorePath, list[Signature]] | None:
        if by_path not in self._lower_realisations:
            return await StoreEngine.query_realisation(self, drv, output_name, by_path=by_path)
        return await super().query_realisation(drv, output_name, by_path=by_path)

    async def register_drv_output(
        self, drv: str, output_name: str, out_path: str, sigs: Iterable[Signature], *, by_path: bool
    ) -> None:
        if by_path not in self._lower_realisations:
            await StoreEngine.register_drv_output(self, drv, output_name, out_path, sigs, by_path=by_path)
            return
        await super().register_drv_output(drv, output_name, out_path, sigs, by_path=by_path)

    async def delete_store_path(self, base_name: str) -> int:
        """A lower path's files are the lower store's: this store frees nothing of them."""
        if base_name in self.lower_names:
            return 0
        return await super().delete_store_path(base_name)
