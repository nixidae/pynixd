"""A local store as a substituter: the `LocalStore` that a path, `local://<root>` or `local?root=` opens.

NIX-DEVIATION (#27): Nix opens such a store as it opens its own. It takes the big lock in `db/` and migrates an
old schema (`local-store.cc:232`, `:255`), so a substituter it only reads from changes. pynixd opens the database
read-only (`LocalStoreDB.open`) and dumps each NAR from the files, so the store stays as it was. A database that
needs a migration does not open, and the daemon answers for the substituter. Reverse it if such stores are common.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from anyio.to_thread import run_sync

from pynixd_store.engine import StoreEngine
from pynixd_store.local_store_db import DatabaseError, LocalStoreDB
from pynixd_store.nar_fs import dump_path

from ..exceptions import BackendError, CacheFault, OpNotImplementedError
from ..serde import (
    IsValidPathRequest,
    IsValidPathResponse,
    QueryPathFromHashPartRequest,
    QueryPathFromHashPartResponse,
    QueryPathInfoRequest,
    QueryPathInfoResponse,
    QueryValidPathsRequest,
    QueryValidPathsResponse,
    Realisation,
    StorePath,
)
from .base import Store

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    from nix_daemon_protocol.wire_ops import WireRequest
    from pynixd_store.drv_parser import Derivation

    from ..config import LocalStoreSubstituterSpec
    from ..connection import ClientConn, Connection
    from ..serde import DrvOutput


async def _no_settings() -> Mapping[str, Any]:
    """The engine reads no setting: it only answers queries."""
    return {}


def _nothing_cached(_path: StorePath) -> None:
    """The store never changes under this engine, so nothing above it is stale."""


class LocalStoreSubstituter(Store):
    """A local store at a chroot root that pynixd reads and never changes."""

    def __init__(self, spec: LocalStoreSubstituterSpec) -> None:
        super().__init__(spec)
        self.root = spec.store_path
        self.layout = spec.layout()
        self.engine = StoreEngine(
            self.layout, writes=False, read_config=_no_settings, on_change=_nothing_cached, name=str(self.store_id)
        )

    def human_readable_uri(self) -> str:
        return f"local?root={self.root}"

    async def start(self, sync_paths: bool = True) -> None:
        if self._started:
            return
        db = await LocalStoreDB.open(self.layout, read_only=True)
        if not db.active:
            raise BackendError(f"pynixd: store {self.store_id} has no database that pynixd can read")
        self.engine.db = db
        self._started = True

    async def close(self) -> None:
        await self.engine.db.close()
        self._started = False

    async def create_conn(self) -> Connection:
        raise OpNotImplementedError(f"{type(self).__name__} does not use daemon wire connections")

    async def call(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        raise_on_error: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        raise OpNotImplementedError(f"{type(self).__name__} cannot call {type(request).__name__}")

    async def execute(
        self,
        request: WireRequest,
        client: ClientConn | None = None,
        suppress_last: bool = False,
        skip_probe: bool = False,
    ) -> Any:
        if isinstance(request, IsValidPathRequest):
            return await self.is_valid_path(request)
        if isinstance(request, QueryPathInfoRequest):
            return await self.query_path_info(request)
        if isinstance(request, QueryPathFromHashPartRequest):
            return await self.query_path_from_hash_part(request)
        if isinstance(request, QueryValidPathsRequest):
            return await self.query_valid_paths(request)
        raise OpNotImplementedError(f"{type(self).__name__} does not support {type(request).__name__}")

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        return await self.engine.read_derivation(str(drv_store_path))

    async def is_valid_path(
        self, request: IsValidPathRequest, client: Any = None, suppress_last: bool = False
    ) -> IsValidPathResponse:
        return IsValidPathResponse.fast(valid=await self.engine.is_valid_path(str(request.path)))

    async def query_path_info(
        self, request: QueryPathInfoRequest, client: Any = None, suppress_last: bool = False
    ) -> QueryPathInfoResponse:
        found = await self.engine.query_path_info(str(request.path))
        if found is None:
            return QueryPathInfoResponse.fast(valid=False)
        return QueryPathInfoResponse.fast(valid=True, info=found.info)

    async def query_path_from_hash_part(
        self, request: QueryPathFromHashPartRequest, client: Any = None, suppress_last: bool = False
    ) -> QueryPathFromHashPartResponse | None:
        found = await self.engine.query_path_from_hash_part(str(request.path))
        return None if found is None else QueryPathFromHashPartResponse(value=StorePath(path=str(found)))

    async def query_valid_paths(
        self, request: QueryValidPathsRequest, client: Any = None, suppress_last: bool = False
    ) -> QueryValidPathsResponse:
        return QueryValidPathsResponse(paths=await self.engine.query_valid_paths(str(path) for path in request.paths))

    async def nar_from_path(self, request: Any, client: Any = None, suppress_last: bool = False) -> Any:
        raise OpNotImplementedError(f"{type(self).__name__} streams NARs through read_nar()")

    async def read_nar(self, path: StorePath, nar_size: int) -> AsyncIterator[bytes]:
        """The NAR of *path*, dumped from the store's files (`Store::narFromPath`, `store-api.cc:334`)."""
        del nar_size
        pieces = dump_path(self.layout.real_store_dir / path.name)
        try:
            while (piece := await run_sync(next, pieces, None)) is not None:
                yield piece
        finally:
            pieces.close()

    async def find_realisation(self, key: DrvOutput, drv_path: StorePath, *, by_path: bool) -> Realisation | None:
        """The store's build trace, keyed as pynixd's own Nix keys it: that Nix is the one that would open this store."""
        try:
            found = await self.engine.query_realisation(
                drv_path.name if by_path else key.drv_hash, key.output_name, by_path=by_path
            )
        except DatabaseError as ex:
            raise CacheFault(f"store {self.store_id}: cannot read its realisations") from ex
        if found is None:
            return None
        out_path, signatures = found
        return Realisation(id=key, out_path=out_path, signatures=signatures)
