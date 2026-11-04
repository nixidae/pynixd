"""The NARs of one store, read on the route of NarFromPath (op 38).

`pynixd` dumps the store's files (`LocalDBStore.nar_pieces`). `daemon` reads
the daemon's NAR over one transfer connection. `shadow` reads the daemon's,
dumps the files beside it, and compares the two (`NarShadow`); the reader
gets the daemon's bytes whatever the comparison says. A store that is not a
`LocalDBStore` has the daemon route.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Self

import anyio
import structlog

from nix_daemon_protocol.nar_from_path import NarFromPathRequest

from .. import metrics
from ..exceptions import BackendError
from ..op_routes import OpRoute
from ..wire import _CHUNK_SIZE
from .local_db import LocalDBStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from contextlib import AbstractAsyncContextManager
    from types import TracebackType

    from nix_daemon_protocol.store_path import StorePath

    from ..connection import Connection
    from .base import Store
    from .daemon import DaemonStore

log = structlog.get_logger(__name__)

NAR_FROM_PATH_OP = NarFromPathRequest.op


def nar_route(store: Store) -> OpRoute:
    return store.routes.get(NAR_FROM_PATH_OP, OpRoute.daemon) if isinstance(store, LocalDBStore) else OpRoute.daemon


class NarShadow:
    """pynixd's dump of a path, beside the daemon's NAR of it, compared by SHA-256 and size.

    The dump reads the same files at the same time. A failure in it counts
    as `error` and touches nothing else.
    """

    def __init__(self, store: LocalDBStore, path: StorePath) -> None:
        self.store = store
        self.path = path
        self.theirs = hashlib.sha256()
        self.ours = hashlib.sha256()
        self.our_size = 0
        self.error: Exception | None = None
        self.done = anyio.Event()

    async def dump(self) -> None:
        try:
            async for piece in self.store.nar_pieces(self.path):
                self.ours.update(piece)
                self.our_size += len(piece)
        except Exception as exc:
            self.error = exc
        self.done.set()

    def compare(self, their_size: int) -> None:
        if self.error is not None or not self.done.is_set():
            log.warning("op_shadow_error", op="NarFromPath", path=str(self.path), error=repr(self.error))
            metrics.OP_SHADOW.labels(store_id=self.store.store_id, op="NarFromPath", outcome="error").inc()
            return
        same = self.our_size == their_size and self.ours.digest() == self.theirs.digest()
        outcome = "match" if same else "mismatch"
        metrics.OP_SHADOW.labels(store_id=self.store.store_id, op="NarFromPath", outcome=outcome).inc()
        if not same:
            log.warning(
                "op_shadow_mismatch",
                op="NarFromPath",
                path=str(self.path),
                pynixd=(self.our_size, self.ours.hexdigest()),
                daemon=(their_size, self.theirs.hexdigest()),
            )


class NarReader:
    """`async with NarReader(store) as nars:`, then `nars.read(path, nar_size)` for each path.

    The daemon route holds one transfer connection for every read, so a
    copy of many paths takes one worker, as one `nix copy` does. A read
    left unfinished marks that connection dirty, and the pool closes it.
    The pynixd route opens no connection.

    The shadow route dumps a path after the daemon's NAR of it ends, not
    beside it as `NarFromPathHandler` does: the substitution queue reads
    inside an async generator, which cannot hold a task group across `yield`.

    *nar_size* is the size the consumer already promised its reader (a
    `Content-Length`, a frame of path info). A dump of another size raises.
    """

    def __init__(self, store: DaemonStore | LocalDBStore) -> None:
        self.store = store
        self.route = nar_route(store)
        self._conn_cm: AbstractAsyncContextManager[Connection] | None = None
        self._conn: Connection | None = None

    async def __aenter__(self) -> Self:
        if self.route is OpRoute.pynixd:
            return self
        self._conn_cm = self.store.transfer_conn()
        self._conn = await self._conn_cm.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if self._conn_cm is not None:
            await self._conn_cm.__aexit__(exc_type, exc, tb)

    async def read(self, path: StorePath, nar_size: int) -> AsyncIterator[bytes]:
        if nar_size <= 0:
            raise BackendError(f"pynixd: no NAR size for '{path}', so its NAR has no known end")
        if self.route is OpRoute.pynixd:
            async for piece in self._dump(path, nar_size):
                yield piece
            return
        async for piece in self._daemon(path, nar_size):
            yield piece

    async def _dump(self, path: StorePath, nar_size: int) -> AsyncIterator[bytes]:
        if not isinstance(self.store, LocalDBStore):
            raise TypeError(f"store {self.store.store_id} has no files to dump")
        size = 0
        async for piece in self.store.nar_pieces(path):
            size += len(piece)
            if size > nar_size:
                break
            yield piece
        if size != nar_size:
            raise BackendError(
                f"pynixd: the NAR of '{path}' broke off: the store's files dump to {size} bytes, "
                f"and its path info says {nar_size}"
            )

    async def _daemon(self, path: StorePath, nar_size: int) -> AsyncIterator[bytes]:
        conn = self._conn
        if conn is None:
            raise RuntimeError("NarReader.read outside `async with`")
        shadow = None
        if self.route is OpRoute.shadow and isinstance(self.store, LocalDBStore):
            shadow = NarShadow(self.store, path)
        finished = False
        try:
            await conn.send_request(NarFromPathRequest(path=path))
            await conn.w.drain()
            await conn.r.drain_stderr()
            remaining = nar_size
            while remaining > 0:
                chunk = await conn.r.readexactly(min(remaining, _CHUNK_SIZE))
                remaining -= len(chunk)
                if shadow is not None:
                    shadow.theirs.update(chunk)
                yield chunk
            finished = True
        finally:
            if not finished:
                conn.dirty = True
        if shadow is not None:
            await shadow.dump()
            shadow.compare(nar_size)
