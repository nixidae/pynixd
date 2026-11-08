"""Handler for AddMultipleToStore (op 44)."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING, ClassVar

import anyio
import structlog

from nix_daemon_protocol.add_multiple_to_store import (
    AddMultipleToStoreRequest,
    AddMultipleToStoreResponse,
)
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd_store.source import Bounded

from .. import metrics
from ..exceptions import ClosingError
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store.local_db import write_router
from ..wire import FramedReader
from ..write_router import Write
from ._base import Handler
from ._import import write_client

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from pynixd_store.source import Source

    from ..serde.context import RequestContext

logger = structlog.get_logger(__name__)


class AddMultipleToStoreHandler(Handler):
    """Server handler for AddMultipleToStore: pynixd imports each path on its route, else the daemon does."""

    op: ClassVar[int] = 44

    async def handle(self, ctx: RequestContext) -> AddMultipleToStoreResponse | None:
        """Decode the header, apply the untrusted-client rules, and import or forward the framed paths."""
        structlog.contextvars.bind_contextvars(operation=type(self).__name__)
        req = await AddMultipleToStoreRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        # `daemon.cc:500`, before `startWork`: repair is not atomic.
        if req.repair and ctx.role < Role.ADMIN:
            raise ClosingError("you are not privileged to repair store paths")
        # `daemon.cc:502`. The connection is root's, so this is pynixd's to do.
        if ctx.role < Role.ADMIN:
            req = req.model_copy(update={"dont_check_sigs": 0})

        frames = FramedReader(ctx.proxy.r)
        try:
            count = await frames.read_uint64()
            async with contextlib.aclosing(_paths(frames, count)) as paths:
                await write_router(ctx.proxy.local_store).add_multiple_to_store(
                    Write(op=req.name, repair=bool(req.repair) or ctx.proxy.repairs_adds),
                    count,
                    paths,
                    dont_check_sigs=bool(req.dont_check_sigs),
                    client=write_client(ctx),
                )
        except Exception:
            # The `FramedSource` destructor (`daemon.cc:507`): the rest of the
            # stream, however long, so the session goes on after the error.
            await frames.ensure_eof()
            raise
        # Every path is read, and only the end frame is left.
        try:
            with anyio.fail_after(10):
                await frames.ensure_eof()
        except TimeoutError:
            if ctx.proxy.metrics_enabled:
                metrics.NAR_FORWARD_EOF_TIMEOUTS.inc()
            logger.warning("add_multiple_forward_source_eof_timeout", count=count)
        return AddMultipleToStoreResponse()


async def _paths(frames: FramedReader, count: int) -> AsyncGenerator[tuple[ValidPathInfo, Source]]:
    """The paths of a client's AddMultipleToStore stream, each NAR `Bounded`.

    A frame holds the end of one NAR and the start of the next path's info, so
    each NAR is read to its end before the next info: `EnsureRead::finish`
    (`daemon.cc:564`) skips a valid path's NAR unread.
    """
    for _ in range(count):
        info = await ValidPathInfo.from_reader(ReadContext(reader=frames, version=1))
        nar = Bounded(frames, info.info.nar_size)
        yield info, nar
        await nar.finish()
