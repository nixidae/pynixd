"""Handler for AddMultipleToStore (op 44)."""

from __future__ import annotations

import contextlib
from dataclasses import replace
from typing import TYPE_CHECKING, ClassVar

import anyio
import structlog
from anyio.lowlevel import checkpoint

from nix_daemon_protocol.add_multiple_to_store import (
    AddMultipleToStoreRequest,
    AddMultipleToStoreResponse,
)
from nix_daemon_protocol.valid_path_info import ValidPathInfo

from .. import metrics
from ..exceptions import ClosingError
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store.daemon_writes import DaemonWrites
from ..store.local_db import LocalDBStore
from ..store.source import Bounded, Metered
from ..wire import FramedReader
from ..write_router import Write
from ._base import Handler
from ._import import check_path_signatures, forward_one_path

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from ..serde.context import RequestContext
    from ..store.add import ImportSettings
    from ..store.source import Source

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

        store = ctx.proxy.local_store
        write = Write(op=req.name, version=ctx.proxy.version, repair=bool(req.repair))
        if isinstance(store, LocalDBStore) and await store.router.engine_takes(write):
            return await self._import(ctx, store, req, write)
        return await self._forward(ctx, req)

    async def _import(
        self, ctx: RequestContext, store: LocalDBStore, req: AddMultipleToStoreRequest, write: Write
    ) -> AddMultipleToStoreResponse:
        """The loop of `daemon.cc:505`: `addToStore` per path, in stream order.

        The first error ends the op after `startWork`, so the session goes on,
        and the paths before it stay valid.
        """
        frames = FramedReader(ctx.proxy.r)
        meter = metrics.TransferMeter(
            enabled=ctx.proxy.metrics_enabled,
            byte_counter=metrics.NAR_ADD_BYTES,
            path_counter=metrics.NAR_ADD_PATHS,
            duration=metrics.NAR_ADD_DURATION,
        )
        on_bytes = meter.on_bytes
        try:
            settings = await store.import_settings()
            expected = await frames.read_uint64()
            for _ in range(expected):
                await checkpoint()
                info = await ValidPathInfo.from_reader(ReadContext(reader=frames, version=1))
                # `daemon.cc:515`: for every client, not only an untrusted one.
                info = info.model_copy(update={"info": info.info.model_copy(update={"ultimate": False})})
                try:
                    await self._import_one(
                        ctx,
                        store,
                        settings,
                        replace(write, info=info),
                        frames,
                        dont_check_sigs=bool(req.dont_check_sigs),
                    )
                finally:
                    store.forget_path_info(info.path)
                if on_bytes is not None:
                    on_bytes(info.info.nar_size)
                meter.finish()
        finally:
            # The `FramedSource` destructor (`daemon.cc:507`): the rest of the stream, so the session goes on.
            await frames.ensure_eof()
        return AddMultipleToStoreResponse()

    async def _import_one(
        self,
        ctx: RequestContext,
        store: LocalDBStore,
        settings: ImportSettings,
        write: Write,
        frames: FramedReader,
        *,
        dont_check_sigs: bool,
    ) -> None:
        info = write.info
        if info is None:
            raise ValueError("a path of AddMultipleToStore carries its info")
        if not await store.router.engine_takes(write):
            await ctx.proxy.add_temp_root(info.path)
            await forward_one_path(ctx, store, info, frames, dont_check_sigs=dont_check_sigs)
            return
        if not dont_check_sigs and settings.require_sigs:
            await check_path_signatures(ctx, store, info, settings)
        # The session holds the path, as `addToStore` roots it (`local-store.cc:1052`). nixidae/nixidae#1007.
        await ctx.proxy.add_temp_root(info.path)
        # A frame holds the end of one NAR and the start of the next path's info.
        nar = Bounded(frames, info.info.nar_size)
        await store.engine.add_to_store(info, nar, repair=False, ignored_acls=settings.ignored_acls)
        # `EnsureRead::finish` (`daemon.cc:564`): a valid path's NAR is skipped unread.
        await nar.finish()

    async def _forward(self, ctx: RequestContext, req: AddMultipleToStoreRequest) -> AddMultipleToStoreResponse:
        frames = FramedReader(ctx.proxy.r)
        count = await frames.read_uint64()
        logger.debug("add_multiple_forward_start", expected=count)
        try:
            async with contextlib.aclosing(_paths(frames, count, metered=ctx.proxy.metrics_enabled)) as paths:
                infos = await DaemonWrites(ctx.proxy.local_store).add_multiple_to_store(
                    count,
                    paths,
                    repair=bool(req.repair),
                    dont_check_sigs=bool(req.dont_check_sigs),
                    options=ctx.proxy.client.options if ctx.proxy.client is not None else None,
                )
        finally:
            # The `FramedSource` destructor (`daemon.cc:507`): the rest of the
            # stream, also after the daemon's error, so the session goes on.
            try:
                with anyio.fail_after(10):
                    await frames.ensure_eof()
            except TimeoutError:
                if ctx.proxy.metrics_enabled:
                    metrics.NAR_FORWARD_EOF_TIMEOUTS.inc()
                logger.warning("add_multiple_forward_source_eof_timeout", count=count)
        # The session holds each path, as Nix's worker does for its client. nixidae/nixidae#1007.
        for info in infos:
            await ctx.proxy.add_temp_root(info.path)
        return AddMultipleToStoreResponse()


async def _paths(frames: FramedReader, count: int, *, metered: bool) -> AsyncGenerator[tuple[ValidPathInfo, Source]]:
    """The paths of a client's AddMultipleToStore stream, each NAR `Bounded` and read to its end before the next info."""
    meter = metrics.TransferMeter(
        enabled=metered,
        byte_counter=metrics.NAR_FORWARD_BYTES,
        path_counter=metrics.NAR_FORWARD_PATHS,
        duration=metrics.NAR_FORWARD_DURATION,
    )
    for _ in range(count):
        info = await ValidPathInfo.from_reader(ReadContext(reader=frames, version=1))
        nar = Bounded(frames, info.info.nar_size)
        yield info, Metered(nar, meter.on_bytes)
        await nar.finish()
        meter.finish()
