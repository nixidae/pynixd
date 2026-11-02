"""Handler for AddToStoreNar (op 39)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import structlog

from nix_daemon_protocol.add_to_store_nar import (
    AddToStoreNarRequest,
    AddToStoreNarResponse,
)

from .. import metrics
from ..exceptions import ClosingError
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store.daemon_writes import DaemonWrites
from ..store.local_db import LocalDBStore
from ..store.source import Metered
from ..wire import FramedReader
from ..write_router import Write
from ._base import Handler
from ._import import check_path_signatures

if TYPE_CHECKING:
    from ..serde.context import RequestContext

logger = structlog.get_logger(__name__)


class AddToStoreNarHandler(Handler):
    """Server handler for AddToStoreNar: pynixd imports the NAR on its route, else the daemon does."""

    op: ClassVar[int] = 39

    async def handle(self, ctx: RequestContext) -> AddToStoreNarResponse | None:
        """Decode the request, apply the untrusted-client rules, and import or forward the framed NAR."""
        structlog.contextvars.bind_contextvars(operation=type(self).__name__)
        req = await AddToStoreNarRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        # `daemon.cc:936`, before `startWork`: repair is not atomic.
        if req.repair and ctx.role < Role.ADMIN:
            raise ClosingError("you are not privileged to repair store paths")
        # `daemon.cc:938-941`. The connection is root's, so this is pynixd's to do.
        if ctx.role < Role.ADMIN:
            req = req.model_copy(
                update={"dont_check_sigs": 0, "info": req.info.model_copy(update={"ultimate": False})},
            )

        # The session holds the path, not the pooled worker: `addToStore`
        # roots it for the client first (`local-store.cc:1052`), and a
        # pooled worker's roots end when `CollectGarbage` retires it.
        # nixidae/nixidae#1007.
        await ctx.proxy.add_temp_root(req.info.path)

        store = ctx.proxy.local_store
        write = Write(op=req.name, version=ctx.proxy.version, repair=bool(req.repair), info=req.info)
        if isinstance(store, LocalDBStore) and await store.router.engine_takes(write):
            return await self._import(ctx, store, req)
        return await self._forward(ctx, req)

    async def _import(
        self, ctx: RequestContext, store: LocalDBStore, req: AddToStoreNarRequest
    ) -> AddToStoreNarResponse:
        """`LocalStore::addToStore` in pynixd. Every error comes after `startWork`, so the session goes on."""
        frames = FramedReader(ctx.proxy.r)
        meter = metrics.TransferMeter(
            enabled=ctx.proxy.metrics_enabled,
            byte_counter=metrics.NAR_ADD_BYTES,
            path_counter=metrics.NAR_ADD_PATHS,
            duration=metrics.NAR_ADD_DURATION,
        )

        try:
            settings = await store.import_settings()
            if not req.dont_check_sigs and settings.require_sigs:
                await check_path_signatures(ctx, store, req.info, settings)
            await store.engine.add_to_store(
                req.info,
                Metered(frames, meter.on_bytes),
                repair=bool(req.repair),
                ignored_acls=settings.ignored_acls,
            )
        finally:
            # The `FramedSource` destructor (`daemon.cc:946`): the rest of the NAR, so the session goes on.
            await frames.ensure_eof()
            store.forget_path_info(req.info.path)
        meter.finish()
        return AddToStoreNarResponse()

    async def _forward(self, ctx: RequestContext, req: AddToStoreNarRequest) -> AddToStoreNarResponse:
        meter = metrics.TransferMeter(
            enabled=ctx.proxy.metrics_enabled,
            byte_counter=metrics.NAR_ADD_BYTES,
            path_counter=metrics.NAR_ADD_PATHS,
            duration=metrics.NAR_ADD_DURATION,
        )
        await DaemonWrites(ctx.proxy.local_store).add_to_store(
            req.info,
            FramedReader(ctx.proxy.r),
            repair=bool(req.repair),
            dont_check_sigs=bool(req.dont_check_sigs),
            options=ctx.proxy.client.options if ctx.proxy.client is not None else None,
            on_bytes=meter.on_bytes,
        )
        meter.finish()
        return AddToStoreNarResponse()
