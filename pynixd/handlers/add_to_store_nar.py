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
from ..op_routes import OpRoute
from ..serde.auth import Role
from ..serde.context import ReadContext, WriteContext
from ..store.add import FrameReader, add_nar
from ..store.local_db import LocalDBStore
from ..wire import FramedReader, forward_framed
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
        if isinstance(store, LocalDBStore) and store.routes.get(self.op) is OpRoute.pynixd:
            reason = await store.why_the_daemon_imports(
                repair=bool(req.repair), version=ctx.proxy.version
            ) or store.why_the_daemon_imports_path(req.info)
            if reason is None:
                return await self._import(ctx, store, req)
            logger.info("add_to_store_nar_fallthrough", path=str(req.info.path), reason=reason)
            metrics.OP_FALLTHROUGH.labels(store_id=store.store_id, op=req.name).inc()
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

        on_bytes = meter.on_bytes

        async def next_chunk() -> bytes:
            chunk = await frames.next_chunk()
            if on_bytes is not None:
                on_bytes(len(chunk))
            return chunk

        try:
            settings = await store.import_settings()
            if not req.dont_check_sigs and settings.require_sigs:
                await check_path_signatures(ctx, store, req.info, settings)
            await add_nar(
                store, req.info, FrameReader(next_chunk), repair=bool(req.repair), ignored_acls=settings.ignored_acls
            )
        finally:
            # The `FramedSource` destructor (`daemon.cc:946`): the rest of the NAR, so the session goes on.
            await frames.ensure_eof()
            store.forget_path_info(req.info.path)
        meter.finish()
        return AddToStoreNarResponse()

    async def _forward(self, ctx: RequestContext, req: AddToStoreNarRequest) -> AddToStoreNarResponse:
        # **The connection that adds the path carries the options of the
        # client.** `LocalStore::addToStore` of Nix checks the signature of
        # each path against `trusted-public-keys`, and that setting reaches
        # the daemon through `SetOptions` alone. A transfer connection with no
        # options made the daemon read its own keys, so `nix copy --from` with
        # `--trusted-public-keys` was refused with "cannot add path ...
        # because it lacks a signature by a trusted key" for a path the cache
        # had signed correctly. `secret-key-files` travels the same way;
        # `require-sigs` does not, since the daemon builds its store config
        # before a client connects (`daemon.cc:489`). Issues
        # nixidae/nixidae#236 and nixidae/nixidae#231.
        options = ctx.proxy.client.options if ctx.proxy.client is not None else None
        async with ctx.proxy.local_store.transfer_conn(options) as conn:
            await conn.apply_options(options)
            await req.to_writer(WriteContext.from_conn(conn))
            await conn.w.drain()
            meter = metrics.TransferMeter(
                enabled=ctx.proxy.metrics_enabled,
                byte_counter=metrics.NAR_ADD_BYTES,
                path_counter=metrics.NAR_ADD_PATHS,
                duration=metrics.NAR_ADD_DURATION,
            )
            await forward_framed(ctx.proxy.r, conn.w, on_bytes=meter.on_bytes)
            meter.finish()
            return await AddToStoreNarResponse.from_reader(
                ReadContext.from_conn(conn),
            )
