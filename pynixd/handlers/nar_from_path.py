"""Handler for NarFromPath (op 38)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import anyio
import structlog

from nix_daemon_protocol.logs import WireLogs
from nix_daemon_protocol.nar_from_path import NarFromPathRequest

from .. import metrics, wire
from ..exceptions import ClosingError, error_text
from ..op_routes import OpRoute
from ..serde import QueryPathInfoRequest
from ..serde.context import ReadContext, WriteContext
from ..store.local_db import LocalDBStore
from ..store.nar_reader import NarShadow, nar_route
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext
    from ..store.daemon import DaemonStore

logger = structlog.get_logger(__name__)


class NarFromPathHandler(Handler):
    """Server handler for NarFromPath — streaming NAR data to client."""

    op: ClassVar[int] = 38

    async def handle(self, ctx: RequestContext) -> None:
        """Decode NarFromPath request, query path info, stream unframed NAR bytes from daemon to client."""
        # 1. Read request from client (serde)
        req = await NarFromPathRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )

        # 2. Query path info to determine nar_size
        store: DaemonStore | LocalDBStore = ctx.proxy.local_store
        info_resp = await store.execute(QueryPathInfoRequest(path=req.path))
        if (not info_resp.valid or info_resp.info is None) and (
            mapped_store := ctx.proxy.store_for_output_path(str(req.path))
        ) is not None:
            store = mapped_store
            info_resp = await store.execute(QueryPathInfoRequest(path=req.path))
        if not info_resp.valid or info_resp.info is None:
            # NIX-DEVIATION (#27): `narFromPath` at `daemon.cc:918` throws
            # after `stopWork`, so Nix writes `STDERR_LAST`, then the error,
            # and closes the session; the client reads the error as NAR bytes.
            # pynixd sends the error alone and closes, so the client reads
            # "path ... is not valid". An answer of nothing leaves the client
            # waiting forever. Reverse it only to match Nix's bytes exactly.
            logger.warning("nar_not_found", path=req.path)
            raise ClosingError(f"path '{req.path}' is not valid")

        nar_size = info_resp.info.nar_size
        logger.debug("nar_from_path_streaming", path=req.path, size=nar_size)

        meter = metrics.TransferMeter(
            enabled=ctx.proxy.metrics_enabled,
            byte_counter=metrics.NAR_SERVE_BYTES,
            path_counter=metrics.NAR_SERVE_PATHS,
            duration=metrics.NAR_SERVE_DURATION,
        )
        route = nar_route(store)
        if isinstance(store, LocalDBStore) and route is OpRoute.pynixd:
            await self._dump(ctx, store, req, meter)
            return
        shadow = NarShadow(store, req.path) if isinstance(store, LocalDBStore) and route is OpRoute.shadow else None

        # 3. Forward request to daemon (serde), stream NAR to client
        failure: Exception | None = None
        async with anyio.create_task_group() as tg, store.transfer_conn() as conn:
            if shadow is not None and nar_size > 0:
                tg.start_soon(shadow.dump)
            await conn.send_request(req)
            await conn.w.drain()

            # 4. Read stderr logs from daemon
            logs = await WireLogs.from_reader(ReadContext.from_conn(conn))

            # 5. Write logs to client
            await ctx.proxy.client.flush()
            await logs.to_writer(
                WriteContext(writer=ctx.proxy.w, version=ctx.version, features=ctx.proxy.standard_features)
            )

            # 6. Stream unframed NAR bytes from daemon to client. A failure
            # here comes after `STDERR_LAST`, so the client reads the NAR and
            # nothing else: the session ends, as Nix's does when
            # `narFromPath` throws after `stopWork` (`daemon.cc:918`).
            # nixidae/nixidae#1008.
            try:
                if nar_size > 0:
                    await wire.forward_raw(
                        conn.r,
                        ctx.proxy.w,
                        nar_size,
                        on_bytes=meter.on_bytes,
                        on_chunk=shadow.theirs.update if shadow is not None else None,
                    )
                else:
                    # No nar_size to count against, and the parse reports no
                    # lengths, so these bytes stay off the byte counter. The
                    # path and the duration still count in `finish` below.
                    # The shadow needs the size, so it sits this one out.
                    await wire.stream_parse_nar(conn.r, ctx.proxy.w)
            except Exception as exc:
                # Raised outside the group, which would wrap it in an ExceptionGroup.
                failure = exc
                tg.cancel_scope.cancel()
        if failure is not None:
            raise ClosingError(f"pynixd: the NAR of '{req.path}' broke off: {error_text(failure)}") from failure

        await ctx.proxy.w.drain()
        meter.finish()
        if shadow is not None and nar_size > 0:
            shadow.compare(nar_size)
        logger.debug("responded_op")
        return

    async def _dump(
        self, ctx: RequestContext, store: LocalDBStore, req: NarFromPathRequest, meter: metrics.TransferMeter
    ) -> None:
        """`STDERR_LAST`, then the NAR dumped from the store's files, as `daemon.cc:966` sends them.

        A failure after `STDERR_LAST` ends the session, as on the daemon route.
        """
        await ctx.proxy.client.flush()
        await WireLogs().to_writer(
            WriteContext(writer=ctx.proxy.w, version=ctx.version, features=ctx.proxy.standard_features)
        )
        count = meter.on_bytes
        try:
            async for piece in store.nar_pieces(req.path):
                ctx.proxy.w.write(piece)
                await ctx.proxy.w.drain()
                if count is not None:
                    count(len(piece))
        except Exception as exc:
            raise ClosingError(f"pynixd: the NAR of '{req.path}' broke off: {error_text(exc)}") from exc
        await ctx.proxy.w.drain()
        meter.finish()
