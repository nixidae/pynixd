"""Handler for NarFromPath (op 38)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import structlog

from nix_daemon_protocol.logs import WireLogs
from nix_daemon_protocol.nar_from_path import NarFromPathRequest

from .. import metrics, wire
from ..exceptions import ClosingError
from ..serde import QueryPathInfoRequest
from ..serde.context import ReadContext, WriteContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext

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
        store = ctx.proxy.local_store
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

        # 3. Forward request to daemon (serde), stream NAR to client
        meter = metrics.TransferMeter(
            enabled=ctx.proxy.metrics_enabled,
            byte_counter=metrics.NAR_SERVE_BYTES,
            path_counter=metrics.NAR_SERVE_PATHS,
            duration=metrics.NAR_SERVE_DURATION,
        )
        async with store.transfer_conn() as conn:
            await req.to_writer(WriteContext.from_conn(conn))
            await conn.w.drain()

            # 4. Read stderr logs from daemon
            logs = await WireLogs.from_reader(ReadContext.from_conn(conn))

            # 5. Write logs to client
            await ctx.proxy.client.flush()
            await logs.to_writer(
                WriteContext(writer=ctx.proxy.w, version=ctx.version, features=ctx.proxy.standard_features)
            )

            # 6. Stream unframed NAR bytes from daemon to client
            if nar_size > 0:
                await wire.forward_raw(conn.r, ctx.proxy.w, nar_size, on_bytes=meter.on_bytes)
            else:
                # No nar_size to count against, and the parse reports no
                # lengths, so these bytes stay off the byte counter. The
                # path and the duration still count in `finish` below.
                await wire.stream_parse_nar(conn.r, ctx.proxy.w)

        await ctx.proxy.w.drain()
        meter.finish()
        logger.debug("responded_op")
        return
