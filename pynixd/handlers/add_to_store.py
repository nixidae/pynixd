"""Handler for AddToStore (op 7)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import structlog

from nix_daemon_protocol.add_to_store import AddToStoreResponse

from .. import metrics
from ..daemon_extensions.sign_path_info import SignPathInfoRequest
from ..exceptions import ClosingError
from ..serde import AddToStoreRequest
from ..serde.auth import Role
from ..serde.context import ReadContext, WriteContext
from ..wire import forward_framed
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext

logger = structlog.get_logger(__name__)


class AddToStoreHandler(Handler):
    """Server handler for AddToStore — streaming with NAR forwarding."""

    op: ClassVar[int] = 7

    async def handle(self, ctx: RequestContext) -> AddToStoreResponse | None:
        """Decode AddToStore request, stream framed NAR to daemon, sign path info, cache result."""
        logger.debug("received_op")
        # **The connection that adds the path carries the options of the
        # client.** `LocalStore::addToStore` of Nix checks the signature of
        # each path against `trusted-public-keys`, and that setting reaches
        # the daemon through `SetOptions` alone. A transfer connection with no
        # options made the daemon read its own keys, so `nix copy --from` with
        # `--trusted-public-keys` was refused with "cannot add path ...
        # because it lacks a signature by a trusted key" for a path the cache
        # had signed correctly. `require-sigs` and `secret-key-files` travel
        # the same way. Issues nixidae/nixidae#236 and nixidae/nixidae#231.
        options = ctx.proxy.client.options if ctx.proxy.client is not None else None
        async with ctx.proxy.local_store.transfer_conn(options) as conn:
            await conn.apply_options(options)
            # 1. Read request header from client (serde)
            req = await AddToStoreRequest.from_reader(
                ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
            )
            # `daemon.cc:414`, before `startWork`: repair is not atomic, and
            # the upstream connection is root's, so the daemon would allow it.
            if req.repair and ctx.role < Role.ADMIN:
                raise ClosingError("you are not privileged to repair paths")

            # 2. Write request header to daemon
            await req.to_writer(WriteContext.from_conn(conn))
            await conn.w.drain()

            # 3. Forward framed NAR bytes from client to daemon
            meter = metrics.TransferMeter(
                enabled=ctx.proxy.metrics_enabled,
                byte_counter=metrics.NAR_ADD_BYTES,
                path_counter=metrics.NAR_ADD_PATHS,
                duration=metrics.NAR_ADD_DURATION,
            )
            await forward_framed(ctx.proxy.r, conn.w, on_bytes=meter.on_bytes)
            meter.finish()

            # 4. Read response from daemon
            resp = await AddToStoreResponse.from_reader(
                ReadContext.from_conn(conn),
            )

            # 5. Sign the path info over the transfer connection, idle now that
            # the NAR and the response crossed it. A sign through the store
            # acquires a second pooled connection for the same work, and the
            # client's options ride along: without them the sign asks for a
            # connection with no options, and the pool discards the idle
            # connection that carries this client's set (`pool.py:196`), so
            # every AddToStore pays a fresh upstream handshake.
            #
            # The cache update below stays whether signing runs or not: it
            # holds what the daemon answered, signed by whoever signs.
            if resp.info is not None:
                if ctx.proxy.local_store.settings.sign_added_paths:
                    sign_resp = await ctx.proxy.local_store.sign_path_info(
                        SignPathInfoRequest(info=resp.info),
                        client=ctx.proxy.client,
                        conn=conn,
                    )
                    resp.info = sign_resp.info

                ctx.proxy.local_store.add_path_info(resp.info)

        return resp
