"""Handler for AddBuildLog (op 45)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from nix_daemon_protocol.add_build_log import AddBuildLogResponse

from ..serde import AddBuildLogRequest
from ..serde.auth import Role
from ..serde.context import ReadContext, WriteContext
from ..wire import forward_framed
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class AddBuildLogHandler(Handler):
    """Server handler for AddBuildLog — admin-only."""

    op: ClassVar[int] = 45

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode AddBuildLog request, verify admin auth, execute via daemon, return response."""
        req = await AddBuildLogRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )

        if ctx.role < Role.ADMIN:
            await ctx.proxy.send_error("you are not privileged to add logs")
            return None

        # The log follows the header in frames (`daemon.cc:1019`). Forwarding
        # the header alone left them in the client stream, where the next
        # operation read them. nixidae/nixidae#1006.
        options = ctx.proxy.client.options if ctx.proxy.client is not None else None
        async with ctx.proxy.local_store.transfer_conn(options) as conn:
            await conn.apply_options(options)
            await req.to_writer(WriteContext.from_conn(conn))
            await conn.w.drain()
            await forward_framed(ctx.proxy.r, conn.w)
            return await AddBuildLogResponse.from_reader(ReadContext.from_conn(conn))
