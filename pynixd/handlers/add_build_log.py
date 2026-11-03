"""Handler for AddBuildLog (op 45)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import AddBuildLogRequest
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store.local_db import write_router
from ..wire import FramedReader
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class AddBuildLogHandler(Handler):
    """Server handler for AddBuildLog — admin only (`daemon.cc:1015`)."""

    op: ClassVar[int] = 45

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the header, refuse a non-admin, then the write router records the log.

        The trust check comes before the framed body, as Nix throws before it
        drains the `FramedSource` (`daemon.cc:1015`). The body then stays
        unread, and the client drops the connection, as it does against Nix.
        """
        req = await AddBuildLogRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        if ctx.role < Role.ADMIN:
            await ctx.proxy.send_error("you are not privileged to add logs")
            return None

        # Nix drains the log whole into a `StringSink` before it writes
        # (`daemon.cc:1019`), so the router takes the bytes, not a stream.
        framed = FramedReader(ctx.proxy.r)
        pieces: list[bytes] = []
        while piece := await framed.next_chunk():
            pieces.append(piece)
        return await write_router(ctx.proxy.local_store).add_build_log(req.path, b"".join(pieces))
