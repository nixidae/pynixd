"""Handler for AddIndirectRoot (op 12)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import AddIndirectRootRequest
from ..serde.context import ReadContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class AddIndirectRootHandler(Handler):
    """Server handler for AddIndirectRoot. Nix allows it to every client, `daemon.cc:694`."""

    op: ClassVar[int] = 12

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request and forward it to the daemon."""
        req = await AddIndirectRootRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        response: object | None = await ctx.proxy.local_store.call(req)
        return response
