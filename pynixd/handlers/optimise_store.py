"""Handler for OptimiseStore (op 34)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import OptimiseStoreRequest
from ..serde.context import ReadContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class OptimiseStoreHandler(Handler):
    """Server handler for OptimiseStore. Nix allows it to every client, `daemon.cc:860`."""

    op: ClassVar[int] = 34

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request and forward it to the daemon."""
        req = await OptimiseStoreRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )

        response: object | None = await ctx.proxy.local_store.call(req)
        return response
