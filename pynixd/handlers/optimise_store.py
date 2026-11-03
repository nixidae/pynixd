"""Handler for OptimiseStore (op 34)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import OptimiseStoreRequest
from ..serde.context import ReadContext
from ..store.local_db import write_router
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class OptimiseStoreHandler(Handler):
    """Server handler for OptimiseStore. Nix allows it to every client, `daemon.cc:887`."""

    op: ClassVar[int] = 34

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request; the store's write router deduplicates the store, holding each path."""
        # The request carries no body past the op code; read it for symmetry.
        await OptimiseStoreRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        return await write_router(ctx.proxy.local_store).optimise_store(
            hold=ctx.proxy.add_temp_root, conn=ctx.proxy.client
        )
