"""Handler for AddSignatures (op 37)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import AddSignaturesRequest
from ..serde.context import ReadContext
from ..store.local_db import write_router
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class AddSignaturesHandler(Handler):
    """Server handler for AddSignatures. Nix allows it to every client, `daemon.cc:904`."""

    op: ClassVar[int] = 37

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request; the store's write router merges the signatures."""
        req = await AddSignaturesRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        return await write_router(ctx.proxy.local_store).add_signatures(str(req.path), req.sigs)
