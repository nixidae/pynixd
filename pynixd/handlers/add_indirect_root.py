"""Handler for AddIndirectRoot (op 12)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import AddIndirectRootRequest
from ..store.local_db import write_router
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class AddIndirectRootHandler(Handler):
    """Server handler for AddIndirectRoot. Nix allows it to every client, `daemon.cc:694`."""

    op: ClassVar[int] = 12

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request; the store's write router adds the root."""
        req = await AddIndirectRootRequest.from_reader(
            ctx.proxy.read_context,
        )
        return await write_router(ctx.proxy.local_store).add_indirect_root(req.path)
