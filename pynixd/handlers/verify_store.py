"""Handler for VerifyStore (op 35)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import VerifyStoreRequest
from ..serde.auth import Role
from ..serde.context import ReadContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class VerifyStoreHandler(Handler):
    """Server handler for VerifyStore. Nix refuses only a repair to an untrusted client."""

    op: ClassVar[int] = 35

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request, refuse an untrusted repair as `daemon.cc:871` does, and forward."""
        req = await VerifyStoreRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )

        if req.repair and ctx.role < Role.ADMIN:
            await ctx.proxy.send_error("you are not privileged to repair paths")
            return None

        response: object | None = await ctx.proxy.local_store.call(req)
        return response
