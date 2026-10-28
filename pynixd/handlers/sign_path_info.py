"""Handler for SignPathInfo (op 107)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import SignPathInfoRequest
from ..serde.auth import Role
from ..serde.context import ReadContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class SignPathInfoHandler(Handler):
    """Server handler for SignPathInfo — admin-only."""

    op: ClassVar[int] = 107

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode SignPathInfo request, verify admin auth, execute via daemon, return response."""
        req = await SignPathInfoRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )

        if ctx.role < Role.ADMIN:
            await ctx.proxy.send_error(
                "Operation 'SignPathInfo' requires administrative privileges.",
            )
            return None

        response: object | None = await ctx.proxy.local_store.call(req)
        return response
