"""Handler for IsValidPath (op 1)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import IsValidPathRequest
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class IsValidPathHandler(Handler):
    """IsValidPath: `DaemonProxy.is_valid_path` answers it. A recursive-nix session never comes here."""

    op: ClassVar[int] = IsValidPathRequest.op

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the path and answer it."""
        return await ctx.proxy.is_valid_path(await IsValidPathRequest.from_reader(ctx.proxy.read_context))
