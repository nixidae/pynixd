"""Handler for AddPermRoot (op 47)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..exceptions import ClosingError
from ..serde import AddPermRootRequest
from ..serde.auth import Role
from ..store.local_db import write_router
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class AddPermRootHandler(Handler):
    """Server handler for AddPermRoot. Nix refuses it to an untrusted client."""

    op: ClassVar[int] = 47

    async def handle(self, ctx: RequestContext) -> object | None:
        """Add the root for a trusted client; refuse and close for any other, as `daemon.cc:680` does."""
        if ctx.role < Role.ADMIN:
            raise ClosingError(
                "you are not privileged to create perm roots\n\n"
                "hint: you can just do this client-side without special privileges, "
                "and probably want to do that instead."
            )
        req = await AddPermRootRequest.from_reader(
            ctx.proxy.read_context,
        )
        return await write_router(ctx.proxy.local_store).add_perm_root(
            req.store_path, req.gc_root, hold=ctx.proxy.add_temp_root
        )
