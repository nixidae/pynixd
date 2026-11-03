"""Handler for VerifyStore (op 35)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import VerifyStoreRequest
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store.local_db import write_router
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class VerifyStoreHandler(Handler):
    """Server handler for VerifyStore. Nix refuses only a repair to an untrusted client (`daemon.cc:896`)."""

    op: ClassVar[int] = 35

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request, refuse an untrusted repair as `daemon.cc:896` does, then route it."""
        req = await VerifyStoreRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )

        if req.repair and ctx.role < Role.ADMIN:
            await ctx.proxy.send_error("you are not privileged to repair paths")
            return None

        return await write_router(ctx.proxy.local_store).verify_store(
            check_contents=bool(req.check_contents),
            repair=bool(req.repair),
            conn=ctx.proxy.client,
        )
