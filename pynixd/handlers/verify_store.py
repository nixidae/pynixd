"""Handler for VerifyStore (op 35)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pynixd_store.store_path import StorePath

from ..serde import VerifyStoreRequest
from ..serde.auth import Role
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
            ctx.proxy.read_context,
        )

        if req.repair and ctx.role < Role.ADMIN:
            await ctx.proxy.send_error("you are not privileged to repair paths")
            return None

        client = ctx.proxy.client

        async def repair(path: str) -> None:
            await ctx.proxy.goal_engine.repair_path(StorePath(path), client)

        repair_path = repair if req.repair and await ctx.proxy.goal_engine.repairs_here(client) else None
        return await write_router(ctx.proxy.local_store).verify_store(
            check_contents=bool(req.check_contents),
            repair=bool(req.repair),
            conn=client,
            repair_path=repair_path,
        )
