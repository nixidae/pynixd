"""Handler for CollectGarbage (op 20)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pynixd_store.collect import in_nix_test

from ..constants import FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS
from ..exceptions import ClosingError
from ..serde import CollectGarbageRequest
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store.local_db import write_router
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class CollectGarbageHandler(Handler):
    """Server handler for CollectGarbage. Nix allows it to every client, `daemon.cc:735`."""

    op: ClassVar[int] = 20

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request, refuse what Nix refuses, then route it."""
        req = await CollectGarbageRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        # The client and the daemon negotiate the feature apart, so either
        # side may lack it. Nix 2.35's own refusal, before `startWork`
        # (`daemon.cc:766`). Not a forward to a 2.34 daemon: its `DELETE_DEAD`
        # ignores the paths and deletes every dead path (`gc.cc:698`).
        if req.needs_the_feature() and not (
            FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS in ctx.proxy.standard_features
            and FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS in ctx.proxy.local_store.features
        ):
            raise ClosingError(
                "Garbage collecting specific paths requested but it is not supported by the negotiated protocol"
            )
        req = req.with_both_shapes()

        # An idle connection keeps a worker of the daemon alive, and that
        # worker holds a temporary root for each path that it took. The
        # collector reads those roots and frees nothing. `nix-daemon` has no
        # such connection, because the client that made the root is gone.
        await ctx.proxy.local_store.retire_idle_connections()
        # After `startWork`: upstream refuses every client (`daemon.cc:773`), Determinate all but its own suite's.
        if req.ignore_liveness and not in_nix_test():
            await ctx.proxy.send_error("you are not allowed to ignore liveness")
            return None
        return await write_router(ctx.proxy.local_store).collect_garbage(
            req, conn=ctx.proxy.client, censor=ctx.role < Role.ADMIN
        )
