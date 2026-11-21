"""Handler for AddToStoreNar (op 39)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import structlog

from nix_daemon_protocol.add_to_store_nar import (
    AddToStoreNarRequest,
    AddToStoreNarResponse,
)

from ..exceptions import ClosingError
from ..serde.auth import Role
from ..store.local_db import write_router
from ..wire import FramedReader
from ..write_router import Write
from ._base import Handler
from ._import import pull_backend_references, sign_admins_paths, write_client

if TYPE_CHECKING:
    from ..serde.context import RequestContext

logger = structlog.get_logger(__name__)


class AddToStoreNarHandler(Handler):
    """Server handler for AddToStoreNar: pynixd imports the NAR on its route, else the daemon does."""

    op: ClassVar[int] = 39

    async def handle(self, ctx: RequestContext) -> AddToStoreNarResponse | None:
        """Decode the request, apply the untrusted-client rules, and import or forward the framed NAR."""
        structlog.contextvars.bind_contextvars(operation=type(self).__name__)
        req = await AddToStoreNarRequest.from_reader(
            ctx.proxy.read_context,
        )
        # `daemon.cc:936`, before `startWork`: repair is not atomic.
        if req.repair and ctx.role < Role.ADMIN:
            raise ClosingError("you are not privileged to repair store paths")
        # `daemon.cc:938-941`. The connection is root's, so this is pynixd's to do.
        grant = ctx.proxy.grant
        if grant is not None:
            grant.check_add(str(req.info.path))
            # The output of the build central asked for: nobody signed it, and central trusts it as built.
            req = req.model_copy(update={"dont_check_sigs": 1})
        elif ctx.role < Role.ADMIN:
            req = req.model_copy(
                update={"dont_check_sigs": 0, "info": req.info.model_copy(update={"ultimate": False})},
            )

        # The session holds the path, not the pooled worker: `addToStore`
        # roots it for the client first (`local-store.cc:1052`), and a
        # pooled worker's roots end when `CollectGarbage` retires it.
        # nixidae/nixidae#1007.
        await ctx.proxy.add_temp_root(req.info.path)

        frames = FramedReader(ctx.proxy.r)
        try:
            await pull_backend_references(ctx, req.info.info.references, {str(req.info.path)})
            await write_router(ctx.proxy.local_store).add_to_store(
                Write(
                    op=req.name,
                    repair=bool(req.repair) or ctx.proxy.repairs_adds,
                    info=req.info,
                ),
                frames,
                dont_check_sigs=bool(req.dont_check_sigs),
                client=write_client(ctx),
            )
        finally:
            # The `FramedSource` destructor (`daemon.cc:946`): the rest of the NAR, so the session goes on.
            await frames.ensure_eof()
        await sign_admins_paths(ctx, {str(req.info.path): req.info.info})
        return AddToStoreNarResponse()
