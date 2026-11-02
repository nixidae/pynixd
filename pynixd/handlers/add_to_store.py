"""Handler for AddToStore (op 7)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import structlog

from nix_daemon_protocol.add_to_store import AddToStoreResponse

from ..daemon_extensions.sign_path_info import SignPathInfoRequest
from ..exceptions import ClosingError
from ..serde import AddToStoreRequest
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store.local_db import write_router
from ..wire import FramedReader
from ..write_router import Dump, Write
from ._base import Handler
from ._import import write_client

if TYPE_CHECKING:
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from ..connection import Connection
    from ..serde.context import RequestContext

logger = structlog.get_logger(__name__)


class AddToStoreHandler(Handler):
    """Server handler for AddToStore: pynixd adds the dump on its route, else the daemon does."""

    op: ClassVar[int] = 7

    async def handle(self, ctx: RequestContext) -> AddToStoreResponse | None:
        """Decode the request, then add the framed dump in pynixd or forward it to the daemon."""
        logger.debug("received_op")
        req = await AddToStoreRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        # `daemon.cc:414`, before `startWork`: repair is not atomic, and
        # the upstream connection is root's, so the daemon would allow it.
        if req.repair and ctx.role < Role.ADMIN:
            raise ClosingError("you are not privileged to repair paths")

        dump = Dump(req.path_name, str(req.cam), frozenset(str(ref) for ref in req.references))

        async def sign(info: ValidPathInfo, conn: Connection | None) -> ValidPathInfo:
            # Nix signs build outputs alone; an untrusted client's path carries no
            # attestation of pynixd's key (nixidae/nixidae#1009).
            if not (ctx.proxy.local_store.settings.sign_added_paths and ctx.role >= Role.ADMIN):
                return info
            signed = await ctx.proxy.local_store.sign_path_info(
                SignPathInfoRequest(info=info), client=ctx.proxy.client, conn=conn
            )
            signed_info: ValidPathInfo = signed.info
            return signed_info

        frames = FramedReader(ctx.proxy.r)
        try:
            info = await write_router(ctx.proxy.local_store).add_to_store_from_dump(
                Write(op=req.name, version=ctx.proxy.version, repair=bool(req.repair), dump=dump),
                frames,
                client=write_client(ctx),
                sign=sign,
            )
        finally:
            # The `FramedSource` destructor (`daemon.cc:420`): the rest of the dump, so the session goes on.
            await frames.ensure_eof()
        return AddToStoreResponse(info=info)
