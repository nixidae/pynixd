"""Handler for AddToStore (op 7)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import structlog

from nix_daemon_protocol.add_to_store import AddToStoreResponse
from nix_daemon_protocol.query_path_info import QueryPathInfoRequest

from .. import metrics
from ..content_address import parse_method_algo
from ..daemon_extensions.sign_path_info import SignPathInfoRequest
from ..exceptions import ClosingError
from ..serde import AddToStoreRequest, StorePath
from ..serde.auth import Role
from ..serde.context import ReadContext, WriteContext
from ..store.local_db import LocalDBStore
from ..store.source import Metered
from ..wire import FramedReader, forward_framed
from ..write_router import Dump, Write
from ._base import Handler

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

        store = ctx.proxy.local_store
        dump = Dump(req.path_name, str(req.cam), frozenset(str(ref) for ref in req.references))
        write = Write(op=req.name, version=ctx.proxy.version, repair=bool(req.repair), dump=dump)
        if isinstance(store, LocalDBStore) and await store.router.engine_takes(write):
            return await self._add(ctx, store, req)
        return await self._forward(ctx, req)

    def _meter(self, ctx: RequestContext) -> metrics.TransferMeter:
        return metrics.TransferMeter(
            enabled=ctx.proxy.metrics_enabled,
            byte_counter=metrics.NAR_ADD_BYTES,
            path_counter=metrics.NAR_ADD_PATHS,
            duration=metrics.NAR_ADD_DURATION,
        )

    async def _add(self, ctx: RequestContext, store: LocalDBStore, req: AddToStoreRequest) -> AddToStoreResponse:
        """`addToStoreFromDump` in pynixd. Every error comes after `startWork`, so the session goes on."""
        frames = FramedReader(ctx.proxy.r)
        meter = self._meter(ctx)

        async def root(path: str) -> None:
            # The session holds the path, as Nix's worker does for its client (`local-store.cc:1264`).
            await ctx.proxy.add_temp_root(path)

        try:
            settings = await store.import_settings()
            method, algo = parse_method_algo(str(req.cam))
            path = await store.engine.add_to_store_from_dump(
                req.path_name,
                method,
                algo,
                frozenset(str(ref) for ref in req.references),
                Metered(frames, meter.on_bytes),
                root=root,
                ignored_acls=settings.ignored_acls,
            )
        finally:
            # The `FramedSource` destructor (`daemon.cc:420`): the rest of the dump, so the session goes on.
            await frames.ensure_eof()
        meter.finish()

        store_path = StorePath(path=path)
        # `invalidatePathInfoCacheFor`: a path valid before this op may hold a negative entry.
        store.forget_path_info(store_path)
        answer = await store.own_query_path_info(QueryPathInfoRequest(path=store_path))
        if answer.info is None:
            raise RuntimeError(f"pynixd added '{path}', and the store database does not hold it")
        info = answer.info.to_valid_path_info(store_path)
        info = await self._sign(ctx, info)
        store.add_path_info(info)
        return AddToStoreResponse(info=info)

    async def _sign(self, ctx: RequestContext, info: ValidPathInfo, conn: Connection | None = None) -> ValidPathInfo:
        # Nix signs build outputs alone; an untrusted client's path carries no
        # attestation of pynixd's key (nixidae/nixidae#1009).
        if not (ctx.proxy.local_store.settings.sign_added_paths and ctx.role >= Role.ADMIN):
            return info
        signed = await ctx.proxy.local_store.sign_path_info(
            SignPathInfoRequest(info=info), client=ctx.proxy.client, conn=conn
        )
        signed_info: ValidPathInfo = signed.info
        return signed_info

    async def _forward(self, ctx: RequestContext, req: AddToStoreRequest) -> AddToStoreResponse:
        # **The connection that adds the path carries the options of the
        # client.** `LocalStore::addToStore` of Nix checks the signature of
        # each path against `trusted-public-keys`, and that setting reaches
        # the daemon through `SetOptions` alone. A transfer connection with no
        # options made the daemon read its own keys, so `nix copy --from` with
        # `--trusted-public-keys` was refused with "cannot add path ...
        # because it lacks a signature by a trusted key" for a path the cache
        # had signed correctly. `require-sigs` and `secret-key-files` travel
        # the same way. Issues nixidae/nixidae#236 and nixidae/nixidae#231.
        options = ctx.proxy.client.options if ctx.proxy.client is not None else None
        async with ctx.proxy.local_store.transfer_conn(options) as conn:
            await conn.apply_options(options)
            await req.to_writer(WriteContext.from_conn(conn))
            await conn.w.drain()
            meter = self._meter(ctx)
            await forward_framed(ctx.proxy.r, conn.w, on_bytes=meter.on_bytes)
            meter.finish()
            resp = await AddToStoreResponse.from_reader(ReadContext.from_conn(conn))
            # The session holds the path, as Nix's worker does for its client
            # (`local-store.cc:1264`). nixidae/nixidae#1007.
            await ctx.proxy.add_temp_root(resp.info.path)
            # The sign goes over this connection, idle now that the dump and
            # the response crossed it. A sign through the store acquires a
            # second pooled connection, and without the client's options the
            # pool discards the idle connection that carries them
            # (`pool.py:196`), so every AddToStore pays a fresh handshake.
            resp.info = await self._sign(ctx, resp.info, conn)
            ctx.proxy.local_store.add_path_info(resp.info)
        return resp
