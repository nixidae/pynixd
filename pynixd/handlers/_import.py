"""What the handlers of ops 7, 39 and 44 share: the session as a write sees it."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from nix_daemon_protocol.query_valid_paths import QueryValidPathsRequest

from ..serde.auth import Role
from ..store.local_db import LocalDBStore
from ..store.transfer import stream_paths_store_to_store
from ..write_router import WriteClient

if TYPE_CHECKING:
    from collections.abc import Container, Iterable, Mapping

    from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
    from pynixd_store.store_path import StorePath

    from ..serde.context import RequestContext
    from ..store.daemon import DaemonStore

logger = structlog.get_logger(__name__)


def write_client(ctx: RequestContext) -> WriteClient:
    """The client's options, its connection, its temporary roots and its metrics switch."""

    async def root(path: str) -> None:
        await ctx.proxy.add_temp_root(path)

    client = ctx.proxy.client
    return WriteClient(
        options=client.options if client is not None else None,
        conn=client,
        root=root,
        metered=ctx.proxy.metrics_enabled,
        reader=client.sync_reader if client is not None else None,
    )


async def pull_backend_references(ctx: RequestContext, references: Iterable[StorePath], sent: Container[str]) -> None:
    """Bring into the local store each reference that only a backend holds, before the add that needs it.

    pynixd answers "valid" for a path its backend holds (`PynixdContext.backend_holds`)
    even after the local store lost it, so a client skips that path and sends only its
    referrers. The local store refuses a referrer while the reference is
    missing there. nixidae/nixidae#1351. *sent*: paths that need no pull, as
    the client sends them itself.

    Any client may cause a pull, as any client may already read these paths
    through `NarFromPath`.
    """
    by_store: dict[str, tuple[DaemonStore | LocalDBStore, set[StorePath]]] = {}
    for ref in references:
        if str(ref) in sent or (store := ctx.proxy.store_for_output_path(str(ref))) is None:
            continue
        by_store.setdefault(store.store_id, (store, set()))[1].add(ref)
    if not by_store:
        return
    # A backend opens after pynixd starts serving: a client may copy before it can answer.
    if (scheduler := ctx.proxy.scheduler) is not None and scheduler.stores_opened is not None:
        await scheduler.stores_opened.wait()
    # Rooted before the check, so the local GC cannot take one in between.
    for _, refs in by_store.values():
        for ref in refs:
            await ctx.proxy.add_temp_root(ref)
    held = await ctx.proxy.local_store.execute(
        QueryValidPathsRequest(paths={ref for _, refs in by_store.values() for ref in refs}), client=ctx.proxy.client
    )
    for store, refs in by_store.values():
        if missing := refs - set(held.paths):
            logger.info("pull_backend_references", store_id=store.store_id, paths=sorted(map(str, missing)))
            await stream_paths_store_to_store(
                store, ctx.proxy.local_store, missing, peer_store_id=store.store_id, direction="in"
            )


async def sign_admins_paths(ctx: RequestContext, sent: Mapping[str, UnkeyedValidPathInfo | None]) -> bool:
    """Sign what an admin added to pynixd's own store (`LocalDBStore.sign_added`); whether this store signs.

    *sent*: each path and the info the admin sent, `None` for a content-addressed add.
    """
    store = ctx.proxy.local_store
    if ctx.role < Role.ADMIN or not isinstance(store, LocalDBStore):
        return False
    await store.sign_added(sent)
    return True
