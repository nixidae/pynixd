"""What the handlers of ops 7, 39 and 44 share: the session as a write sees it."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..serde.auth import Role
from ..store.local_db import LocalDBStore
from ..write_router import WriteClient

if TYPE_CHECKING:
    from collections.abc import Mapping

    from nix_daemon_protocol.path_info import UnkeyedValidPathInfo

    from ..serde.context import RequestContext


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


async def sign_admins_paths(ctx: RequestContext, sent: Mapping[str, UnkeyedValidPathInfo | None]) -> bool:
    """Sign what an admin added to pynixd's own store (`LocalDBStore.sign_added`); whether this store signs.

    *sent*: each path and the info the admin sent, `None` for a content-addressed add.
    """
    store = ctx.proxy.local_store
    if ctx.role < Role.ADMIN or not isinstance(store, LocalDBStore):
        return False
    await store.sign_added(sent)
    return True
