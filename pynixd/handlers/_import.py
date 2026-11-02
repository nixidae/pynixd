"""What the handlers of ops 7, 39 and 44 share: the session as a write sees it."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..write_router import WriteClient

if TYPE_CHECKING:
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
    )
