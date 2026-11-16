"""Handler for PynixdGhaCacheWait (op 114)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, ClassVar

from ..daemon_extensions.pynixd_gha_cache_wait import PynixdGhaCacheWaitRequest, PynixdGhaCacheWaitResponse
from ..serde.auth import Role
from ..store import is_gha_cache
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class PynixdGhaCacheWaitHandler(Handler):
    """Admin-only: a job's last step, so a user's session cannot hold the daemon's queue."""

    op: ClassVar[int] = 114

    async def handle(self, ctx: RequestContext) -> object | None:
        await PynixdGhaCacheWaitRequest.from_reader(ctx.proxy.read_context)
        if ctx.role < Role.ADMIN:
            await ctx.proxy.send_error("Operation 'PynixdGhaCacheWait' requires administrative privileges.")
            return None
        counts: dict[str, object] = {}
        for store_id, store in ctx.proxy.stores.items():
            if is_gha_cache(store):
                counts[str(store_id)] = json.loads((await store.wait_for_uploads()).as_json())
        return PynixdGhaCacheWaitResponse(payload=json.dumps(counts))
