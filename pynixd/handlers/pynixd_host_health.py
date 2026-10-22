"""Handler for PynixdHostHealth (op 112): one sample of this host, for a pynixd peer."""

from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING, ClassVar

from ..daemon_extensions.pynixd_host_health import PynixdHostHealthRequest, PynixdHostHealthResponse
from ..monitor import SAMPLED_FILES, cores_from, local_disk, local_read
from ..serde.auth import Role
from ..serde.context import ReadContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class PynixdHostHealthHandler(Handler):
    """Answers the files a resource monitor reads, the cores, and the store's disk. Admin-only."""

    op: ClassVar[int] = 112

    async def handle(self, ctx: RequestContext) -> object | None:
        """Read one sample of this host and answer it as JSON."""
        await PynixdHostHealthRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        if ctx.role < Role.ADMIN:
            await ctx.proxy.send_error("Operation 'PynixdHostHealth' requires administrative privileges.")
            return None
        files = {path: await local_read(path) for path in SAMPLED_FILES}
        disk = await local_disk(ctx.proxy.local_store.layout.real_store_dir)()
        payload = {
            "files": files,
            "cores": await cores_from(local_read),
            "disk": dataclasses.asdict(disk) if disk is not None else None,
        }
        return PynixdHostHealthResponse(version=1, payload=json.dumps(payload))
