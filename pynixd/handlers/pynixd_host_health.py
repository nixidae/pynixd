"""Handlers for PynixdHostHealth (op 112) and PynixdHostHealthStream (op 113): this host, for a pynixd peer."""

from __future__ import annotations

import contextlib
import dataclasses
import json
from typing import TYPE_CHECKING, ClassVar

import anyio

from ..background import Background
from ..daemon_extensions.pynixd_host_health import (
    HEALTH_JSON,
    PynixdHostHealthRequest,
    PynixdHostHealthResponse,
    PynixdHostHealthStreamRequest,
)
from ..monitor import SAMPLED_FILES, DummyResourceMonitor, cores_from, create_monitor, local_disk, local_read
from ..psi import SystemHealth
from ..serde import LogNext
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


class PynixdHostHealthStreamHandler(Handler):
    """Sends each sample of this host's monitor while the session lasts. Admin-only.

    The samples are the local store's, so a PSI trigger that wakes its agent
    reaches the controller at once. With monitoring off here, or sampling
    slower than the controller asks, the session runs a poller of its own.
    nixidae/nixidae#1016.
    """

    op: ClassVar[int] = 113

    async def handle(self, ctx: RequestContext) -> object | None:
        """Forward samples until the controller closes the session."""
        request = await PynixdHostHealthStreamRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        if ctx.role < Role.ADMIN:
            await ctx.proxy.send_error("Operation 'PynixdHostHealthStream' requires administrative privileges.")
            return None
        local = ctx.proxy.local_store
        send, receive = anyio.create_memory_object_stream[SystemHealth](8)

        def listen(health: SystemHealth) -> None:
            # A controller that stops reading loses samples, not memory.
            with contextlib.suppress(anyio.WouldBlock):
                send.send_nowait(health)

        interval = request.interval_ms / 1000
        monitor = local.monitor
        background = Background("PynixdHostHealthStream")
        async with background.running():
            if monitor is None or isinstance(monitor, DummyResourceMonitor) or monitor.interval > interval:
                own = create_monitor(local.pressure_settings.model_copy(update={"interval": interval}), listen)
                own.disk_fn = local_disk(local.layout.real_store_dir)
                own.start(background)
            else:
                local.sample_listeners.add(listen)
                listen(monitor.health)
            try:
                async with receive:
                    async for health in receive:
                        await ctx.proxy.client.send(LogNext(text=HEALTH_JSON.dump_json(health).decode()))
            finally:
                local.sample_listeners.discard(listen)
        return None
