"""What AddToStoreNar (op 39) and AddMultipleToStore (op 44) share on the `pynixd` route."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.logs import LogNext

from ..store.daemon_writes import DaemonWrites
from ..store.source import Bounded

if TYPE_CHECKING:
    from nix_daemon_protocol.valid_path_info import ValidPathInfo

    from ..serde.context import RequestContext
    from ..store.add import ImportSettings
    from ..store.local_db import LocalDBStore
    from ..wire import FramedReader


async def check_path_signatures(
    ctx: RequestContext, store: LocalDBStore, info: ValidPathInfo, settings: ImportSettings
) -> None:
    """`pathInfoIsUntrusted` and its refusal (`local-store.cc:1048`), with the client's key overrides."""
    options = ctx.proxy.client.options if ctx.proxy.client is not None else None
    warnings: list[str] = []
    try:
        store.engine.check_signatures(info, settings, options.overrides if options is not None else {}, warnings)
    finally:
        # Nix prints each warning as it checks, before any refusal.
        for warning in warnings:
            await ctx.proxy.client.send(LogNext(text=f"{warning}\n"))


async def forward_one_path(
    ctx: RequestContext, store: LocalDBStore, info: ValidPathInfo, frames: FramedReader, *, dont_check_sigs: bool
) -> None:
    """One path of a shared stream to the daemon, as AddToStoreNar: its `narSize` bytes, framed.

    The daemon's error raises here, after `startWork`, and its log lines reach the client as they come.
    """
    nar = Bounded(frames, info.info.nar_size)
    await DaemonWrites(store).add_to_store(
        info,
        nar,
        dont_check_sigs=dont_check_sigs,
        options=ctx.proxy.client.options if ctx.proxy.client is not None else None,
        logs=ctx.proxy.client,
    )
    await nar.finish()
