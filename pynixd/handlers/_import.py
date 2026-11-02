"""What AddToStoreNar (op 39) and AddMultipleToStore (op 44) share on the `pynixd` route."""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.add_to_store_nar import AddToStoreNarRequest, AddToStoreNarResponse
from nix_daemon_protocol.logs import LogNext

from ..hintfmt import magenta
from ..serde.context import ReadContext, WriteContext
from ..signing import check_signatures, key_settings, public_keys
from ..store.add import AddError
from ..wire import FramedWriter, forward_raw

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
    trusted, secret_files = key_settings(
        settings.trusted_public_keys,
        settings.secret_key_files,
        options.overrides if options is not None else {},
    )
    warnings: list[str] = []
    try:
        good = check_signatures(info, public_keys(trusted, secret_files), str(store.layout.store_dir), warnings.append)
    except ValueError as error:
        raise AddError(str(error)) from error
    for warning in warnings:
        await ctx.proxy.client.send(LogNext(text=f"{warning}\n"))
    if not good:
        raise AddError(f"cannot add path '{magenta(info.path)}' because it lacks a signature by a trusted key")


async def forward_one_path(
    ctx: RequestContext, store: LocalDBStore, info: ValidPathInfo, frames: FramedReader, *, dont_check_sigs: bool
) -> None:
    """One path of a shared stream to the daemon, as AddToStoreNar: its `narSize` bytes, framed.

    The daemon's error raises here, after `startWork`, and its log lines reach the client as they come.
    """
    options = ctx.proxy.client.options if ctx.proxy.client is not None else None
    request = AddToStoreNarRequest(info=info, repair=0, dont_check_sigs=int(dont_check_sigs))
    async with store.transfer_conn(options) as conn:
        await conn.apply_options(options)
        await request.to_writer(WriteContext.from_conn(conn))
        framed = FramedWriter(conn.w)
        await forward_raw(frames, framed, info.info.nar_size)
        await framed.finalize()
        await AddToStoreNarResponse.from_reader(ReadContext.from_conn(conn, client=ctx.proxy.client))
