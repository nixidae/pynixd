"""Handler for AddMultipleToStore (op 44)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import anyio
import structlog
from anyio.lowlevel import checkpoint

from nix_daemon_protocol.add_multiple_to_store import (
    AddMultipleToStoreRequest,
    AddMultipleToStoreResponse,
)
from nix_daemon_protocol.valid_path_info import ValidPathInfo

from .. import metrics
from ..exceptions import ClosingError
from ..serde.auth import Role
from ..serde.context import ReadContext, WriteContext
from ..store.transfer import as_copied
from ..wire import FramedReader, FramedWriter, NixReader, NixWriter, forward_raw
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext

logger = structlog.get_logger(__name__)


class AddMultipleToStoreHandler(Handler):
    """Server handler for AddMultipleToStore — streaming with framed NAR forwarding."""

    op: ClassVar[int] = 44

    async def handle(self, ctx: RequestContext) -> AddMultipleToStoreResponse | None:
        """Decode AddMultipleToStore request, stream framed NAR payloads to daemon, cache path infos, return response."""
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
            # 1. Read request header from client (serde)
            req = await AddMultipleToStoreRequest.from_reader(
                ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
            )
            # `daemon.cc:500`, before `startWork`: repair is not atomic.
            if req.repair and ctx.role < Role.ADMIN:
                raise ClosingError("you are not privileged to repair store paths")
            # `daemon.cc:507`. The connection is root's, so this is pynixd's to do.
            if ctx.role < Role.ADMIN:
                req = req.model_copy(update={"dont_check_sigs": 0})

            # 2. Write request header to daemon
            await req.to_writer(WriteContext.from_conn(conn))
            await conn.w.drain()

            # 3. Concurrently: forward payload + read daemon response
            # An anyio task group hands back no task object, so the child
            # records its result in this list. The group waits for the child
            # on exit, which is what `await resp_task` did before.
            responses: list[AddMultipleToStoreResponse] = []
            # Same reason as `responses`: an anyio task group is typed as able
            # to swallow what its body raised, so a name the body binds is not
            # bound for certain after the block.
            infos: list[ValidPathInfo] = []

            async def _read_response() -> None:
                responses.append(
                    await AddMultipleToStoreResponse.from_reader(
                        ReadContext.from_conn(conn),
                    ),
                )

            async with anyio.create_task_group() as tg:
                tg.start_soon(_read_response)
                infos.extend(await self._forward_stream(ctx.proxy.r, conn.w, metered=ctx.proxy.metrics_enabled))

            if not responses:
                raise RuntimeError("the AddMultipleToStore reader task recorded no response")
            resp = responses[0]

            # Update path info cache
            ctx.proxy.local_store.add_path_infos([as_copied(info) for info in infos])
            # The session holds each path, as Nix's worker does for its
            # client. nixidae/nixidae#1007.
            for info in infos:
                await ctx.proxy.add_temp_root(info.path)

            return resp

    async def _forward_stream(
        self,
        src: NixReader,
        dst: NixWriter,
        *,
        metered: bool,
    ) -> list[ValidPathInfo]:
        """Forward AddMultipleToStore payload, snooping ValidPathInfos.

        Payload structure after the header:
            [count:uint64][path_info_bytes + nar_bytes]...[0-size terminator]
        """
        fsrc = FramedReader(src)
        fdst = FramedWriter(dst)

        expected = await fsrc.read_uint64()
        fdst.write_uint64(expected)
        logger.debug("add_multiple_forward_start", expected=expected)

        meter = metrics.TransferMeter(
            enabled=metered,
            byte_counter=metrics.NAR_FORWARD_BYTES,
            path_counter=metrics.NAR_FORWARD_PATHS,
            duration=metrics.NAR_FORWARD_DURATION,
        )
        infos: list[ValidPathInfo] = []
        for _ in range(expected):
            # Per path as well as per chunk. A transfer of many small paths
            # spends its time in this metadata read rather than in the byte
            # loop below, and measured the worst loop stall of the three
            # shapes in tests/benchmark/test_bench_nar_profile.py.
            await checkpoint()
            info = await ValidPathInfo.from_reader(ReadContext(reader=fsrc, version=1))
            infos.append(info)
            fdst.write(await info.bytes_wire())
            # The same loop every other NAR path takes. `forward_raw` carries
            # the drain and the checkpoint, and why each is needed.
            await forward_raw(fsrc, fdst, info.info.nar_size, on_bytes=meter.on_bytes)
            meter.finish()

        await fdst.finalize()
        try:
            with anyio.fail_after(10):
                await fsrc.ensure_eof()
        except TimeoutError:
            if metered:
                metrics.NAR_FORWARD_EOF_TIMEOUTS.inc()
            logger.warning("add_multiple_forward_source_eof_timeout", count=len(infos))
        return infos
