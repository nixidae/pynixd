"""Handler for BuildDerivation (op 36)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import anyio
import structlog

from ..exceptions import DelegationLoopError
from ..goals.resolution import _nix_drv_name, canonical_derivation, unparse_basic_derivation
from ..serde import BuildDerivationRequest, BuildDerivationResponse
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store_path import StorePath
from ._base import Handler

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireResponse

    from ..serde.context import RequestContext

logger = structlog.get_logger(__name__)


class BuildDerivationHandler(Handler):
    """Server handler for BuildDerivation — scheduler or fallback to daemon."""

    op: ClassVar[int] = 36

    async def handle(self, ctx: RequestContext) -> WireResponse | None:
        """Decode BuildDerivation request, enqueue via scheduler or fallback to daemon, return result."""
        logger.debug("received_op")

        self_req = await BuildDerivationRequest.from_reader(ReadContext.from_request(ctx))
        if ctx.role < Role.ADMIN:
            outputs = self_req.derivation.outputs.values()
            if not (outputs and all(output.is_ca for output in outputs)):
                # `daemon.cc:634`, after `startWork`, so the session goes on.
                await ctx.proxy.send_error("you are not privileged to build input-addressed derivations")
                return None
            # `daemon.cc:642-653`: the client's path is not evidence, so Nix
            # writes the derivation and builds the path that it gets.
            try:
                canonical = canonical_derivation(self_req.derivation, _nix_drv_name(self_req.drv_path))
            except ValueError as exc:
                # `drv.type()` runs after `startWork`, so the session goes on.
                await ctx.proxy.send_error(str(exc))
                return None
            self_req = self_req.model_copy(update={"derivation": canonical})
            name = f"{_nix_drv_name(self_req.drv_path)}.drv"
            text = unparse_basic_derivation(self_req.derivation)
            references = {str(path) for path in self_req.derivation.input_srcs}
            drv_path = await ctx.proxy.local_store.add_text_to_store(name, text, references)
            self_req = self_req.model_copy(update={"drv_path": StorePath(path=drv_path)})

        if not ctx.proxy.use_scheduler_for_builds:
            logger.debug("handle_local_mode_fallback")
            result: BuildDerivationResponse = await ctx.proxy.local_store.execute(self_req, client=ctx.proxy.client)

            if result.result.status == 0:
                logger.debug("responded_op")
            return result.model_copy(
                update={"result": result.result.for_the_wire(ctx.proxy.standard_features)},
            )

        if ctx.proxy.scheduler is None:
            raise RuntimeError("BuildDerivation requires a configured scheduler")

        # The client's options reach the builder, as the build hook's SetOptions
        # does (`remote-store.cc:129`): `max-silent-time` and `timeout` apply.
        options = ctx.proxy.client.options if ctx.proxy.client is not None else None
        delegated = ctx.proxy.client is not None and ctx.proxy.client.delegated
        try:
            build_id, future = await ctx.proxy.scheduler.build_derivation(
                self_req, options=options, delegated=delegated
            )
        except DelegationLoopError as exc:
            # A `BackendError` on the delegating side, which retries elsewhere.
            await ctx.proxy.send_error(str(exc))
            return None
        subscribed = False
        if ctx.proxy.client is not None:
            subscribed = await ctx.proxy.scheduler.queue.subscribe(
                build_id,
                ctx.proxy.client,
                cancel_on_unsubscribe=True,
            )
        logger.info(
            "build_derivation_enqueued",
            build_id=build_id,
            drv_path=self_req.drv_path,
            required_count=len(self_req.derivation.input_srcs),
        )
        try:
            response = await future
            logger.debug("responded_op")
            # **The backend and this client negotiate their features apart.**
            # A backend that offers `realisation-with-path-not-hash` fills one
            # `builtOutputs` field of the result and leaves the other at
            # `None`, and this client reads whichever its own set names.
            # `for_the_wire` fills the one it will read. Issue #14.
            return response.model_copy(
                update={"result": response.result.for_the_wire(ctx.proxy.standard_features)},
            )
        finally:
            # Shielded: a hung-up client cancels this, and the last
            # unsubscribe is what ends a build nobody else wants. Issue #91.
            with anyio.CancelScope(shield=True):
                if subscribed and ctx.proxy.client is not None:
                    await ctx.proxy.scheduler.queue.unsubscribe(build_id, ctx.proxy.client)
                # The request answered and this client left the stream: a finished
                # build nobody else streams can go, log buffer and all.
                await ctx.proxy.scheduler.queue.prune()
