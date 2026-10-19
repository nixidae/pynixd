"""Handler for SetOptions (op 19)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from nix_daemon_protocol.logs import LogNext

from ..serde import SetOptionsRequest, SetOptionsResponse
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..serde.protocol import Verbosity
from ..trust import restrict_overrides, warning_text
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class SetOptionsHandler(Handler):
    """Server handler for SetOptions. An untrusted client keeps what Nix lets it keep."""

    op: ClassVar[int] = 19

    async def handle(self, ctx: RequestContext) -> object | None:
        """Keep the options of this session, and answer the client.

        **The options belong to the session, and not to one connection.**
        This sent the request over the pool, so it reached whichever
        connection was free, and every later operation of the client reached
        another one. A client that set `--post-build-hook` then saw the hook
        run for three of the five derivations that its request built, because
        pynixd built the five on several connections. Issue nixidae/nixidae#231.

        `Connection.apply_options` now sends the set on the connection that is
        about to do the work, so nothing goes upstream here.
        """
        req = await SetOptionsRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )

        resp = SetOptionsResponse()
        if ctx.role < Role.ADMIN:
            # Every upstream connection is root's, so nix-daemon filters
            # nothing on it. `daemon.cc:788` filters for an untrusted client.
            overrides, warnings = restrict_overrides(req.overrides, ctx.proxy.ctx.trust)
            req = req.model_copy(update={"overrides": overrides})
            # `TunnelLogger::log` drops a message above the client's
            # verbosity, and `apply` sets that verbosity first.
            if req.verbosity >= Verbosity.WARN:
                for warning in warnings:
                    resp.logs.add(LogNext(text=warning_text(warning)))

        ctx.proxy.client.options = req
        return resp
