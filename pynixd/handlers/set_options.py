"""Handler for SetOptions (op 19)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import structlog

from nix_daemon_protocol.logs import LogNext

from ..constants import FEATURE_SET_OPTIONS_BY_NAME
from ..serde import SetOptionsRequest, SetOptionsResponse
from ..serde.protocol import Verbosity
from ..session_options import session_options
from ..trust import warning_text
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext

logger = structlog.get_logger(__name__)


class SetOptionsHandler(Handler):
    """Server handler for SetOptions. `session_options` decides what a session keeps."""

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
            ctx.proxy.read_context,
        )
        client = ctx.proxy.client
        pynixd = ctx.proxy.ctx
        kept = session_options(
            req,
            role=ctx.role,
            remote=not client.on_this_machine and FEATURE_SET_OPTIONS_BY_NAME in client.standard_features,
            policy=pynixd.trust,
            remote_settings=frozenset(pynixd.settings.remote_settings),
            daemon=pynixd.daemon_header,
        )
        if kept.dropped:
            logger.debug("remote_settings_dropped", names=kept.dropped)

        resp = SetOptionsResponse()
        # `TunnelLogger::log` drops a message above the client's verbosity, and `apply` sets that verbosity first.
        if req.verbosity >= Verbosity.WARN:
            for warning in kept.warnings:
                resp.logs.add(LogNext(text=warning_text(warning)))

        # A `SetOptions` value wins over one on the SSH command.
        client.pynixd_options = {**client.pynixd_options, **kept.pynixd_options}
        if kept.options is None:
            return resp
        # `ClientSettings::apply` sets the names it is given and leaves every
        # other one as an earlier SetOptions set it (`daemon.cc:221-300`).
        options = kept.options
        if client.options is not None:
            options = options.model_copy(update={"overrides": {**client.options.overrides, **options.overrides}})
        client.options = options
        return resp
