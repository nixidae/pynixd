"""Handler for CollectGarbage (op 20)."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, ClassVar

from pynixd_store.collect import CENSORED, in_nix_test
from pynixd_store.hintfmt import ANSI_MAGENTA, ANSI_NORMAL

from ..constants import FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS
from ..exceptions import ClosingError
from ..serde import CollectGarbageRequest, CollectGarbageResponse, LogError
from ..serde.auth import Role
from ..serde.context import ReadContext
from ..store.local_db import write_router
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext

_ROOT_NAME = re.compile(
    rf"(because it's (?:referenced by the GC root|in use by) '(?:{re.escape(ANSI_MAGENTA)})?)"
    r"(\{[^']*\}|/proc/[^'\x1b]*)"
    rf"((?:{re.escape(ANSI_NORMAL)})?'\.)"
)


def censored_refusal(message: str) -> str:
    """A delegated daemon's refusal, with each runtime and temporary root as `{censored}`.

    pynixd's connection to the daemon is root's, so a Determinate v3.24.0
    daemon does not censor (`daemon.cc:807`, `censor = !trusted`), and names
    the `/proc` link or the `{nix-process:<pid>}` label (`gc.cc:762`, `:777`).
    A `gcroots` link stays, as `handlers/find_roots.censor` keeps it. Upstream
    2.35 names no root (`gc.cc:780`), so its text has nothing to match.
    """
    return _ROOT_NAME.sub(lambda m: f"{m[1]}{CENSORED}{m[3]}", message)


def censor_refusals(response: CollectGarbageResponse) -> CollectGarbageResponse:
    """`censored_refusal` on each error in *response*'s logs, in place."""
    messages = response.logs.messages
    for i, msg in enumerate(messages):
        if isinstance(msg, LogError):
            messages[i] = msg.model_copy(update={"msg": censored_refusal(msg.msg)})
    return response


class CollectGarbageHandler(Handler):
    """Server handler for CollectGarbage. Nix allows it to every client, `daemon.cc:735`."""

    op: ClassVar[int] = 20

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the request, refuse what Nix refuses, then route it."""
        req = await CollectGarbageRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        # The client and the daemon negotiate the feature apart, so either
        # side may lack it. Nix 2.35's own refusal, before `startWork`
        # (`daemon.cc:766`). Not a forward to a 2.34 daemon: its `DELETE_DEAD`
        # ignores the paths and deletes every dead path (`gc.cc:698`).
        if req.needs_the_feature() and not (
            FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS in ctx.proxy.standard_features
            and FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS in ctx.proxy.local_store.features
        ):
            raise ClosingError(
                "Garbage collecting specific paths requested but it is not supported by the negotiated protocol"
            )
        req = req.with_both_shapes()

        # An idle connection keeps a worker of the daemon alive, and that
        # worker holds a temporary root for each path that it took. The
        # collector reads those roots and frees nothing. `nix-daemon` has no
        # such connection, because the client that made the root is gone.
        await ctx.proxy.local_store.retire_idle_connections()
        # After `startWork`: upstream refuses every client (`daemon.cc:773`), Determinate all but its own suite's.
        if req.ignore_liveness and not in_nix_test():
            await ctx.proxy.send_error("you are not allowed to ignore liveness")
            return None
        untrusted = ctx.role < Role.ADMIN
        response = await write_router(ctx.proxy.local_store).collect_garbage(
            req, conn=ctx.proxy.client, censor=untrusted
        )
        return censor_refusals(response) if untrusted else response
