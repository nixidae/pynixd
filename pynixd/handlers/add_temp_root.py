"""Handler for AddTempRoot (op 11)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import AddTempRootRequest, AddTempRootResponse
from ..serde.context import ReadContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class AddTempRootHandler(Handler):
    """Server handler for AddTempRoot — pynixd writes the root itself.

    The root belongs to the client session, and it goes away when that
    session does. `DaemonProxy.add_temp_root` and `pynixd.temp_roots` hold
    the mechanism, and issue #20 gives the defect that they correct.

    The role of the client makes no difference now. The operation used to
    forward to the upstream daemon for an admin and to do nothing for anyone
    else, because only an admin may add a root through the protocol. pynixd
    writes the file, so the access that counts is its own.
    """

    op: ClassVar[int] = 11

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the path, hold it for this session, and report success."""
        req = await AddTempRootRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        await ctx.proxy.add_temp_root(req.path)
        return AddTempRootResponse.fast(value=1)  # type: ignore[return-value]  # the base returns object | None
