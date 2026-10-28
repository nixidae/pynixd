"""Handler for AddTempRoots (op 49)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from ..serde import AddTempRootsRequest, AddTempRootsResponse
from ..serde.context import ReadContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class AddTempRootsHandler(Handler):
    """Server handler for AddTempRoots — pynixd holds each root itself.

    The batch form of AddTempRoot (op 11): `copyPaths` of Nix pins the
    destination set with it before every copy, so without this handler a
    new client silently holds no roots on pynixd at all — the feature gate
    has no fallback for old daemons. Like op 11 the roots belong to the
    client session, so each path goes through `DaemonProxy.add_temp_root`
    and no backend is involved. Issue #66.
    """

    op: ClassVar[int] = 49

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode the path set, hold each for this session, and report success."""
        req = await AddTempRootsRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )
        for path in req.paths:
            await ctx.proxy.add_temp_root(path)
        return AddTempRootsResponse.fast(value=1)  # type: ignore[return-value]  # the base returns object | None
