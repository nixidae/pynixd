"""Handler for PynixdRootsReport (op 110)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pynixd_store.liveness import walk_labeled_roots

from ..daemon_extensions.pynixd_roots_report import (
    PynixdRootsReportRequest,
    PynixdRootsReportResponse,
    RootsReportRow,
)
from ..serde.auth import Role
from ..serde.context import ReadContext
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext


class PynixdRootsReportHandler(Handler):
    """Server handler for PynixdRootsReport — admin-only."""

    op: ClassVar[int] = 110

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode PynixdRootsReport request, verify admin auth, attribute storage."""
        await PynixdRootsReportRequest.from_reader(
            ReadContext(reader=ctx.proxy.r, version=ctx.proxy.version, features=ctx.proxy.standard_features),
        )

        if ctx.role < Role.ADMIN:
            await ctx.proxy.send_error(
                "Operation 'PynixdRootsReport' requires administrative privileges.",
            )
            return None

        store = ctx.proxy.ctx.local_store
        layout = getattr(store, "layout", None)
        db = getattr(store, "db", None)
        if layout is None or db is None:
            return PynixdRootsReportResponse(rows=[])
        labeled = walk_labeled_roots(layout.state_dir, str(layout.store_dir))
        report = await db.query_roots_report([(label, sorted(seeds)) for label, seeds in labeled])
        if report is None:
            return PynixdRootsReportResponse(rows=[])
        return PynixdRootsReportResponse(
            rows=[
                RootsReportRow(
                    label=row.label,
                    full_paths=row.full_paths,
                    full_bytes=row.full_bytes,
                    exclusive_paths=row.exclusive_paths,
                    exclusive_bytes=row.exclusive_bytes,
                )
                for row in report
            ],
        )
