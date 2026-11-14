"""Handler for PynixdState (op 111)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import anyio
import structlog

from nix_daemon_protocol.ids import LOCAL_STORE_ID

from .. import state as state_collector
from ..daemon_extensions.pynixd_state import PynixdStateRequest, PynixdStateResponse
from ..exceptions import OpNotImplementedError
from ..serde.auth import Role
from ..state_models import FederatedError, StateReport
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext
    from ..store import Store

log = structlog.get_logger(__name__)

FEDERATED_STORE_TIMEOUT: float = 30.0
"""How long one store's answer may take. State reads are cheap; past this
the store is wedged or gone, and it is marked unreachable instead of
wedging `pynixd state` behind it. Issue #81."""


async def _query_one(
    store: Store, wants: list[str], timeout: float = FEDERATED_STORE_TIMEOUT
) -> StateReport | FederatedError:
    """One store's own sections, or why it has none to give.

    Support is decided from the handshake cache before anything is sent,
    in the codebase's established idiom (`"Name" in store.features`):
    a store that never advertised the op is marked, not probed. The
    refusal catch stays as belt-and-braces for a peer that changed
    under us. Issue #82.
    """
    if PynixdStateRequest.name not in store.features:
        return FederatedError(error="state op not advertised by this store")
    try:
        with anyio.fail_after(timeout):
            resp = await store.execute(
                PynixdStateRequest(version=1, wants=wants, federated=False),
            )
    except OpNotImplementedError:
        return FederatedError(error="state op not implemented by this store")
    except TimeoutError:
        return FederatedError(error=f"no answer in {timeout} seconds")
    except Exception as exc:
        log.warning("state_federated_store_failed", error=str(exc), exc_info=True)
        return FederatedError(error=f"{type(exc).__name__}: {exc}")
    try:
        return StateReport.model_validate_json(resp.payload)
    except (ValueError, AttributeError) as exc:
        return FederatedError(error=f"unreadable answer: {exc}")


async def _federated_sections(ctx: RequestContext, wants: list[str]) -> dict[str, StateReport | FederatedError]:
    """Every non-local store's answer, keyed by store id.

    One level only: the queries go out with `federated` false, so a store
    answers itself and no topology can loop. A task group bounds the whole
    fan-out by the slowest store, not the sum of them.
    """
    merged: dict[str, StateReport | FederatedError] = {}

    async def query(store_id: object, store: Store) -> None:
        merged[str(store_id)] = await _query_one(store, wants)

    async with anyio.create_task_group() as tg:
        for store_id, store in ctx.proxy.stores.items():
            if store_id == LOCAL_STORE_ID:
                continue
            tg.start_soon(query, store_id, store)
    return merged


class PynixdStateHandler(Handler):
    """Server handler for PynixdState — admin-only."""

    op: ClassVar[int] = 111

    async def handle(self, ctx: RequestContext) -> object | None:
        """Decode PynixdState request, verify admin auth, collect sections."""
        req = await PynixdStateRequest.from_reader(
            ctx.proxy.read_context,
        )

        if ctx.role < Role.ADMIN:
            await ctx.proxy.send_error(
                "Operation 'PynixdState' requires administrative privileges.",
            )
            return None

        report = state_collector.collect(ctx.proxy.ctx, req.wants)
        if req.federated:
            report = report.model_copy(update={"federated": await _federated_sections(ctx, req.wants)})
        return PynixdStateResponse(version=1, payload=report.model_dump_json(exclude_none=True))
