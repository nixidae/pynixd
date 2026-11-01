"""Handler for FindRoots (op 14)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from nix_daemon_protocol.find_roots import FindRootsEntry, FindRootsRequest, FindRootsResponse

from ..serde.auth import Role
from ._base import Handler

if TYPE_CHECKING:
    from ..serde.context import RequestContext

CENSORED = "{censored}"


def censor(roots: list[FindRootsEntry]) -> list[FindRootsEntry]:
    """What `LocalStore::findRoots(censor=true)` answers, from the uncensored roots.

    `src/libstore/gc.cc:207,343` replace every temporary and runtime root of a
    path with one `{censored}`. A temporary root is `{temp:<pid>}`, and a
    runtime root is `{...}` or a link under `/proc`. A root in `gcroots` keeps
    its link.
    """
    kept: list[FindRootsEntry] = []
    hidden: set[str] = set()
    for entry in roots:
        if entry.link.startswith(("{", "/proc/")):
            if entry.target not in hidden:
                hidden.add(entry.target)
                kept.append(FindRootsEntry(link=CENSORED, target=entry.target))
        else:
            kept.append(entry)
    return kept


class FindRootsHandler(Handler):
    """Server handler for FindRoots. The store answers uncensored, so pynixd censors."""

    op: ClassVar[int] = 14

    async def handle(self, ctx: RequestContext) -> object | None:
        """Ask the daemon, and censor the answer for an untrusted client, as `daemon.cc:717` does."""
        resp: FindRootsResponse = await ctx.proxy.local_store.execute(FindRootsRequest(), client=ctx.proxy.client)
        if ctx.role < Role.ADMIN:
            resp = resp.model_copy(update={"roots": censor(resp.roots)})
        return resp
