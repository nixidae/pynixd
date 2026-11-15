"""What a fake `SubstitutionQueue` shares: the answer of `substituters_for`."""

from __future__ import annotations

from typing import Any

from pynixd.goals.query_missing import client_names_a_substituter


class ServesAsBackends:
    """A fake queue that stands for pynixd's backends, beside a daemon configured with `uncovered`."""

    uncovered: tuple[str, ...] = ()

    async def substituters_for(self, client: Any) -> tuple[object, ...] | None:
        """The daemon for a client naming no backend's substituter or an uncovered one; else this fake, as pynixd's backends."""
        options = getattr(client, "options", None)
        if options is not None and not options.use_substitutes:
            return ()
        return None if client_names_a_substituter(client) or self.uncovered else (self,)
