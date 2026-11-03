"""The base of every pynixd error."""

from __future__ import annotations

from typing import Self


class PynixdError(Exception):
    """Base for all pynixd errors.

    `traces` are Nix's `addTrace` lines, outermost first. The daemon sends
    them with the error, and the client prints each above the message.
    """

    traces: tuple[str, ...] = ()

    def with_trace(self, hint: str) -> Self:
        """`addTrace`: *hint* goes in front of the traces already there."""
        self.traces = (hint, *self.traces)
        return self
