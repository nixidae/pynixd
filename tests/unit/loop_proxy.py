"""Enough of `DaemonProxy` for `op_loop` to run, shared by its tests.

`op_loop` is called unbound (`DaemonProxy.op_loop(proxy)`), so this needs
every attribute the loop touches: `r`, `w`, `client`, `dispatch`,
`send_error`, `version`, `standard_features`, `write_context`, `_op_timing`, `_op_metrics`
and `_metrics_enabled`. When the
loop grows a new attribute, this is the one place to add it, and every
loop test fails together instead of one fake drifting silently.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from pynixd.proxy import DaemonProxy

if TYPE_CHECKING:
    from collections.abc import Sequence


class LoopReader:
    """A reader that answers a list of operation codes, then end of file."""

    def __init__(self, ops: Sequence[int]) -> None:
        self.remaining = list(ops)
        self.reads = 0

    async def read_uint64(self) -> int:
        self.reads += 1
        if not self.remaining:
            raise EOFError
        return self.remaining.pop(0)


class LoopProxy:
    """A loop test drives this, and reads what the loop did back."""

    def __init__(
        self, ops: Sequence[int], *, fail: bool = False, metrics_enabled: bool = True, response: Any = None
    ) -> None:
        self.r = LoopReader(ops)
        self.w = SimpleNamespace(drain=self._nothing)
        self.version = None
        self.standard_features: frozenset[str] = frozenset()
        self.write_context = SimpleNamespace(writer=self.w, version=self.version, features=self.standard_features)
        self._response = response
        self.client = SimpleNamespace(flush=self._nothing)
        self.errors: list[str] = []
        self.dispatched: list[int] = []
        self._op_timing: dict[int, tuple[int, float]] = {}
        self._op_metrics: dict[tuple[str, str], object] = {}
        self._metrics_enabled = metrics_enabled
        self._fail = fail

    async def _nothing(self) -> None:
        return None

    async def send_error(self, text: str, traces: Sequence[str] = ()) -> None:
        self.errors.append(text)

    async def dispatch(self, op_num: int) -> Any:
        if self._fail:
            raise RuntimeError("the store refused")
        self.dispatched.append(op_num)
        return self._response

    async def run(self) -> None:
        await DaemonProxy.op_loop(cast("DaemonProxy", self))
