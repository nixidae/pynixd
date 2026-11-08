"""A build into a store with no daemon holds its input closure there before the copy, as `copyPaths` does.

`_hold_inputs_on` does the same on a builder over its build connection
(`src/libstore/store-api.cc`, issue #887). A store with no daemon has no such
connection, so pynixd writes the roots into the store's own `temproots`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import scheduler as scheduler_module
from pynixd.exceptions import BackendError
from pynixd.scheduler import Scheduler
from pynixd_store.store_path import StorePath

INPUT = StorePath("/nix/store/00000000000000000000000000000001-input")
DEPENDENCY = StorePath("/nix/store/00000000000000000000000000000002-dependency")


class Roots:
    def __init__(self, *, writable: bool = True) -> None:
        self.held: set[StorePath] = set()
        self.writable = writable

    async def add(self, path: StorePath) -> bool:
        if self.writable:
            self.held.add(path)
        return self.writable


class LocalStore:
    async def execute(self, _request: Any, **_kwargs: Any) -> Any:
        return SimpleNamespace(infos=[SimpleNamespace(path=INPUT), SimpleNamespace(path=DEPENDENCY)])


def _build() -> Any:
    return SimpleNamespace(build_id="b", request=SimpleNamespace(derivation=SimpleNamespace(input_srcs=[INPUT])))


async def _send(roots: Roots, monkeypatch: pytest.MonkeyPatch) -> list[set[StorePath]]:
    held_at_copy: list[set[StorePath]] = []

    async def copy(*_args: Any, **_kwargs: Any) -> int:
        held_at_copy.append(set(roots.held))
        return 0

    monkeypatch.setattr(scheduler_module, "stream_paths_store_to_store", copy)
    stub = SimpleNamespace(local_store=LocalStore())
    to = SimpleNamespace(store_id=StoreId("builder"))
    await Scheduler._send_inputs(cast("Any", stub), _build(), cast("Any", to), cast("Any", roots))  # noqa: SLF001 -- the order is the unit
    return held_at_copy


async def test_the_closure_is_held_before_the_copy(monkeypatch: pytest.MonkeyPatch) -> None:
    assert await _send(Roots(), monkeypatch) == [{INPUT, DEPENDENCY}]


async def test_a_store_that_cannot_hold_a_root_copies_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(BackendError, match="store builder cannot hold"):
        await _send(Roots(writable=False), monkeypatch)
