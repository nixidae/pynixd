"""A build into a store with no daemon holds its input closure there before the copy, as `copyPaths` does.

`Executor.hold_inputs_on` does the same on a builder over its build connection
(`src/libstore/store-api.cc`, issue #887). A store with no daemon has no such
connection, so pynixd writes the roots into the store's own `temproots`.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue
from pynixd.exceptions import BackendError
from tests._conftest.execution import FakeLocal, FakeRoots, FakeSandbox, Streamed, fake_executor, queued
from tests.unit.test_builder_holds_inputs import _info

INPUT = "/nix/store/00000000000000000000000000000001-input"
DEPENDENCY = "/nix/store/00000000000000000000000000000002-dependency"


async def _send(roots: FakeRoots) -> list[set[str]]:
    """The roots that held as each copy started."""
    queue = BuildQueue()
    build = await queued(queue, input_srcs=[INPUT])
    held_at_copy: list[set[str]] = []
    stream = Streamed()
    stream.before = lambda: held_at_copy.append(set(roots.paths))
    executor = fake_executor(queue, {}, local=FakeLocal([_info(INPUT), _info(DEPENDENCY)]), stream=stream)
    await executor.send_inputs(build, FakeSandbox(store_id=StoreId("builder")), roots)
    return held_at_copy


async def test_the_closure_is_held_before_the_copy() -> None:
    assert await _send(FakeRoots()) == [{INPUT, DEPENDENCY}]


async def test_a_store_that_cannot_hold_a_root_copies_nothing() -> None:
    with pytest.raises(BackendError, match="store builder cannot hold"):
        await _send(FakeRoots(holds=False))
