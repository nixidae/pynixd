"""`Background`: what leaves its `async with`, and what it refuses. nixidae/nixidae#1143."""

from __future__ import annotations

import anyio
import pytest

from pynixd.background import Background
from tests.functional.mock_store import MockStore


async def _fails() -> None:
    raise RuntimeError("loop")


@pytest.mark.anyio
async def test_an_error_of_the_body_leaves_as_itself() -> None:
    """`pynixd state` prints each socket's error, not 'unhandled errors in a TaskGroup'."""
    background = Background("test")
    with pytest.raises(RuntimeError, match="body") as raised:
        async with background.running():
            background.spawn(anyio.sleep_forever)
            raise RuntimeError("body") from OSError("cause")
    assert isinstance(raised.value.__cause__, OSError), "the unwrap lost the error's cause"


@pytest.mark.anyio
async def test_an_error_of_a_loop_leaves_in_a_group() -> None:
    background = Background("test")
    with pytest.raises(ExceptionGroup) as raised:
        async with background.running():
            background.spawn(_fails)
            await anyio.sleep_forever()
    assert raised.group_contains(RuntimeError, match="loop", depth=None)


@pytest.mark.anyio
async def test_a_cancelled_spawn_ends_that_loop_alone() -> None:
    background = Background("test")
    ended = anyio.Event()

    async def watched() -> None:
        try:
            await anyio.sleep_forever()
        finally:
            ended.set()

    async with background.running():
        background.spawn(anyio.sleep_forever)
        background.spawn(watched).cancel()
        with anyio.fail_after(5):
            await ended.wait()


@pytest.mark.anyio
async def test_a_store_opened_twice_refuses() -> None:
    """A second open would orphan the first, and its task group would end in another task."""
    store = MockStore("twice", feature_matrix={"x86_64-linux": set()})
    async with store:
        with pytest.raises(RuntimeError, match="already open"):
            await store.__aenter__()
