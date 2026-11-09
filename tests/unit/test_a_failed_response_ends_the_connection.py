"""A response that fails to encode partway ends the connection.

The client has read part of the response, so an error frame after it puts the
stream out of step, and the client waits on bytes that never come. Nix ends
such a session: an error after `stopWork` finds `errorAllowed` false at
`daemon.cc:1218`, and the outer catch at `daemon.cc:1232` returns. Issue
nixidae/nixidae#1212.
"""

from __future__ import annotations

import pytest

from tests.unit.loop_proxy import LoopProxy


class _HalfWritten:
    """A response whose encoder raises, as `write_uint64` does on `None`."""

    async def to_writer(self, ctx: object) -> None:
        raise TypeError("required argument is not an integer")


@pytest.mark.anyio
async def test_a_response_that_fails_to_encode_ends_the_loop() -> None:
    proxy = LoopProxy([1, 1], response=_HalfWritten())

    await proxy.run()

    assert proxy.errors == ["TypeError: required argument is not an integer"]
    assert proxy.dispatched == [1]
    assert proxy.r.reads == 1


@pytest.mark.anyio
async def test_a_failure_before_the_response_keeps_the_loop_going() -> None:
    """The store's refusal comes before any byte of a response, so the error frame is in step."""
    proxy = LoopProxy([1, 1], fail=True)

    await proxy.run()

    assert proxy.errors == ["RuntimeError: the store refused"] * 2
