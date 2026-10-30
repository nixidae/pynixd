"""A pooled connection retires after `max_lifetime`.

`idle_ttl` retires a connection that nobody uses. This rule retires one that
everybody uses. The reason is the temporary roots of the daemon: a worker adds
a root for each path that it builds or substitutes and releases those roots
when it exits, so a connection in steady use holds every root it ever made.
Issue #20.
"""

from __future__ import annotations

import time
from typing import Any, cast

import pytest

from pynixd.background import Background
from pynixd.connection import Connection
from pynixd.store.pool import ConnectionPool


class FakeReader:
    bytes_read = 0

    async def is_dirty(self) -> bool:
        return False

    def peer_closed(self) -> bool:
        return False


class FakeWriter:
    async def is_dirty(self) -> bool:
        return False

    async def close(self) -> None:
        return None


class FakeConnection(Connection):
    """Enough of `Connection` for the rules of the pool.

    It inherits the real class so the pool takes it without a cast at each
    call. `Connection.__init__` opens nothing -- it assigns fields -- so the
    two casts here are the whole of the pretence: the pool reads `is_dirty`
    and `close` off the pair, and nothing else of a reader or a writer.
    """

    def __init__(self, conn_id: str, opened_at: float | None = None) -> None:
        super().__init__(
            cast("Any", FakeReader()),
            cast("Any", FakeWriter()),
            conn_id,
        )
        if opened_at is not None:
            self.opened_at = opened_at
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def make_pool(max_lifetime: float) -> ConnectionPool:
    async def factory() -> Connection:
        return FakeConnection("new")

    return ConnectionPool(
        store_id="test",
        factory=factory,
        background=Background("test"),
        max_lifetime=max_lifetime,
    )


def test_a_young_connection_stays() -> None:
    pool = make_pool(300.0)
    assert not pool._too_old(FakeConnection("a"), time.monotonic())


def test_a_connection_past_its_lifetime_retires() -> None:
    pool = make_pool(300.0)
    now = time.monotonic()
    old = FakeConnection("a", opened_at=now - 301.0)
    assert pool._too_old(old, now)


def test_a_lifetime_of_zero_turns_the_rule_off() -> None:
    """A store that wants the old behaviour sets zero, and nothing retires."""
    pool = make_pool(0.0)
    now = time.monotonic()
    ancient = FakeConnection("a", opened_at=now - 100_000.0)
    assert not pool._too_old(ancient, now)


@pytest.mark.anyio
async def test_the_pool_does_not_hand_out_a_connection_past_its_lifetime() -> None:
    pool = make_pool(300.0)
    now = time.monotonic()
    old = FakeConnection("old", opened_at=now - 301.0)
    pool.idle_conns.append((old, now))
    pool.all_conns.append(old)

    handed = await pool.get_or_create_conn()

    assert old.closed
    assert handed is not old
    assert old not in pool.all_conns


@pytest.mark.anyio
async def test_the_pool_reuses_a_connection_inside_its_lifetime() -> None:
    pool = make_pool(300.0)
    now = time.monotonic()
    young = FakeConnection("young", opened_at=now - 1.0)
    pool.idle_conns.append((young, now))
    pool.all_conns.append(young)

    handed = await pool.get_or_create_conn()

    assert handed is young
    assert not young.closed
