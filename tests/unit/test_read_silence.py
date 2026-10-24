"""A substituting read ends when the daemon falls silent, not when it runs long.

A `QueryValidPaths` with `substitute` set downloads before it answers, and
the daemon reports progress all the way (`daemon.cc:346`). A pure read keeps
its bound on the whole read: `test_upstream_faults.py` pins that.
nixidae/nixidae#1074.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest

from pynixd.exceptions import BackendError
from pynixd.serde import QueryPathInfoRequest, QueryValidPathsRequest, StorePath
from pynixd.store import daemon

SILENCE = 0.1


class _Reading:
    """A connection whose daemon sends a progress line every `every` seconds, `lines` times."""

    def __init__(self, every: float, lines: int) -> None:
        self.r = SimpleNamespace(last_read_at=time.monotonic())
        self.every = every
        self.lines = lines

    async def call(self) -> str:
        for _ in range(self.lines):
            await anyio.sleep(self.every)
            self.r.last_read_at = time.monotonic()
        return "answer"


@pytest.mark.anyio
async def test_a_read_that_keeps_reporting_outlasts_the_bound() -> None:
    """A substitution takes as long as its closure, and the daemon reports progress all the way."""
    conn = _Reading(every=SILENCE / 4, lines=12)

    assert await daemon._until_silent(cast("Any", conn), conn.call, SILENCE) == "answer"


@pytest.mark.anyio
async def test_a_read_that_falls_silent_ends_at_the_bound() -> None:
    """A builder that went silent must not hold the build. nixidae/nixidae#1063."""
    conn = _Reading(every=SILENCE * 20, lines=1)
    started = time.monotonic()

    with pytest.raises(TimeoutError):
        await daemon._until_silent(cast("Any", conn), conn.call, SILENCE)
    assert time.monotonic() - started < SILENCE * 10


@pytest.mark.anyio
async def test_the_error_of_the_call_keeps_its_class() -> None:
    """A caller catches `BackendError`; a task group would hand it an `ExceptionGroup`."""

    async def refuse() -> None:
        raise BackendError("refused")

    with pytest.raises(BackendError, match="refused"):
        await daemon._until_silent(cast("Any", _Reading(every=0, lines=0)), refuse, SILENCE)


def test_only_a_substituting_query_downloads() -> None:
    paths = {StorePath(path="/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-source")}

    assert daemon._downloads(QueryValidPathsRequest(paths=paths, substitute=1))
    assert not daemon._downloads(QueryValidPathsRequest(paths=paths, substitute=0))
    assert not daemon._downloads(QueryPathInfoRequest(path=next(iter(paths))))
