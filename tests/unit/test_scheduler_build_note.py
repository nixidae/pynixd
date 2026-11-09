"""pynixd names the backend, and only when the backend is not the local one.

pynixd wrote `pynixd: starting build on local at
<timestamp>` before each build. The line broke `main:cli-characterisation`,
which compares the output of a command against a recorded `.exp` file, and the
timestamp made no two recordings of one build agree.

Nix itself writes the location for a remote build only: `building '<drv>' on
'<machine>'...`. The backend daemon writes the plain `building '<drv>'...`
line, and pynixd forwards it.

Refs nixidae/nixidae#214.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.build_queue import BuildQueue
from pynixd.execution import say_where_it_builds
from tests._conftest.execution import log_bytes, queued
from tests._conftest.placement import FakeStore


async def _note(store_id: StoreId) -> bytes:
    build = await queued(BuildQueue())
    await say_where_it_builds(build, FakeStore(store_id))
    return log_bytes(build)


@pytest.mark.anyio
async def test_a_local_build_gets_no_note() -> None:
    assert await _note(LOCAL_STORE_ID) == b""


@pytest.mark.anyio
async def test_a_remote_build_names_the_backend() -> None:
    assert b"pynixd: building on builder1\n" in await _note(StoreId("builder1"))


@pytest.mark.anyio
async def test_the_note_carries_no_timestamp() -> None:
    """A recording of one build must agree with a recording of the next one."""
    assert await _note(StoreId("builder1")) == await _note(StoreId("builder1"))
