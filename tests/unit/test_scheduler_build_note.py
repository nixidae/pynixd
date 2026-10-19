"""pynixd names the backend, and only when the backend is not the local one.

`Scheduler.execute_build` wrote `pynixd: starting build on local at
<timestamp>` before each build. The line broke `main:cli-characterisation`,
which compares the output of a command against a recorded `.exp` file, and the
timestamp made no two recordings of one build agree.

Nix itself writes the location for a remote build only: `building '<drv>' on
'<machine>'...`. The backend daemon writes the plain `building '<drv>'...`
line, and pynixd forwards it.

Refs nixidae/nixidae#214.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.scheduler import Scheduler

if TYPE_CHECKING:
    from nix_daemon_protocol.logs import LogMessage
    from pynixd.build_queue import QueuedBuild
    from pynixd.store import DaemonStore


class FakeBuild:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def post_log_and_fanout(self, msg: LogMessage) -> None:
        self.messages.append(str(getattr(msg, "text", "")))


class FakeStore:
    def __init__(self, store_id: StoreId) -> None:
        self.store_id = store_id


async def _note(store_id: StoreId) -> list[str]:
    build = FakeBuild()
    store = FakeStore(store_id)
    await Scheduler._say_where_it_builds(  # noqa: SLF001 -- the note is the unit under test
        cast("QueuedBuild", build),
        cast("DaemonStore", store),
    )
    return build.messages


@pytest.mark.anyio
async def test_a_local_build_gets_no_note() -> None:
    assert await _note(LOCAL_STORE_ID) == []


@pytest.mark.anyio
async def test_a_remote_build_names_the_backend() -> None:
    assert await _note(StoreId("builder1")) == ["pynixd: building on builder1\n"]


@pytest.mark.anyio
async def test_the_note_carries_no_timestamp() -> None:
    """A recording of one build must agree with a recording of the next one."""
    first: Any = await _note(StoreId("builder1"))
    second: Any = await _note(StoreId("builder1"))
    assert first == second
