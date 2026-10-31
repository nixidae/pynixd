"""Shutdown answers each in-flight session, cancels it, and waits for its end. Issue #64."""

from __future__ import annotations

from typing import Any, cast

import anyio
import pytest

from pynixd import sessions as sessions_module
from pynixd.sessions import SHUTDOWN_ERROR, ClientSessions


class _Proxy:
    transport = "unix"

    def __init__(self) -> None:
        self.errors: list[str] = []

    async def send_error(self, reason: str) -> None:
        self.errors.append(reason)


@pytest.mark.anyio
async def test_shutdown_answers_cancels_and_waits_for_each_session() -> None:
    sessions = ClientSessions()
    proxy = _Proxy()
    cleaned = anyio.Event()

    async def run() -> None:
        session = sessions.track(cast("Any", proxy))
        try:
            with session.scope:
                await anyio.sleep_forever()
        finally:
            await anyio.sleep(0.01)
            cleaned.set()
            sessions.untrack(session)

    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(run)
            await anyio.wait_all_tasks_blocked()
            await sessions.shutdown()
            assert cleaned.is_set(), "shutdown returns after the session's cleanup"

    assert proxy.errors == [SHUTDOWN_ERROR]
    assert len(sessions) == 0


@pytest.mark.anyio
async def test_shutdown_abandons_a_session_past_the_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sessions_module, "SHUTDOWN_DEADLINE", 0.05)
    sessions = ClientSessions()
    sessions.track(cast("Any", _Proxy()))

    with anyio.fail_after(5):
        await sessions.shutdown()

    assert len(sessions) == 1
