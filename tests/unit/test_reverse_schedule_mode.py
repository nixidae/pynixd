"""A reverse builder serves the controller's sessions in its configured mode."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pynixd import reverse_client
from pynixd.config import ReverseInitiatorSettings, ScheduleMode

if TYPE_CHECKING:
    from collections.abc import Callable


class _Channel:
    def set_write_buffer_limits(self, *, high: int, low: int) -> None:
        pass


class _Process:
    command = "nix-daemon --stdio"
    stdin = object()
    stdout = object()
    stderr = object()
    channel = _Channel()

    def exit(self, code: int) -> None:
        self.exit_code = code

    async def wait_closed(self) -> None:
        pass


class _Ctx:
    class local_store:  # noqa: N801 - stands in for an attribute
        systems: list[str] = ["aarch64-darwin"]


def _capture(seen: dict[str, Any]) -> Callable[..., Any]:
    class _Proxy:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            seen.update(kwargs)

        async def run(self) -> None:
            pass

    return _Proxy


@pytest.mark.parametrize("mode", [ScheduleMode.proxy, ScheduleMode.scheduler])
async def test_session_uses_configured_schedule_mode(monkeypatch: pytest.MonkeyPatch, mode: ScheduleMode) -> None:
    seen: dict[str, Any] = {}
    monkeypatch.setattr(reverse_client, "DaemonProxy", _capture(seen))
    monkeypatch.setattr(reverse_client, "SSHNixReader", lambda *_a, **_k: None)
    monkeypatch.setattr(reverse_client, "SSHNixWriter", lambda *_a, **_k: None)
    monkeypatch.setattr(reverse_client, "ssh_hangup", lambda _p: None)

    initiator = reverse_client.ReverseInitiator(_Ctx(), ReverseInitiatorSettings(schedule_mode=mode))  # type: ignore[arg-type]
    process = _Process()
    await initiator._handle_request(process)  # type: ignore[arg-type] # noqa: SLF001

    assert seen["schedule_mode"] is mode
    assert process.exit_code == 0


def test_default_stays_proxy() -> None:
    assert ReverseInitiatorSettings().schedule_mode is ScheduleMode.proxy
