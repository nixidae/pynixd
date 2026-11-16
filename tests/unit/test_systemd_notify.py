"""`pynixd.systemd.notify`: what systemd's `Type=notify` reads, and nothing without it."""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest

from pynixd.systemd import notify


def test_nothing_is_sent_outside_systemd(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    assert notify("READY=1") is False


def test_the_state_reaches_the_socket_systemd_names(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "notify"
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as manager:
        manager.bind(str(path))
        monkeypatch.setenv("NOTIFY_SOCKET", str(path))
        assert notify("READY=1") is True
        assert manager.recv(64) == b"READY=1"


@pytest.mark.skipif(sys.platform != "linux", reason="abstract sockets are Linux's")
def test_an_abstract_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    """systemd writes an abstract address with a leading `@`."""
    name = f"pynixd-notify-test-{id(monkeypatch)}"
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as manager:
        manager.bind("\0" + name)
        monkeypatch.setenv("NOTIFY_SOCKET", "@" + name)
        notify("STOPPING=1")
        assert manager.recv(64) == b"STOPPING=1"
