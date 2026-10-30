from __future__ import annotations

import threading
import time
from pathlib import Path
from unittest import mock

import anyio
import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store import LocalDBStore
from tests.conftest import make_test_spec

"""
A failed start tears down what it started (issue #61).

`Server.start` acquires state before it finishes: the local store opens
pool connections and database handles, whose threads keep the
interpreter alive. When `start` raised after that point, nothing tore
them down, so the process survived its own traceback and systemd saw a
start timeout instead of the failure.

The test below fails `trust_policy` -- late enough that the store fully
started, the same shape as the original report -- and asserts the start
error propagates, the server reads as stopped, and no non-daemon thread
the start created is still alive.
"""


async def test_start_failure_tears_down_started_state(tmp_path: Path) -> None:
    store_path = tmp_path / "store"
    store_path.mkdir()
    local_store = LocalDBStore(
        make_test_spec(store_id="local", store_path=store_path, no_probe=True),
    )
    server = Server(
        stores={StoreId("local"): local_store},
        ssh_port=None,
        http_port=None,
    )
    before = {t.ident for t in threading.enumerate() if not t.daemon}

    async def boom() -> None:
        raise RuntimeError("repro61: trust_policy failed")

    with mock.patch.object(local_store, "trust_policy", boom), pytest.raises(RuntimeError, match="repro61"):
        async with server:
            pass

    assert not server._started

    leaked: list[threading.Thread] = []
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        leaked = [
            t
            for t in threading.enumerate()
            if not t.daemon and t.ident not in before and t is not threading.current_thread()
        ]
        if not leaked:
            break
        await anyio.sleep(0.2)
    assert not leaked, f"start left non-daemon threads behind: {[t.name for t in leaked]}"
