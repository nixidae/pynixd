"""A failed open leaves no threads behind.

`LocalStoreDB.open` probes the database with a pooled connection, and
each pooled connection holds a worker thread. When the probe failed,
`open` returned an inactive instance with the pool still open: the
threads outlived the call, and a short-lived process with no other work
-- a CLI whose store database is unreadable -- never exited. The same
class of failure as issue #61 one level up, found by the freshness
simulation tripping over it.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import anyio
import pytest

from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout


@pytest.mark.anyio
async def test_a_failed_open_closes_what_the_probe_opened(tmp_path: Path) -> None:
    db_path = tmp_path / "nix" / "var" / "nix" / "db" / "db.sqlite"
    db_path.parent.mkdir(parents=True)
    db_path.touch()
    before = {t.ident for t in threading.enumerate() if not t.daemon}

    db = await LocalStoreDB.open(StoreLayout.chroot(tmp_path))

    assert not db.active
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
    assert not leaked, f"failed open left threads behind: {[t.name for t in leaked]}"
