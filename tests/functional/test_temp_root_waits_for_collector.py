"""A temporary root waits for a collector that holds the big lock and serves no socket.

Seen on dynhetz (nixidae/nixidae#1386): pynixd's own collection timed out an
upstream `CollectGarbage`, the daemon worker aborted, and its core dump held
`gc.lock` for 18 s with the roots socket already closed. pynixd gave up on the
root after 10 s, and the evaluation that sent it failed.
"""

from __future__ import annotations

import os
import signal
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread

from nix_daemon_protocol import AddTempRootRequest
from pynixd.connection import Connection
from pynixd.serde import StorePath
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, arm_store
from tests.functional.test_build_without_sandbox import _keep_store_dirs

if TYPE_CHECKING:
    from pathlib import Path

__all__ = ["_keep_store_dirs"]

_HELD_S = 12
"""Longer than the 10 s that pynixd waited before it gave up."""


def _open_writer(fifo: Path) -> int:
    """Return once the collector opens *fifo*: `gc.cc:397` reads it with the big lock held, before the socket."""
    return os.open(fifo, os.O_WRONLY)


async def test_a_root_waits_while_a_dying_collector_holds_the_lock(tmp_path: Path) -> None:
    after_lock = tmp_path / "after-lock.fifo"
    os.mkfifo(after_lock)
    config = for_test_store(substituters=())
    (tmp_path / "simple.txt").write_text("held by a temporary root\n")
    async with arm_store(tmp_path, Arm.daemon, config) as store:
        _rc, out, _err, _both = await run_subproc(
            [NIX_BIN, "store", "add-path", "--store", store.url, tmp_path / "simple.txt"],
            env=dict(store.env),
            nix_config=store.config,
        )
        path = out.strip()
        state = tmp_path / "var/nix"
        # The server's state directory, not the client's: the same `gc.lock`.
        collector_env = {
            **os.environ,
            **store.env,
            "NIX_STATE_DIR": str(state),
            "NIX_CONFIG": config.to_nix_config_env(),
            "_NIX_TEST_GC_SYNC_1": str(after_lock),
        }
        collector = await anyio.open_process(
            [NIX_BIN.with_name("nix-store"), "--store", "local", "--gc"], env=collector_env, stdout=None, stderr=None
        )
        writer = await anyio.to_thread.run_sync(_open_writer, after_lock, abandon_on_cancel=True)
        try:
            # `arm_store` serves the daemon arm on `<root>/daemon.sock`.
            link = await open_unix_connection(tmp_path / f"{Arm.daemon}.sock")
            conn = Connection(UnixNixReader(link, identifier="test"), UnixNixWriter(link, identifier="test"), "test")
            await conn.connect()
            answered = anyio.Event()
            outcome: list[object] = []

            async def add() -> None:
                try:
                    outcome.append(await conn.call(AddTempRootRequest(path=StorePath(path)), raise_on_error=True))
                except Exception as exc:  # noqa: BLE001 -- the assertion below names it
                    outcome.append(exc)
                answered.set()

            async with anyio.create_task_group() as tg:
                tg.start_soon(add)
                with anyio.move_on_after(_HELD_S):
                    await answered.wait()
                assert outcome == [], f"the root did not wait for the collector: {outcome}"
                # The worker on dynhetz died with the lock held; its lock went with the process.
                collector.send_signal(signal.SIGKILL)
                await collector.wait()
                with anyio.fail_after(10):
                    await answered.wait()
            assert not isinstance(outcome[0], Exception), outcome[0]
            # Read before the session ends: its end deletes the file.
            held = "".join(f.read_text() for f in (state / "temproots").glob("*-pynixd-*"))
            assert path in held
            await conn.close()
        finally:
            os.close(writer)
            if collector.returncode is None:
                collector.kill()
                await collector.wait()
