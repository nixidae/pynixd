"""The zygote forks a build's sandbox helper, and the helper dies with whoever owns it (nixidae/nixidae#1590)."""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import sys
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
import pytest

from pynixd_store.builder.helpers import Helpers
from tests.unit.test_build_child import _spec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from pynixd_store.builder.helpers import ForkedHelper

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="the zygote forks Linux helpers")

OWNER = """
import os, sys, anyio, structlog
from pathlib import Path
from pynixd_store.builder.helpers import Helpers

# Its default is stdout, which carries the pids.
structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))

async def main():
    async with Helpers() as helpers:
        null = os.open(os.devnull, os.O_RDWR)
        _read, write = os.pipe()
        helper = await helpers.start(Path(sys.argv[1]), null, write, None)
        print(helper.pid, helpers.zygote_pid, flush=True)
        await anyio.sleep_forever()

anyio.run(main)
"""


class Started:
    """A helper with the parent's ends of its terminal and its status pipe."""

    def __init__(self, process: ForkedHelper, log_fd: int, status_fd: int) -> None:
        self.process = process
        self._log_fd = log_fd
        self._status_fd = status_fd

    async def log(self) -> str:
        """Everything the builder wrote, once every copy of its terminal is closed."""
        return await anyio.to_thread.run_sync(_read_all, self._log_fd, abandon_on_cancel=True)

    async def setup_error(self) -> str:
        return await anyio.to_thread.run_sync(_read_all, self._status_fd, abandon_on_cancel=True)


def _read_all(fd: int) -> str:
    with os.fdopen(os.dup(fd), "rb") as stream:
        return stream.read().decode()


@contextlib.asynccontextmanager
async def _started(helpers: Helpers, tmp_path: Path, script: str, name: str = "one") -> AsyncIterator[Started]:
    directory = tmp_path / name
    directory.mkdir()
    spec_file = directory / "spec.json"
    spec_file.write_text(json.dumps(_spec(directory / "root", script)))
    log_read, log_write = os.pipe()
    status_read, status_write = os.pipe()
    process = await helpers.start(spec_file, log_write, status_write, None)
    os.close(log_write)
    os.close(status_write)
    try:
        yield Started(process, log_read, status_read)
    finally:
        with anyio.CancelScope(shield=True):
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            with anyio.fail_after(10):
                await process.wait()
            os.close(log_read)
            os.close(status_read)


async def test_a_builder_writes_its_log_and_gives_its_exit_code(tmp_path: Path) -> None:
    async with Helpers() as helpers, _started(helpers, tmp_path, "echo hello; exit 3") as helper:
        assert await helper.log() == "hello\n"
        assert await helper.process.wait() == 3
        assert await helper.setup_error() == ""


async def test_a_setup_error_reaches_the_status_pipe(tmp_path: Path) -> None:
    async with Helpers() as helpers:
        spec_file = tmp_path / "spec.json"
        spec = _spec(tmp_path / "root", "true", binds=[("/missing", "/no/such/source", False)])
        spec_file.write_text(json.dumps(spec))
        null = os.open(os.devnull, os.O_RDWR)
        read, write = os.pipe()
        process = await helpers.start(spec_file, null, write, None)
        os.close(write)
        os.close(null)
        assert "/no/such/source" in await anyio.to_thread.run_sync(_read_all, read)
        assert await process.wait() == 1
        os.close(read)


async def test_builds_share_one_zygote_and_run_at_once(tmp_path: Path) -> None:
    async with Helpers() as helpers, _started(helpers, tmp_path, "read _; exit 0", "a") as first:
        zygote = helpers.zygote_pid
        async with _started(helpers, tmp_path, "exit 5", "b") as second:
            # The second ends while the first runs, so neither holds the other's log.
            assert await second.log() == ""
            assert await second.process.wait() == 5
            assert helpers.zygote_pid == zygote
        await first.process.wait()


async def test_a_killed_helper_ends_its_builder(tmp_path: Path) -> None:
    async with Helpers() as helpers, _started(helpers, tmp_path, "while :; do :; done") as helper:
        helper.process.kill()
        assert await helper.process.wait() == -signal.SIGKILL
        with anyio.fail_after(10):
            assert await helper.log() == ""


async def test_a_dead_zygote_takes_its_builds_and_the_next_build_gets_another(tmp_path: Path) -> None:
    async with Helpers() as helpers:
        async with _started(helpers, tmp_path, "while :; do :; done", "a") as first:
            zygote = helpers.zygote_pid
            assert zygote is not None
            os.kill(zygote, signal.SIGKILL)
            with anyio.fail_after(10):
                assert await first.process.wait() == -signal.SIGKILL
                assert await first.log() == ""
        async with _started(helpers, tmp_path, "exit 4", "b") as second:
            assert await second.process.wait() == 4
            assert helpers.zygote_pid != zygote


async def test_a_builder_inherits_no_descriptor_and_no_ignored_signal(tmp_path: Path) -> None:
    script = "echo /proc/self/fd/*; while read -r k v; do case $k in SigIgn:) echo $v;; esac; done < /proc/self/status"
    async with (
        Helpers() as helpers,
        _started(helpers, tmp_path, "while :; do :; done", "a"),
        _started(helpers, tmp_path, script, "b") as helper,
    ):
        fds, ignored = (await helper.log()).splitlines()
    assert {name.rpartition("/")[2] for name in fds.split()} <= {"0", "1", "2", "3"}
    assert int(ignored, 16) & (1 << (signal.SIGINT - 1)) == 0


async def test_a_killed_pynixd_leaves_no_helper(tmp_path: Path) -> None:
    spec_file = tmp_path / "spec.json"
    spec_file.write_text(json.dumps(_spec(tmp_path / "root", "while :; do :; done")))
    async with await anyio.open_process(
        [sys.executable, "-c", OWNER, str(spec_file)], stdout=subprocess.PIPE, stderr=None
    ) as owner:
        if owner.stdout is None:
            raise RuntimeError("no pipe")
        pids = [int(word) for word in (await owner.stdout.receive()).split()]
        pidfds = [os.pidfd_open(pid) for pid in pids]
        owner.kill()
        with anyio.fail_after(10):
            for pidfd in pidfds:
                await anyio.wait_readable(pidfd)
                os.close(pidfd)
