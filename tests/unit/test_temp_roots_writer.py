"""The edge cases of `RootWriter`: the thread, its queue, and the order of a session's jobs.

`test_temp_roots.py` holds the contract with Nix's collector; this file holds the writer's own.
Each case runs on asyncio and on uvloop, pynixd's loop. The `writer` phase of
`internal.tests.temp-roots` runs this file too, as root, with `--noconftest`.

The freeze case needs `TEMP_ROOTS_FREEZE` (a mount this process may freeze) and
`TEMP_ROOTS_OTHER` (a directory on another filesystem); it skips without them.
"""

from __future__ import annotations

import fcntl
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Final

import anyio
import anyio.to_thread
import pytest
import uvloop
from anyio import wait_all_tasks_blocked
from anyio.lowlevel import checkpoint

from pynixd_store import temp_roots
from pynixd_store.temp_roots import (
    Outcome,
    RootWriteError,
    RootWriter,
    TempRoots,
    WriterClosedError,
)
from tests._conftest.loop_lag import LoopLag
from tests._conftest.temp_roots import Collector, read_roots, short_path as short_path

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator

ROOTS: Final = [f"/nix/store/{index:032d}-root" for index in range(400)]
LOOP_FREE_S: Final = 0.1
"""The longest loop stall a case accepts while the writer is blocked."""
GATE_LIMIT_S: Final = 30.0
CASE_LIMIT_S: Final = 20.0
"""Every case ends within this; without the suite's conftest nothing else bounds a hang."""


@pytest.fixture(
    params=[
        pytest.param(("asyncio", {}), id="asyncio"),
        pytest.param(("asyncio", {"loop_factory": uvloop.new_event_loop}), id="uvloop"),
    ]
)
def anyio_backend(request: pytest.FixtureRequest) -> tuple[str, dict[str, object]]:
    return request.param  # type: ignore[no-any-return]


@pytest.fixture(autouse=True)
async def bounded() -> AsyncIterator[None]:
    """A case that waits on a writer that will never answer fails here instead of hanging."""
    with anyio.fail_after(CASE_LIMIT_S):
        yield


def leftover_files(state: Path) -> list[str]:
    directory = state / "temproots"
    return sorted(entry.name for entry in directory.iterdir()) if directory.exists() else []


class Gate:
    """Holds every `write_now` on the writer thread until `open`; `reached` says one is waiting."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.opened = threading.Event()
        self.reached = threading.Event()
        self.written: list[str] = []
        original = TempRoots.write_now
        gate = self

        def gated(roots_self: TempRoots, roots: list[str]) -> Outcome:
            gate.reached.set()
            if not gate.opened.wait(GATE_LIMIT_S):
                raise TimeoutError("the gate never opened")
            outcome = original(roots_self, roots)
            gate.written.extend(roots)
            return outcome

        monkeypatch.setattr(TempRoots, "write_now", gated)

    def open(self) -> None:
        self.opened.set()

    async def wait_reached(self) -> None:
        await anyio.to_thread.run_sync(self.reached.wait, GATE_LIMIT_S)


@pytest.fixture
def gate(monkeypatch: pytest.MonkeyPatch) -> Iterator[Gate]:
    held = Gate(monkeypatch)
    yield held
    held.open()


async def test_roots_reach_the_file_in_the_order_they_were_added(tmp_path: Path) -> None:
    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        for root in ROOTS[:100]:
            assert await roots.add(root)
        assert await roots.add_many(ROOTS[100:])
        assert read_roots(roots.path) == ROOTS
        await roots.close()
        assert not roots.path.exists()


async def test_the_loop_never_touches_a_roots_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every open, lock, write and close of the roots file and `gc.lock` runs on the writer's thread."""
    calls: list[tuple[str, int, object]] = []

    def recording(name: str, real: Callable[..., object]) -> Callable[..., object]:
        def call(*args: object, **kwargs: object) -> object:
            calls.append((name, threading.get_ident(), args[0]))
            return real(*args, **kwargs)

        return call

    for name, module in (("open", os), ("write", os), ("close", os), ("flock", fcntl)):
        monkeypatch.setattr(module, name, recording(name, getattr(module, name)))

    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        assert await roots.add(ROOTS[0])
        fds = {roots._fd, roots._gc_lock_fd}  # noqa: SLF001 -- the descriptors under test
        assert await roots.add_many(ROOTS[1:10])
        await roots.close()
        # Up to the close only: after it, the kernel hands these descriptor numbers to anything.
        upto_close = len(calls)
        thread = writer.thread_ident
    monkeypatch.undo()

    ours = [
        (name, ident)
        for name, ident, target in calls[:upto_close]
        if (name == "open" and str(target).startswith(str(tmp_path))) or (name != "open" and target in fds)
    ]
    assert {name for name, _ in ours} == {"open", "write", "close", "flock"}
    assert {ident for _, ident in ours} == {thread}


async def test_sessions_at_once_share_the_writers_passes(tmp_path: Path) -> None:
    """Group commit: sixteen sessions adding one root at a time need far fewer passes than adds."""
    async with RootWriter(tmp_path) as writer:
        sessions = [writer.session() for _ in range(16)]
        expected = [[f"/nix/store/{s:016d}{i:016d}-root" for i in range(40)] for s in range(16)]

        async def one_at_a_time(roots: TempRoots, paths: list[str]) -> None:
            for path in paths:
                assert await roots.add(path)

        async with anyio.create_task_group() as tg:
            for roots, paths in zip(sessions, expected, strict=True):
                tg.start_soon(one_at_a_time, roots, paths)
        for roots, paths in zip(sessions, expected, strict=True):
            assert read_roots(roots.path) == paths
            await roots.close()
        assert writer.passes < 16 * 40


async def test_a_root_added_while_the_collector_runs_goes_over_its_socket(short_path: Path) -> None:
    collector = Collector(short_path)
    try:
        async with RootWriter(short_path) as writer:
            roots = writer.session()
            assert await roots.add_many(ROOTS[:3])
            assert collector.roots == ROOTS[:3]
            assert read_roots(roots.path) == ROOTS[:3]
            await roots.close()
    finally:
        collector.stop()


async def test_a_root_waits_for_a_collector_with_no_socket_and_the_loop_runs(
    short_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A collector holding `gc.lock` with no socket (a worker dumping core, nixidae/nixidae#1386)."""
    monkeypatch.setattr(temp_roots, "RETRY_DELAY", 0.01)
    lock = os.open(short_path / "gc.lock", os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX)
    lag = LoopLag()
    done = anyio.Event()
    async with RootWriter(short_path) as writer:
        roots = writer.session()

        async def add() -> None:
            assert await roots.add(ROOTS[0])
            done.set()

        async with anyio.create_task_group() as tg:
            tg.start_soon(lag.run)
            tg.start_soon(add)
            await anyio.sleep(0.3)
            assert not done.is_set()
            os.close(lock)
            await done.wait()
            lag.stop()
        assert read_roots(roots.path) == [ROOTS[0]]
        await roots.close()
    assert lag.max_lag_s < LOOP_FREE_S


async def test_an_unwritable_state_directory_gives_no_root(tmp_path: Path) -> None:
    """`temproots` is a file, so even root cannot make the directory; pynixd then asks the daemon."""
    (tmp_path / "temproots").write_text("")
    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        assert not await roots.add(ROOTS[0])
        assert not await roots.add_many(ROOTS[1:3])
        await roots.close()


async def test_a_read_only_store_writes_nothing(tmp_path: Path) -> None:
    async with RootWriter(tmp_path) as writer:
        roots = writer.session(read_only=True)
        assert await roots.add_many(ROOTS[:5])
        assert writer.passes == 0
        assert not (tmp_path / "temproots").exists()


async def test_a_failed_write_reaches_its_caller_and_the_writer_goes_on(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = TempRoots.write_now
    failures = iter([ValueError("disk said no")])

    def failing_once(roots_self: TempRoots, roots: list[str]) -> Outcome:
        failure = next(failures, None)
        if failure is not None:
            raise failure
        return original(roots_self, roots)

    monkeypatch.setattr(TempRoots, "write_now", failing_once)
    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        with pytest.raises(RootWriteError) as raised:
            await roots.add(ROOTS[0])
        assert isinstance(raised.value.__cause__, ValueError)
        assert await roots.add(ROOTS[1])
        assert read_roots(roots.path) == [ROOTS[1]]
        await roots.close()


async def test_a_cancelled_add_is_still_written_and_its_close_comes_after(tmp_path: Path, gate: Gate) -> None:
    """The caller gave up, but the root is in the file until the close, and the close leaves no file."""
    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        with anyio.move_on_after(0.1) as scope:
            await roots.add(ROOTS[0])
        assert scope.cancelled_caught
        async with anyio.create_task_group() as tg:
            tg.start_soon(roots.close)
            await wait_all_tasks_blocked()
            gate.open()
        assert gate.written == [ROOTS[0]]
        assert leftover_files(tmp_path) == []


async def test_an_add_queued_behind_its_close_makes_no_file(tmp_path: Path, gate: Gate) -> None:
    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        late: list[BaseException] = []

        async def add_late() -> None:
            try:
                await roots.add(ROOTS[1])
            except RootWriteError as exc:
                late.append(exc)

        async with anyio.create_task_group() as tg:
            tg.start_soon(roots.add, ROOTS[0])
            await gate.wait_reached()
            tg.start_soon(roots.close)
            await wait_all_tasks_blocked()
            tg.start_soon(add_late)
            await wait_all_tasks_blocked()
            gate.open()
        assert len(late) == 1
        assert leftover_files(tmp_path) == []


async def test_an_add_after_close_is_refused(tmp_path: Path) -> None:
    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        assert await roots.add(ROOTS[0])
        await roots.close()
        with pytest.raises(RuntimeError, match="closed"):
            await roots.add(ROOTS[1])
        assert leftover_files(tmp_path) == []


async def test_the_writer_finishes_queued_jobs_before_it_stops(tmp_path: Path, gate: Gate) -> None:
    held: list[bool] = []
    async with anyio.create_task_group() as tg, RootWriter(tmp_path) as writer:
        roots = writer.session()

        async def add(path: str) -> None:
            held.append(await roots.add(path))

        for path in ROOTS[:5]:
            tg.start_soon(add, path)
        await gate.wait_reached()
        await wait_all_tasks_blocked()
        threading.Timer(0.2, gate.open).start()
    assert held == [True] * 5
    assert sorted(read_roots(roots.path)) == sorted(ROOTS[:5])
    await roots.close()
    assert leftover_files(tmp_path) == []


async def test_a_job_after_the_writer_stopped_is_refused_and_close_still_releases(tmp_path: Path) -> None:
    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        assert await roots.add(ROOTS[0])
    with pytest.raises(WriterClosedError):
        await roots.add(ROOTS[1])
    await roots.close()
    assert leftover_files(tmp_path) == []


async def test_a_blocked_write_holds_no_loop(tmp_path: Path, gate: Gate) -> None:
    lag = LoopLag()
    async with RootWriter(tmp_path) as writer, anyio.create_task_group() as tg:
        roots = writer.session()
        tg.start_soon(lag.run)
        await anyio.sleep(0.02)  # the probe must be running before the write can block anything
        threading.Timer(0.5, gate.open).start()
        started = time.monotonic()
        assert await roots.add(ROOTS[0])
        waited = time.monotonic() - started
        lag.stop()
        await roots.close()
    assert waited >= 0.4
    assert lag.max_lag_s < LOOP_FREE_S


async def test_shutdown_gives_up_on_a_stuck_writer_and_its_jobs_still_finish(
    tmp_path: Path, gate: Gate, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(temp_roots, "SHUTDOWN_SECONDS", 0.2)
    held: list[bool] = []
    async with anyio.create_task_group() as tg:
        writer = RootWriter(tmp_path)
        async with writer:
            roots = writer.session()

            async def add() -> None:
                held.append(await roots.add(ROOTS[0]))

            tg.start_soon(add)
            await gate.wait_reached()
            started = time.monotonic()
        assert time.monotonic() - started < 1.0
        gate.open()
    assert held == [True]
    await roots.close()
    assert leftover_files(tmp_path) == []


class Crash(BaseException):
    """Not an `Exception`: the writer must still wake its callers before its thread ends."""


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
async def test_a_dying_writer_wakes_its_callers_and_refuses_new_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def crash(_self: RootWriter, _jobs: object) -> None:
        raise Crash

    monkeypatch.setattr(RootWriter, "_perform", crash)
    async with RootWriter(tmp_path) as writer:
        roots = writer.session()
        with pytest.raises(RootWriteError):
            await roots.add(ROOTS[0])
        await anyio.to_thread.run_sync(writer.join)
        with pytest.raises(WriterClosedError):
            await roots.add(ROOTS[1])
        await roots.close()


async def test_a_writer_nobody_started_refuses_a_job(tmp_path: Path) -> None:
    """No thread would ever take the job, so the caller would wait forever."""
    roots = RootWriter(tmp_path).session()
    with pytest.raises(RuntimeError, match="outside `async with`"):
        await roots.add(ROOTS[0])
    assert leftover_files(tmp_path) == []


async def test_a_close_under_a_deadline_returns_and_still_releases(tmp_path: Path, gate: Gate) -> None:
    """A builder bounds its close (`execution.py`); the close returns at the deadline and runs later."""
    async with RootWriter(tmp_path) as writer, anyio.create_task_group() as tg:
        roots = writer.session()
        tg.start_soon(roots.add, ROOTS[0])
        await gate.wait_reached()
        started = time.monotonic()
        with anyio.move_on_after(0.2):
            await roots.close()
        assert time.monotonic() - started < 1.0
        gate.open()
    # The gated write made the file, so only the close that ran after it removed it.
    assert gate.written == [ROOTS[0]]
    assert leftover_files(tmp_path) == []


# ── A frozen filesystem (root, in the guest) ─────────────────────────

FREEZE = os.environ.get("TEMP_ROOTS_FREEZE")
OTHER = os.environ.get("TEMP_ROOTS_OTHER")
needs_freeze = pytest.mark.skipif(
    FREEZE is None or OTHER is None or os.geteuid() != 0,
    reason="needs TEMP_ROOTS_FREEZE, TEMP_ROOTS_OTHER and root",
)


@needs_freeze
async def test_a_frozen_store_holds_neither_the_loop_nor_another_store() -> None:
    """One writer per state directory: the store on the frozen filesystem waits alone."""
    if FREEZE is None or OTHER is None:
        raise RuntimeError("needs_freeze let this run")
    frozen_state = Path(tempfile.mkdtemp(dir=FREEZE))
    other_state = Path(tempfile.mkdtemp(dir=OTHER))
    lag = LoopLag()
    thaw = subprocess.Popen(["/bin/sh", "-c", f"sleep 1.5; fsfreeze --unfreeze {FREEZE}"])  # noqa: S603 -- fixed argv
    subprocess.run(["fsfreeze", "--freeze", FREEZE], check=True)  # noqa: S603, S607 -- PATH's util-linux
    try:
        async with (
            RootWriter(frozen_state) as frozen_writer,
            RootWriter(other_state) as other_writer,
            anyio.create_task_group() as tg,
        ):
            tg.start_soon(lag.run)
            frozen = frozen_writer.session()
            other = other_writer.session()
            frozen_done = anyio.Event()

            async def add_frozen() -> None:
                assert await frozen.add(ROOTS[0])
                frozen_done.set()

            tg.start_soon(add_frozen)
            await checkpoint()
            started = time.monotonic()
            assert await other.add(ROOTS[1])
            other_s = time.monotonic() - started
            assert not frozen_done.is_set()
            await frozen_done.wait()
            lag.stop()
            await frozen.close()
            await other.close()
    finally:
        thaw.wait()
        subprocess.run(["fsfreeze", "--unfreeze", FREEZE], check=False, capture_output=True)  # noqa: S603, S607
        shutil.rmtree(frozen_state, ignore_errors=True)
        shutil.rmtree(other_state, ignore_errors=True)
    assert other_s < LOOP_FREE_S
    assert lag.max_lag_s < LOOP_FREE_S
