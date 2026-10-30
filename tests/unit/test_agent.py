"""`pynixd-agent` frames reach the build limit, and a dead agent is noticed. Issue #98."""

from __future__ import annotations

import asyncio
import contextlib
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import anyio

from pynixd.agent import AGENT_HELLO, AgentMonitor, agent_args, agent_for, local_monitor, read_agent_frame
from pynixd.background import Background
from pynixd.config import PressureSettings
from pynixd.monitor import SAMPLED_FILES, GenericResourcePoller, ResourceMonitor, health_from_files
from pynixd.psi import DiskSpace

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pynixd.psi import SystemHealth

PSI = "some avg10=1.00 avg60=0.00 avg300=0.00 total=1\nfull avg10={full} avg60=0.00 avg300=0.00 total=1\n"


def _file(path: str, text: str) -> bytes:
    data = text.encode()
    return f"F {path} {len(data)}\n".encode() + data


def _frame(memory_full: float = 0.0, reason: str = "tick") -> bytes:
    return (
        _file("/proc/pressure/cpu", PSI.format(full=0.0))
        + _file("/proc/pressure/memory", PSI.format(full=memory_full))
        + _file("/proc/pressure/io", PSI.format(full=0.0))
        + _file("/proc/meminfo", "MemTotal: 1000 kB\nMemAvailable: 900 kB\n")
        + _file("/proc/loadavg", "1.0 1.0 1.0 1/1 1\n")
        + b"D 5 9\n"
        + f"E {reason} 1\n".encode()
    )


class Stream:
    """An agent's output from bytes, then silence or EOF."""

    def __init__(self, data: bytes, hang: bool) -> None:
        self.data = data
        self.hang = hang

    async def readline(self) -> bytes:
        if not self.data:
            if self.hang:
                await anyio.sleep_forever()
            return b""
        line, _, self.data = self.data.partition(b"\n")
        return line + b"\n"

    async def readexactly(self, n: int) -> bytes:
        out, self.data = self.data[:n], self.data[n:]
        return out


async def test_a_frame_parses_every_file_and_the_reason() -> None:
    frame = await read_agent_frame(Stream(_frame(memory_full=3.5, reason="memory"), hang=False))
    assert frame.reason == "memory"
    assert frame.disk == DiskSpace(free_bytes=5, total_bytes=9)
    assert set(frame.files) == {
        "/proc/pressure/cpu",
        "/proc/pressure/memory",
        "/proc/pressure/io",
        "/proc/meminfo",
        "/proc/loadavg",
    }


def test_the_args_arm_a_trigger_at_each_falling_mark() -> None:
    args = agent_args(PressureSettings(interval=2.0, memory_full_high=2.0, io_full_high=25.0), "/nix/store")
    assert args == ["2000", "memory:full:40000:2000000", "io:full:500000:2000000", "/nix/store"]


def test_the_agent_matches_a_system_of_the_builder() -> None:
    agents = {"aarch64-linux": Path("/a"), "x86_64-linux": Path("/x")}
    assert agent_for(["i686-linux", "x86_64-linux"], agents) == Path("/x")
    assert agent_for(["aarch64-darwin"], agents) is None


async def _run(monitor: AgentMonitor, seconds: float) -> None:
    monitor.running = True
    with anyio.move_on_after(seconds):
        await monitor.run()


async def test_samples_reach_the_handler_and_a_silent_agent_restarts() -> None:
    seen: list[SystemHealth] = []
    launches: list[list[str]] = []

    @contextlib.asynccontextmanager
    async def launch(args: list[str]) -> AsyncIterator[Stream]:
        launches.append(args)
        yield Stream(b"pynixd-agent 1 8\n" + _frame(memory_full=3.5), hang=True)

    monitor = AgentMonitor(PressureSettings(interval=0.05), seen.append, launch, fallback=lambda: None)
    await _run(monitor, 0.5)

    assert seen and seen[0].psi is not None and seen[0].psi.memory.full_avg10 == 3.5
    assert seen[0].disk == DiskSpace(free_bytes=5, total_bytes=9)
    assert seen[0].cores == 8.0
    assert len(launches) >= 2, "a silent channel must count as dead"


async def test_a_command_that_is_not_the_agent_falls_back() -> None:
    """A forced command runs `nix-daemon --stdio` instead, which never says hello."""
    fell_back: list[bool] = []

    @contextlib.asynccontextmanager
    async def launch(args: list[str]) -> AsyncIterator[Stream]:
        del args
        yield Stream(b"\x63\x78\x69\x6e\x00\x00\x00\x00\n", hang=True)

    monitor = AgentMonitor(
        PressureSettings(interval=0.05), lambda _: None, launch, fallback=lambda: fell_back.append(True)
    )
    await _run(monitor, 2.0)
    assert fell_back == [True]


async def test_the_real_agent_speaks_what_the_parser_reads() -> None:
    """The C agent and the Python parser agree, and EOF on stdin ends the agent.

    The guest installs `pynixd-agent` (`tests/derivations/guest`).
    """
    binary = shutil.which("pynixd-agent")
    assert binary is not None, "pynixd-agent is not on PATH"
    proc = await asyncio.create_subprocess_exec(
        binary,
        # A long interval, so the frame after the first is the asked one.
        *agent_args(PressureSettings(interval=10.0), "/"),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    assert proc.stdin is not None and proc.stdout is not None
    with anyio.fail_after(5):
        hello = (await proc.stdout.readline()).decode()
        first = await read_agent_frame(proc.stdout)
        proc.stdin.write(b"\n")
        asked = (await read_agent_frame(proc.stdout)).reason
        proc.stdin.close()
        rc = await proc.wait()

    assert hello.startswith(AGENT_HELLO)
    assert first.reason == "tick"
    assert asked == "ask"
    assert set(first.files) <= set(SAMPLED_FILES)
    assert first.disk is not None and 0 < first.disk.free_bytes <= first.disk.total_bytes
    health, _ = health_from_files(first.files, None, float(hello.split()[2]))
    assert health.meminfo is not None and health.meminfo.mem_total > 0
    assert health.load1 is not None
    assert rc == 0


async def test_the_local_store_runs_the_agent_for_its_triggers() -> None:
    binary = shutil.which("pynixd-agent")
    assert binary is not None, "pynixd-agent is not on PATH"
    seen: list[SystemHealth] = []
    replaced: list[ResourceMonitor] = []
    settings = PressureSettings(interval=0.2, agents={"this-system": Path(binary)})

    monitor = local_monitor(settings, seen.append, ["this-system"], replaced.append)
    assert isinstance(monitor, AgentMonitor)
    background = Background("test")
    async with background.running():
        monitor.start(background)
        with anyio.fail_after(5):
            while len(seen) < 2:
                await anyio.sleep(0.05)
    assert replaced == []
    assert seen[0].meminfo is not None and seen[0].meminfo.mem_total > 0


def test_no_agent_for_this_system_polls_in_process() -> None:
    monitor = local_monitor(PressureSettings(agents={}), lambda _: None, ["this-system"], lambda _: None)
    assert isinstance(monitor, GenericResourcePoller)
