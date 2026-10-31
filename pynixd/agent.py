"""`pynixd-agent` on a builder: host samples over one SSH channel. Issue #98.

pynixd uploads the static binary (`agent/src/main.rs`) to the builder and
runs it. The agent sleeps in `poll()` and writes one frame per interval, or
at once when a PSI trigger at a high mark fires, so a fall in the build
limit does not wait for the next poll. It parses nothing; pynixd parses the
frame with the code the SFTP poller uses.
"""

from __future__ import annotations

import contextlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import anyio
import structlog

from .line_reader import LineReader
from .monitor import ResourceMonitor, create_monitor, health_from_files
from .psi import DiskSpace

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable
    from contextlib import AbstractAsyncContextManager

    from .config import PressureSettings
    from .psi import CgroupCpuStat, SystemHealth

log = structlog.get_logger(__name__)

AGENT_HELLO = "pynixd-agent 1 "
TRIGGER_WINDOW_US = 2_000_000
"""The shortest window an unprivileged user may arm a PSI trigger with."""
AGENTS_ENV = "PYNIXD_AGENTS"
"""JSON of system to agent binary. The package sets it to its own builds."""


class AgentStream(Protocol):
    """The output of a running `pynixd-agent`."""

    async def readline(self) -> bytes: ...
    async def readexactly(self, n: int) -> bytes: ...


class AgentUnavailableError(Exception):
    """The command did not answer as `pynixd-agent`."""


def agent_for(systems: Iterable[str], agents: dict[str, Path] | None) -> Path | None:
    """The agent binary for a builder of *systems*; `None` when there is none.

    *agents* `None` reads `PYNIXD_AGENTS`.
    """
    if agents is None:
        raw = os.environ.get(AGENTS_ENV)
        agents = {system: Path(path) for system, path in json.loads(raw).items()} if raw else {}
    return next((agents[system] for system in systems if system in agents), None)


def agent_args(settings: PressureSettings, disk_path: str | None = None) -> list[str]:
    """The interval, a trigger at each high mark that lowers the limit, and the store to `statvfs`."""
    args = [str(int(settings.interval * 1000))]
    for resource, high in (("memory", settings.memory_full_high), ("io", settings.io_full_high)):
        stall = max(1, int(high / 100 * TRIGGER_WINDOW_US))
        args.append(f"{resource}:full:{stall}:{TRIGGER_WINDOW_US}")
    if disk_path is not None:
        args.append(disk_path)
    return args


@dataclass(frozen=True)
class AgentFrame:
    files: dict[str, str | None]
    """The text of each file the agent read."""
    reason: str
    """Why the agent woke: `tick`, `ask`, or the resource whose trigger fired."""
    disk: DiskSpace | None = None


async def read_agent_frame(stream: AgentStream) -> AgentFrame:
    """One frame of the agent's output."""
    files: dict[str, str | None] = {}
    disk: DiskSpace | None = None
    while True:
        line = await stream.readline()
        if not line:
            raise EOFError("pynixd-agent closed its output")
        tag, _, rest = line.decode().rstrip("\n").partition(" ")
        if tag == "F":
            path, _, size = rest.rpartition(" ")
            files[path] = (await stream.readexactly(int(size))).decode()
        elif tag == "D":
            free, _, total = rest.partition(" ")
            disk = DiskSpace(free_bytes=int(free), total_bytes=int(total))
        elif tag == "E":
            return AgentFrame(files, rest.partition(" ")[0], disk)
        else:
            raise ValueError(f"pynixd-agent sent {line[:80]!r}")


def local_monitor(
    settings: PressureSettings,
    on_sample: Callable[[SystemHealth], None],
    systems: Iterable[str],
    replace: Callable[[ResourceMonitor], None],
) -> ResourceMonitor:
    """This machine's monitor: the agent as a subprocess, for its triggers,
    or the in-process poller where no agent fits. *replace* installs the
    poller if the agent does not start."""
    agent = agent_for(systems, settings.agents) if settings.agent else None
    if agent is None:
        return create_monitor(settings, on_sample)

    @contextlib.asynccontextmanager
    async def launch(args: list[str]) -> AsyncIterator[AgentStream]:
        proc = await anyio.open_process([str(agent), *args], stderr=None)
        try:
            if proc.stdout is None:
                raise AgentUnavailableError("no stdout")
            yield LineReader(proc.stdout)
        finally:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.aclose()

    return AgentMonitor(settings, on_sample, launch, lambda: replace(create_monitor(settings, on_sample)))


class AgentMonitor(ResourceMonitor):
    """Samples from `pynixd-agent` on the builder.

    *launch* starts the agent with the given arguments and yields its output;
    leaving the context ends the agent. A frame that does not come within
    three intervals counts as a dead channel, and the agent starts again.
    *fallback* runs when the agent never answered: a host that cannot run it
    is polled another way. *disk_path* is the store the agent reports the
    filesystem of.
    """

    def __init__(
        self,
        settings: PressureSettings,
        on_sample: Callable[[SystemHealth], None],
        launch: Callable[[list[str]], AbstractAsyncContextManager[AgentStream]],
        fallback: Callable[[], None],
        disk_path: str | None = None,
    ) -> None:
        super().__init__(settings, on_sample)
        self.launch = launch
        self.fallback = fallback
        self.disk_path = disk_path

    async def run(self) -> None:
        answered = False
        backoff = self.interval
        while self.running:
            try:
                async with self.launch(agent_args(self.settings, self.disk_path)) as stream:
                    with anyio.fail_after(self.interval * 3 + 10):
                        hello = (await stream.readline()).decode()
                    if not hello.startswith(AGENT_HELLO):
                        raise AgentUnavailableError(f"answered {hello[:80]!r}")
                    cores = float(hello.split()[2])
                    answered = True
                    backoff = self.interval
                    log.info("resource_agent_started", cores=cores)
                    stat: CgroupCpuStat | None = None
                    while self.running:
                        with anyio.fail_after(self.interval * 3):
                            frame = await read_agent_frame(stream)
                        health, stat = health_from_files(frame.files, stat, cores)
                        health.disk = frame.disk
                        await self._publish_sample(health)
            except anyio.get_cancelled_exc_class():
                raise
            except Exception as e:
                if not answered:
                    log.warning("resource_agent_unavailable", error=str(e) or type(e).__name__)
                    self.fallback()
                    return
                log.warning("resource_agent_lost", error=str(e) or type(e).__name__, retry_in=backoff)
            await anyio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
