"""
Host sampling for pynixd stores: PSI, memory, CPU and load. Issue #98.

Each sample goes to the store's build limit. Local stores read `/proc`
directly, SSH stores read the same files over SFTP. Every file is
machine-wide: a build competes with everything else on its host.

The poller arms no PSI trigger. A trigger fd always polls readable, and an
asyncio reader waits for readability, so a reader on one runs on every loop
iteration; `pynixd/agent.py` waits for POLLPRI instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

import anyio
import structlog

from .daemon_extensions.pynixd_host_health import (
    HEALTH_JSON,
    PynixdHostHealthRequest,
    PynixdHostHealthStreamRequest,
)
from .exceptions import BackendError
from .psi import (
    CgroupCpuStat,
    DiskSpace,
    SystemHealth,
    compute_cpu_util,
    count_cpus_from_proc_stat,
    parse_cpu_max,
    parse_cpu_stat,
    parse_loadavg,
    parse_meminfo,
    parse_psi_output,
)
from .serde import LogError, LogNext
from .serde.context import WriteContext

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine, Mapping

    from .config import PressureSettings

log = structlog.get_logger(__name__)


class ResourceMonitor(ABC):
    """Samples a host and hands each sample to *on_sample*."""

    def __init__(self, settings: PressureSettings, on_sample: Callable[[SystemHealth], None]) -> None:
        self.settings = settings
        self.on_sample = on_sample
        self.running = False
        self.task: asyncio.Task[None] | None = None
        self.health = SystemHealth()
        self.disk_fn: Callable[[], Awaitable[DiskSpace | None]] | None = None
        """Samples the store's filesystem, whatever reads the other files.

        The store sets it: `os.statvfs` locally, SFTP's `statvfs` on an SSH
        builder. A pynixd peer answers its own disk in `PynixdHostHealth`.
        nixidae/nixidae#1013."""

    @property
    def interval(self) -> float:
        return self.settings.interval

    @abstractmethod
    async def run(self) -> None:
        """Monitor loop."""
        ...

    def start(self) -> None:
        """Start the monitor loop as a background task."""
        if not self.running:
            self.running = True
            self.task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        """Stop the monitor loop and cancel its background task."""
        self.running = False
        if self.task:
            self.task.cancel()
            with contextlib.suppress(Exception, anyio.get_cancelled_exc_class()):
                await self.task

    async def _publish_sample(self, health: SystemHealth) -> None:
        """Add the store's disk to *health*, then publish it."""
        if self.disk_fn is not None and health.disk is None:
            try:
                health.disk = await self.disk_fn()
            except Exception:
                log.warning("disk_sample_failed", exc_info=True)
        self._publish(health)

    def _publish(self, health: SystemHealth) -> None:
        self.health = health
        try:
            self.on_sample(health)
        except Exception:
            log.exception("resource_sample_handler_failed")


class DummyResourceMonitor(ResourceMonitor):
    """A store with monitoring off: one empty sample, so the limit is the ceiling."""

    async def run(self) -> None:
        self._publish(SystemHealth())


class GenericResourcePoller(ResourceMonitor):
    """Polls PSI, memory, CPU and load through *read_fn*.

    *read_fn* answers a file's text, or `None` when the host has no such file
    or refuses it. *fallback* runs when the first sample raises: a host that
    refuses the channel (SFTP off) is not asked again every interval.
    """

    def __init__(
        self,
        settings: PressureSettings,
        on_sample: Callable[[SystemHealth], None],
        read_fn: Callable[[str], Coroutine[None, None, str | None]],
        fallback: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(settings, on_sample)
        self.read_fn = read_fn
        self.fallback = fallback
        self.cpu_stat_prev: CgroupCpuStat | None = None
        self.cpu_cores: float | None = None

    async def _cores(self) -> float | None:
        return await cores_from(self.read_fn)

    async def sample(self) -> SystemHealth:
        """Read one sample."""
        if self.cpu_cores is None:
            self.cpu_cores = await self._cores()
        files = {path: await self.read_fn(path) for path in SAMPLED_FILES}
        health, self.cpu_stat_prev = health_from_files(files, self.cpu_stat_prev, self.cpu_cores)
        return health

    async def run(self) -> None:
        """Sample every `interval` until stopped."""
        log.info("resource_poller_started", interval=self.interval)
        answered = False
        while self.running:
            try:
                await self._publish_sample(await self.sample())
                answered = True
            except anyio.get_cancelled_exc_class():
                raise
            except Exception as e:
                if not answered and self.fallback is not None:
                    log.warning("resource_poller_unavailable", error=str(e) or type(e).__name__)
                    self.fallback()
                    return
                log.exception("resource_poller_tick_failed")
            await anyio.sleep(self.interval)


async def cores_from(read_fn: Callable[[str], Coroutine[None, None, str | None]]) -> float | None:
    """The cores a host's cgroup grants, else the CPUs `/proc/stat` lists."""
    text = await read_fn("/sys/fs/cgroup/cpu.max")
    cores = parse_cpu_max(text) if text else None
    if cores is None:
        text = await read_fn("/proc/stat")
        cores = float(count_cpus_from_proc_stat(text)) if text else None
    return cores


class PeerHealthMonitor(ResourceMonitor):
    """Samples a pynixd peer over the daemon protocol, by what the peer announces.

    A reverse builder runs pynixd and accepts nothing but daemon sessions, so
    SFTP and `pynixd-agent` cannot reach it; its own pynixd samples its host
    and its store's disk. `PynixdHostHealthStream` holds one session outside
    the pools and gets each sample as the peer takes it, PSI triggers
    included (nixidae/nixidae#1016). An older peer is polled with
    `PynixdHostHealth` every interval (nixidae/nixidae#1013). A peer with
    neither gives empty samples, as monitoring off does, and ranks last.
    """

    def __init__(self, settings: PressureSettings, on_sample: Callable[[SystemHealth], None], store: Any) -> None:
        super().__init__(settings, on_sample)
        self.store = store
        self.stat: CgroupCpuStat | None = None

    async def run(self) -> None:
        backoff = self.interval
        while self.running:
            features = self.store.features
            try:
                if PynixdHostHealthStreamRequest.name in features:
                    await self._stream()
                elif PynixdHostHealthRequest.name in features:
                    await self._poll()
                else:
                    self._publish(SystemHealth())
                backoff = self.interval
            except anyio.get_cancelled_exc_class():
                raise
            except Exception as e:
                self._publish(SystemHealth())
                backoff = min(backoff * 2, 60.0)
                log.warning(
                    "peer_health_sample_failed",
                    store_id=getattr(self.store, "store_id", None),
                    error=str(e) or type(e).__name__,
                    retry_in=backoff,
                )
            await anyio.sleep(backoff)

    async def _poll(self) -> None:
        response = await self.store.execute(PynixdHostHealthRequest())
        data = json.loads(response.payload)
        health, self.stat = health_from_files(data.get("files", {}), self.stat, data.get("cores"))
        if data.get("disk"):
            health.disk = DiskSpace(**data["disk"])
        self._publish(health)

    async def _stream(self) -> None:
        """Publish the peer's samples until its session ends or falls silent."""
        # The peer sends its last sample at once, so the first one needs no grace.
        silence = self.interval * 3
        conn = await self.store.create_conn()
        try:
            with anyio.CancelScope(deadline=anyio.current_time() + silence) as scope:
                # Read here, not through `Connection.call`: the stream never
                # ends, and `call` waits for a response after it.
                request = PynixdHostHealthStreamRequest(interval_ms=int(self.interval * 1000))
                await request.to_writer(WriteContext.from_conn(conn))
                await conn.w.drain()
                async for msg in conn.r.read_stderr():
                    if isinstance(msg, LogError):
                        raise BackendError(msg.msg)
                    if not isinstance(msg, LogNext):
                        continue
                    health = HEALTH_JSON.validate_json(msg.text)
                    # The peer's monotonic clock means nothing here; staleness is measured on arrival.
                    health.timestamp = time.monotonic()
                    scope.deadline = anyio.current_time() + silence
                    self._publish(health)
            if scope.cancelled_caught:
                log.warning("peer_health_stream_silent", store_id=getattr(self.store, "store_id", None))
                # The ranker does not age a sample; an empty one ranks the peer as unmonitored.
                self._publish(SystemHealth())
        finally:
            with anyio.CancelScope(shield=True):
                await conn.close()


SAMPLED_FILES: tuple[str, ...] = (
    "/proc/pressure/cpu",
    "/proc/pressure/memory",
    "/proc/pressure/io",
    "/proc/meminfo",
    "/proc/loadavg",
    "/sys/fs/cgroup/cpu.stat",
)
"""What one sample reads. `FILES` in `agent/src/main.rs` is the same list."""


def health_from_files(
    files: Mapping[str, str | None],
    cpu_stat_prev: CgroupCpuStat | None,
    cores: float | None,
) -> tuple[SystemHealth, CgroupCpuStat | None]:
    """One sample from the text of `SAMPLED_FILES`, and the CPU counter for the next."""
    parts = [files.get(f"/proc/pressure/{name}") for name in ("cpu", "memory", "io")]
    psi = parse_psi_output("\n".join(p for p in parts if p)) if all(parts) else None
    text = files.get("/proc/meminfo")
    meminfo = parse_meminfo(text) if text else None
    text = files.get("/proc/loadavg")
    load1 = parse_loadavg(text) if text else None
    cpu_util = None
    text = files.get("/sys/fs/cgroup/cpu.stat")
    stat = parse_cpu_stat(text) if text else cpu_stat_prev
    if text and cpu_stat_prev and stat:
        cpu_util = compute_cpu_util(cpu_stat_prev, stat, cores)
    health = SystemHealth(
        psi=psi,
        meminfo=meminfo,
        cpu_util=cpu_util,
        cores=cores,
        load1=load1,
        timestamp=time.monotonic(),
    )
    return health, stat


async def local_read(path: str) -> str | None:
    try:
        return Path(path).read_text()  # noqa: ASYNC240 — /proc and /sys reads do not block
    except OSError:
        return None


def local_disk(path: Path) -> Callable[[], Awaitable[DiskSpace | None]]:
    """A disk sampler of *path* on this machine."""

    async def sample() -> DiskSpace | None:
        try:
            return DiskSpace.from_statvfs(os.statvfs(path))
        except OSError:
            return None

    return sample


def create_monitor(settings: PressureSettings, on_sample: Callable[[SystemHealth], None]) -> ResourceMonitor:
    """A poller of this machine's own files."""
    return GenericResourcePoller(settings, on_sample, local_read)
