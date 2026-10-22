"""A pynixd peer streams its host's samples to the controller. nixidae/nixidae#1016.

The peer forwards each sample of its own monitor, so a PSI trigger there
reaches the controller at once, with nothing but pynixd on the peer.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import anyio
import pytest

from nix_daemon_protocol.constants import PROTOCOL_VERSION
from pynixd.config import PressureSettings
from pynixd.daemon_extensions.pynixd_host_health import HEALTH_JSON, PynixdHostHealthStreamRequest
from pynixd.handlers.pynixd_host_health import PynixdHostHealthStreamHandler
from pynixd.monitor import PeerHealthMonitor
from pynixd.psi import DiskSpace, SystemHealth
from pynixd.serde import LogNext
from pynixd.serde.auth import Role
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.wire import BytesReader, BytesWriter

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path


class Client:
    """The controller's end of the session: records each sample."""

    def __init__(self, wanted: int) -> None:
        self.samples: list[SystemHealth] = []
        self.wanted = wanted
        self.first = anyio.Event()
        self.done = anyio.Event()

    async def send(self, msg: LogNext) -> None:
        self.samples.append(HEALTH_JSON.validate_json(msg.text))
        self.first.set()
        if len(self.samples) >= self.wanted:
            self.done.set()


async def _ctx(local: Any, client: Client, interval_ms: int) -> Any:
    writer = BytesWriter("test")
    request = PynixdHostHealthStreamRequest(interval_ms=interval_ms)
    await request.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    proxy = SimpleNamespace(
        r=BytesReader(writer.get_bytes()[8:], "test"),
        version=PROTOCOL_VERSION,
        standard_features=frozenset(),
        local_store=local,
        client=client,
    )
    return SimpleNamespace(proxy=proxy, role=Role.ADMIN)


def _local(monitor: Any, tmp_path: Path) -> Any:
    return SimpleNamespace(
        monitor=monitor,
        sample_listeners=set(),
        pressure_settings=PressureSettings(),
        layout=SimpleNamespace(real_store_dir=tmp_path),
    )


@pytest.mark.anyio
async def test_the_peer_forwards_each_sample_of_its_monitor(tmp_path: Path):
    local = _local(SimpleNamespace(interval=1.0, health=SystemHealth(load1=0.1)), tmp_path)
    client = Client(wanted=2)
    ctx = await _ctx(local, client, interval_ms=2000)

    async with anyio.create_task_group() as tg:
        tg.start_soon(PynixdHostHealthStreamHandler().handle, ctx)
        with anyio.fail_after(5):
            await client.first.wait()
            for listener in tuple(local.sample_listeners):
                listener(SystemHealth(load1=0.7, disk=DiskSpace(free_bytes=5, total_bytes=9)))
            await client.done.wait()
        tg.cancel_scope.cancel()

    assert [sample.load1 for sample in client.samples] == [0.1, 0.7]
    assert client.samples[1].disk == DiskSpace(free_bytes=5, total_bytes=9)
    assert local.sample_listeners == set(), "a closed session must stop listening"


@pytest.mark.anyio
@pytest.mark.parametrize("monitor", ["off", "slower"])
async def test_the_peer_samples_itself_when_its_monitor_cannot_serve(tmp_path: Path, monitor: str):
    own = None if monitor == "off" else SimpleNamespace(interval=60.0, health=SystemHealth())
    local = _local(own, tmp_path)
    client = Client(wanted=2)
    ctx = await _ctx(local, client, interval_ms=50)

    async with anyio.create_task_group() as tg:
        tg.start_soon(PynixdHostHealthStreamHandler().handle, ctx)
        with anyio.fail_after(5):
            await client.done.wait()
        tg.cancel_scope.cancel()

    assert client.samples[0].meminfo is not None
    assert client.samples[0].disk is not None
    assert local.sample_listeners == set()


class PeerReader:
    """The peer's end: one sample, then nothing."""

    async def read_stderr(self) -> AsyncIterator[LogNext]:
        sample = SystemHealth(load1=0.5, disk=DiskSpace(free_bytes=5, total_bytes=9), timestamp=1.0)
        yield LogNext(text=HEALTH_JSON.dump_json(sample).decode())
        await anyio.sleep_forever()


class Conn:
    """A session outside the pools, as `create_conn` opens one."""

    def __init__(self) -> None:
        self.closed = False
        self.r = PeerReader()
        self.w = BytesWriter("controller")
        self.version = PROTOCOL_VERSION
        self.standard_features: frozenset[str] = frozenset()

    async def close(self) -> None:
        self.closed = True


@pytest.mark.anyio
async def test_the_controller_publishes_samples_and_drops_a_silent_stream():
    conn = Conn()

    class Peer:
        store_id = "reverse-1"
        features = frozenset({PynixdHostHealthStreamRequest.name})

        async def create_conn(self) -> Conn:
            return conn

    samples: list[SystemHealth] = []
    monitor = PeerHealthMonitor(PressureSettings(interval=0.05), samples.append, Peer())

    with anyio.fail_after(5):
        await monitor._stream()  # noqa: SLF001 -- one session is the unit

    assert [sample.load1 for sample in samples] == [0.5, None]
    assert samples[0].disk == DiskSpace(free_bytes=5, total_bytes=9)
    assert samples[1].psi is None and samples[1].cpu_util is None, "a silent peer reads as unmonitored"
    assert samples[0].timestamp > 1.0, "staleness is measured on arrival, not on the peer's clock"
    assert conn.closed
    sent = await PynixdHostHealthStreamRequest.from_reader(
        ReadContext(reader=BytesReader(conn.w.get_bytes()[8:], "peer"), version=PROTOCOL_VERSION)
    )
    assert sent.interval_ms == 50


@pytest.mark.anyio
async def test_a_peer_without_either_operation_reads_as_unmonitored():
    class Peer:
        store_id = "old-peer"
        features: frozenset[str] = frozenset()

    samples: list[SystemHealth] = []
    monitor = PeerHealthMonitor(PressureSettings(interval=0.01), samples.append, Peer())
    monitor.running = True
    with anyio.move_on_after(0.1):
        await monitor.run()

    assert samples
    assert all(sample.psi is None and sample.cpu_util is None for sample in samples)
