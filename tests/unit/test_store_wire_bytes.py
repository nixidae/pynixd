"""Connection wire bytes, counted at the transport boundary.

Three layers of accumulation. The transports expose no cumulative
counters, so the transport-boundary classes count their own: writers in
`_write_to_transport` (one int-add per coalesced flush, framed bytes
included), readers where the transport fills (readahead counts once at
fill, never again from the buffer). Each pool folds its dead
connections into two accumulators and sums the live ones on demand.
The collector keeps the highest total per store across pool
replacement, so a re-registered builder does not reset its lifetime
numbers.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from pynixd import metrics
from pynixd.background import Background
from pynixd.connection import Connection
from pynixd.metrics import StoreTrafficCollector
from pynixd.store.pool import ConnectionPool
from pynixd.wire import SSHNixReader, SSHNixWriter, UnixNixReader, UnixNixWriter


class _Raw:
    """An underlying transport yielding fixed-size chunks."""

    def __init__(self, chunk_size: int) -> None:
        self.chunk_size = chunk_size

    async def read(self, n: int) -> bytes:
        return b"z" * min(n, self.chunk_size)

    async def readexactly(self, n: int) -> bytes:
        return b"z" * n


class _Sink:
    """An underlying transport collecting what it is handed."""

    def __init__(self) -> None:
        self.data = bytearray()

    def write(self, data: bytes) -> None:
        self.data.extend(data)


def test_unix_writer_counts_flushed_bytes() -> None:
    """One int-add per flush, through the coalescing buffer too."""
    writer = UnixNixWriter(cast("Any", _Sink()))
    writer._write_to_transport(b"abc")
    assert writer.bytes_written == 3

    writer = UnixNixWriter(cast("Any", _Sink()))
    writer.write(b"x" * 70_000)
    assert writer.bytes_written == 70_000


def test_ssh_writer_counts_flushed_bytes() -> None:
    """Same choke point on the SSH side."""
    writer = SSHNixWriter(cast("Any", _Sink()))
    writer._write_to_transport(b"abc")
    assert writer.bytes_written == 3


async def test_unix_reader_counts_transport_fills_once() -> None:
    """A buffer fill counts; serving from the buffer counts nothing."""
    reader = UnixNixReader(cast("Any", _Raw(chunk_size=100)))
    assert await reader.readexactly(8) == b"z" * 8
    assert reader.bytes_read == 100
    assert await reader.readexactly(8) == b"z" * 8
    assert reader.bytes_read == 100


async def test_unix_reader_counts_a_passthrough_exactly() -> None:
    """A read past the readahead counts only the new transport bytes."""
    reader = UnixNixReader(cast("Any", _Raw(chunk_size=100)))
    assert len(await reader.readexactly(20_000)) == 20_000
    assert reader.bytes_read == 20_000


async def test_ssh_reader_counts_transport_fills_once() -> None:
    """Same semantics under the larger SSH readahead."""
    reader = SSHNixReader(cast("Any", _Raw(chunk_size=100)))
    assert await reader.readexactly(8) == b"z" * 8
    assert reader.bytes_read == 100
    assert len(await reader.readexactly(300_000)) == 300_000
    assert reader.bytes_read == 100 + 300_000 - 92


class _FakeWriter:
    def __init__(self, written: int) -> None:
        self.bytes_written = written
        self.closed = 0

    async def close(self) -> None:
        self.closed += 1


async def test_close_reports_once() -> None:
    """Overlapping removal paths close twice; the pool hears it once."""
    conn = Connection(
        cast("Any", SimpleNamespace(bytes_read=5)),
        cast("Any", _FakeWriter(7)),
        "c",
    )
    reported: list[Connection] = []
    conn.on_close = reported.append
    await conn.close()
    await conn.close()
    assert reported == [conn]


def _pool() -> ConnectionPool:
    async def factory() -> Connection:
        raise AssertionError("aggregation never creates")

    return ConnectionPool(
        store_id="wire-pool",
        factory=factory,
        background=Background("test"),
    )


def test_pool_folds_live_and_retired() -> None:
    """Live conns sum from their readers; dead ones from the accumulators."""
    pool = _pool()
    assert pool.traffic_totals() == (0, 0)
    pool.wire_bytes_read += 10
    pool.wire_bytes_written += 20
    live = SimpleNamespace(
        r=SimpleNamespace(bytes_read=1),
        w=SimpleNamespace(bytes_written=2),
    )
    pool.all_conns.append(cast("Any", live))
    assert pool.traffic_totals() == (11, 22)


def test_note_conn_closed_accumulates() -> None:
    """The close hook feeds the retired accumulators."""
    pool = _pool()
    conn = SimpleNamespace(
        r=SimpleNamespace(bytes_read=30),
        w=SimpleNamespace(bytes_written=40),
    )
    pool._note_conn_closed(cast("Any", conn))
    assert (pool.wire_bytes_read, pool.wire_bytes_written) == (30, 40)


class _FakePool:
    """A pool double the weak set can hold."""

    def __init__(self, store_id: str, totals: tuple[int, int]) -> None:
        self.store_id = store_id
        self._totals = totals

    def traffic_totals(self) -> tuple[int, int]:
        return self._totals


def test_collector_sums_one_store_once() -> None:
    """Two pools of one store merge; the family carries both directions."""
    collector = StoreTrafficCollector()
    first = _FakePool("wire-store", (3, 4))
    second = _FakePool("wire-store", (5, 6))
    collector.register(cast("Any", first))
    collector.register(cast("Any", second))
    (family,) = list(collector.collect())
    samples = {(s.labels["direction"]): s.value for s in family.samples}
    assert samples == {"in": 8, "out": 10}


def test_collector_survives_pool_replacement() -> None:
    """A re-registered store keeps its lifetime totals, then advances."""
    collector = StoreTrafficCollector()
    old = _FakePool("wire-replaced", (10, 10))
    collector.register(cast("Any", old))
    (family,) = list(collector.collect())
    assert {(s.labels["direction"]): s.value for s in family.samples} == {"in": 10, "out": 10}

    del old
    new = _FakePool("wire-replaced", (3, 3))
    collector.register(cast("Any", new))
    (family,) = list(collector.collect())
    assert {(s.labels["direction"]): s.value for s in family.samples} == {"in": 10, "out": 10}

    new._totals = (12, 4)
    (family,) = list(collector.collect())
    assert {(s.labels["direction"]): s.value for s in family.samples} == {"in": 12, "out": 10}


def test_wire_reader_reports_zeros_for_a_quiet_store() -> None:
    """A store with no traffic is zeros, like every other reader here."""
    assert metrics.store_wire_bytes(["wire-quiet-never-touched"])["wire-quiet-never-touched"] == {
        "wire_bytes_in": 0,
        "wire_bytes_out": 0,
    }
