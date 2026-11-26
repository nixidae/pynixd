"""A build pod's cores and requests from its build's history (`pynixd/pod_size.py`)."""

from __future__ import annotations

from pynixd.pod_size import MIN_CPU_MILLIS, MIN_MEMORY_BYTES, SizeSource, size_pod
from pynixd_store.memory_model import MemoryModel

GIB = 2**30


def test_a_build_with_no_history_gets_the_store_s_cores_and_the_template_s_requests() -> None:
    """A sibling pod built with every core of its node, 32 on sg1, and asked for 100m."""
    size = size_pod(cores=8, memory_bytes=16 * GIB, model=None, cores_used=None)

    assert (size.cores, size.cpu_millis, size.memory_bytes) == (8, None, None)
    assert size.cpu_source == size.memory_source == SizeSource.TEMPLATE


def test_the_memory_request_belongs_to_the_cores_the_build_gets() -> None:
    model = MemoryModel(1 * GIB, 2 * GIB)

    size = size_pod(cores=8, memory_bytes=None, model=model, cores_used=None)
    assert (size.cores, size.memory_bytes) == (8, model.peak(8))
    # At most 16 GiB: 7 cores need 15 GiB, 8 need 17.
    bounded = size_pod(cores=8, memory_bytes=16 * GIB, model=model, cores_used=None)
    assert (bounded.cores, bounded.memory_bytes) == (7, model.peak(7))
    assert bounded.memory_source == SizeSource.HISTORY
    # Not even one core fits: one core, and the bound.
    huge = size_pod(cores=8, memory_bytes=2 * GIB, model=MemoryModel(4 * GIB, GIB), cores_used=None)
    assert (huge.cores, huge.memory_bytes) == (1, 2 * GIB)
    assert size_pod(cores=8, memory_bytes=None, model=MemoryModel(0, 1), cores_used=None).memory_bytes == (
        MIN_MEMORY_BYTES
    )


def test_the_cpu_request_is_what_the_build_kept_busy() -> None:
    assert size_pod(cores=8, memory_bytes=None, model=None, cores_used=1.16).cpu_millis == 1200
    assert size_pod(cores=8, memory_bytes=None, model=None, cores_used=0.01).cpu_millis == MIN_CPU_MILLIS
    # Never more than the cores the build gets.
    assert size_pod(cores=4, memory_bytes=None, model=None, cores_used=30.0).cpu_millis == 4000


def test_the_client_s_cores_and_a_kill_s_bound_the_cores() -> None:
    assert size_pod(cores=8, memory_bytes=None, model=None, cores_used=None, ceilings=(2, None)).cores == 2
    assert size_pod(cores=8, memory_bytes=None, model=None, cores_used=None, ceilings=(0, 3)).cores == 3


def test_a_pod_no_node_took_asks_for_the_template_s_requests_next() -> None:
    """A history-sized request that no node has room for waits `schedule_seconds`, then the same pod again."""
    model = MemoryModel(GIB, GIB)
    first = size_pod(cores=8, memory_bytes=None, model=model, cores_used=6.0)
    second = size_pod(cores=8, memory_bytes=None, model=model, cores_used=6.0, unplaced=True)

    assert (first.cpu_millis, first.memory_bytes) == (6000, model.peak(8))
    assert (second.cores, second.cpu_millis, second.memory_bytes) == (8, None, None)
