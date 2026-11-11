"""A cgroup per build, below pynixd's own when that one is delegated to it (nixidae/nixidae#1187).

cgroup v2 lets no cgroup that holds a process enable a controller for its children, so pynixd first moves
its own processes into a `pynixd` leaf, as Nix's daemon moves itself into `nix-daemon` (`daemon.cc:266`).
The helper joins the build's cgroup itself (`child.py`), before it forks the builder.
"""

from __future__ import annotations

import contextlib
import errno
import functools
import itertools
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import anyio.to_thread
import structlog

from .usage import BuildUsage, UsageSample, UsageSource

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

ROOT = Path("/sys/fs/cgroup")
LEAF = "pynixd"
CONTROLLERS = ("cpu", "memory", "io")
REMOVE_TRIES = 100
REMOVE_PAUSE = 0.01
"""Seconds between two tries of `rmdir`: the kernel keeps a cgroup busy until each killed process is gone."""
SETTLE_SECONDS = 2.0
"""How long `_base` keeps moving processes into the leaf. The functional suite's workers fork at once at its start,
and a fixed ten tries with no pause lose that race: every build of the process then runs without a cgroup."""
SETTLE_PAUSE = 0.005

_log = structlog.get_logger(__name__)
_serial = itertools.count()


GUEST = Path(ROOT, "build")
"""The build's cgroup in the build VM. `nix/krun/init.c` makes it and gives it to the build's uid."""


def own_cgroup(proc_self_cgroup: str) -> str | None:
    """The v2 path in `/proc/self/cgroup`: the line `0::<path>`."""
    for line in proc_self_cgroup.splitlines():
        if line.startswith("0::"):
            return line[3:]
    return None


@functools.cache
def _base() -> Path | None:
    """The delegated cgroup that build cgroups go below, made ready once per process; `None` when there is none."""
    try:
        path = own_cgroup(Path("/proc/self/cgroup").read_text())
    except OSError:
        return None
    if path is None:
        return None
    own = ROOT / path.lstrip("/")
    # A second pynixd process in the same delegation, such as a test's, finds itself in the leaf already.
    if own.name == LEAF:
        own = own.parent
    if not all(os.access(own / name, os.W_OK) for name in ("cgroup.procs", "cgroup.subtree_control")):
        return None
    try:
        available = (own / "cgroup.controllers").read_text().split()
        if "memory" not in available:
            _log.info("build_cgroups_off", cgroup=str(own), reason="no memory controller", available=available)
            return None
        leaf = own / LEAF
        leaf.mkdir(exist_ok=True)
        enable = " ".join(f"+{name}" for name in CONTROLLERS if name in available)
        # A process that forks while the others move lands in *own*, and the write below then fails with EBUSY.
        # `_make` runs in a worker thread, so the pause blocks no event loop.
        deadline = time.monotonic() + SETTLE_SECONDS
        while time.monotonic() < deadline:
            for pid in (own / "cgroup.procs").read_text().split():
                with contextlib.suppress(ProcessLookupError):
                    (leaf / "cgroup.procs").write_text(pid)
            try:
                (own / "cgroup.subtree_control").write_text(enable)
            except OSError as e:
                if e.errno != errno.EBUSY:
                    raise
                time.sleep(SETTLE_PAUSE)
                continue
            return own
    except OSError as e:
        _log.warning("build_cgroups_off", cgroup=str(own), error=str(e))
        return None
    _log.warning("build_cgroups_off", cgroup=str(own), reason="processes kept arriving")
    return None


def _make() -> Path | None:
    base = _base()
    if base is None:
        return None
    path = base / f"build-{os.getpid()}-{next(_serial)}"
    try:
        path.mkdir()
    except OSError as e:
        _log.warning("build_cgroup_failed", cgroup=str(path), error=str(e))
        return None
    return path


def _fields(text: str) -> dict[str, int]:
    """`key value` lines, as `cpu.stat` holds them."""
    return {key: int(value) for key, value in (line.split() for line in text.splitlines() if line)}


def io_bytes(io_stat: str) -> tuple[int, int]:
    """The bytes read and written, summed over the devices of `io.stat`."""
    read = written = 0
    for line in io_stat.splitlines():
        for word in line.split()[1:]:
            key, _, value = word.partition("=")
            if key == "rbytes":
                read += int(value)
            elif key == "wbytes":
                written += int(value)
    return read, written


def stall_us(pressure: str) -> int:
    """The `total=` of the `some` line: microseconds in which at least one task waited."""
    for line in pressure.splitlines():
        kind, *words = line.split()
        if kind == "some":
            return next(int(word[6:]) for word in words if word.startswith("total="))
    raise ValueError(f"no 'some' line in {pressure!r}")


def _stall(path: Path) -> int | None:
    """`None` without PSI: the kernel has no `CONFIG_PSI`, or boots with `psi=0`."""
    try:
        return stall_us(path.read_text())
    except OSError:
        return None


def _io(path: Path) -> tuple[int, int]:
    # No io controller, no `io.stat`: the build is then counted as reading nothing from a disk.
    io_stat = path / "io.stat"
    return io_bytes(io_stat.read_text()) if io_stat.exists() else (0, 0)


def sample(path: Path, at_ms: int) -> UsageSample:
    io_read, io_written = _io(path)
    return UsageSample(
        at_ms=at_ms,
        cpu_us=_fields((path / "cpu.stat").read_text())["usage_usec"],
        memory_bytes=int((path / "memory.current").read_text()),
        io_read_bytes=io_read,
        io_write_bytes=io_written,
    )


async def sample_every(path: Path, seconds: float, into: list[UsageSample], *, in_thread: bool = True) -> None:
    """Append a `sample` of *path* to *into* every *seconds*, until cancelled.

    `in_thread=False` reads on the loop: a process that ran `unshare(CLONE_NEWPID)` gets
    EINVAL from `clone(CLONE_THREAD)`, so the helper can start no thread.
    """
    started = time.monotonic()
    while True:
        await anyio.sleep(seconds)
        at_ms = round((time.monotonic() - started) * 1000)
        taken = (
            await anyio.to_thread.run_sync(_sample_populated, path, at_ms)
            if in_thread
            else _sample_populated(path, at_ms)
        )
        if taken is not None:
            into.append(taken)


def _sample_populated(path: Path, at_ms: int) -> UsageSample | None:
    """A `sample`, or `None` while the cgroup holds no process: before the helper joins and after the build ends."""
    if "populated 1" not in (path / "cgroup.events").read_text():
        return None
    return sample(path, at_ms)


def read(path: Path) -> BuildUsage:
    cpu = _fields((path / "cpu.stat").read_text())
    io_read, io_written = _io(path)
    return BuildUsage(
        source=UsageSource.CGROUP,
        cpu_user_us=cpu["user_usec"],
        cpu_system_us=cpu["system_usec"],
        peak_memory_bytes=int((path / "memory.peak").read_text()),
        io_read_bytes=io_read,
        io_write_bytes=io_written,
        cpu_stall_us=_stall(path / "cpu.pressure"),
        memory_stall_us=_stall(path / "memory.pressure"),
        io_stall_us=_stall(path / "io.pressure"),
    )


async def _remove(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        (path / "cgroup.kill").write_text("1")
    for _ in range(REMOVE_TRIES):
        try:
            path.rmdir()
        except FileNotFoundError:
            return
        except OSError as e:
            if e.errno != errno.EBUSY:
                raise
            await anyio.sleep(REMOVE_PAUSE)
        else:
            return
    _log.warning("build_cgroup_left", cgroup=str(path))


@contextlib.asynccontextmanager
async def build_cgroup() -> AsyncIterator[Path | None]:
    """A new cgroup for one build, or `None`; on exit every process in it is killed and the cgroup removed."""
    path = await anyio.to_thread.run_sync(_make)
    try:
        yield path
    finally:
        if path is not None:
            with anyio.CancelScope(shield=True), anyio.move_on_after(REMOVE_TRIES * REMOVE_PAUSE * 2):
                await _remove(path)
