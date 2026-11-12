"""What each process under the test runner waits on, read from `/proc`, for a test that is about to time out.

The deadline cancels the test, and the cancel tears down every daemon the test
started. So the census runs before the deadline, from a timer thread
(nixidae/nixidae#1283). No gdb: the hosts set `kernel.yama.ptrace_scope` to 1,
so a debugger may attach only to its own descendants.
"""

from __future__ import annotations

import os
from pathlib import Path

_PROC = Path("/proc")


def _read(path: Path) -> str:
    try:
        return path.read_text(errors="replace").strip()
    except OSError as error:
        return f"<{error.strerror}>"


def _parents(proc: Path) -> dict[int, int]:
    """Each process's parent, from its `status`: `task/*/children` needs `CONFIG_PROC_CHILDREN`."""
    parents = {}
    for entry in proc.glob("[0-9]*"):
        for line in _read(entry / "status").splitlines():
            if line.startswith("PPid:"):
                parents[int(entry.name)] = int(line.split()[1])
    return parents


def descendants(pid: int, proc: Path = _PROC) -> list[int]:
    """Every process below *pid*, parents before their children."""
    parents = _parents(proc)
    order: list[int] = []
    pending = sorted(child for child, parent in parents.items() if parent == pid)
    while pending:
        child = pending.pop(0)
        order.append(child)
        pending.extend(sorted(grand for grand, parent in parents.items() if parent == child))
    return order


def _fds(proc: Path, pid: int) -> list[str]:
    lines = []
    for fd in sorted((proc / str(pid) / "fd").glob("*"), key=lambda p: int(p.name) if p.name.isdigit() else -1):
        try:
            lines.append(f"{fd.name} -> {os.readlink(fd)}")
        except OSError as error:
            lines.append(f"{fd.name} <{error.strerror}>")
    return lines


def describe(pid: int, proc: Path = _PROC) -> str:
    """One process: its command line, state, and what each thread waits in."""
    base = proc / str(pid)
    status = dict(line.split(":\t", 1) for line in _read(base / "status").splitlines() if ":\t" in line)
    argv = _read(base / "cmdline").replace("\0", " ")
    lines = [f"pid {pid} ppid {status.get('PPid', '?')} {status.get('State', '?')}: {argv}"]
    for task in sorted((base / "task").glob("*"), key=lambda p: int(p.name) if p.name.isdigit() else -1):
        lines.append(f"  thread {task.name} wchan={_read(task / 'wchan')} syscall={_read(task / 'syscall')}")
    lines.extend(f"  fd {line}" for line in _fds(proc, pid))
    return "\n".join(lines)


def census(pid: int, proc: Path = _PROC) -> str:
    """*pid*'s descendants, each described, then the kernel's table of file locks."""
    parts = [describe(child, proc) for child in descendants(pid, proc)]
    parts.append("/proc/locks:\n" + _read(proc / "locks"))
    return "\n\n".join(parts) + "\n"
