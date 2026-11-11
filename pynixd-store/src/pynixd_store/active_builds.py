"""The builds that pynixd's own builder runs now, as Determinate Nix's `QueryActiveBuilds` answers them.

Determinate keeps a file per build in `<stateDir>/active-builds`, because its builds run in many forked
workers. pynixd builds in one process, so the record stays in memory and no reader races a writer.

NIX-DEFECT (#23): `LocalStore::buildStarted` (`local-store-active-builds.cc:297` of Determinate v3.24.0)
creates the file, locks it at `:300`, and writes it by path at `:305`. A `queryActiveBuilds` between the
create and the lock gets the lock and deletes the file (`:242`); the write at `:305` then makes a new file
that no lock holds, which the next query deletes too, so the build never shows. A query between the lock
and the write parses an empty file (`:246`) and logs an error. pynixd answers from memory and has neither
race. Lillecarl/nix#419. nixidae/nixidae#1250.
"""

from __future__ import annotations

import contextlib
import itertools
import os
import pwd
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator

_CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")


@dataclass(frozen=True)
class ActiveBuild:
    """`ActiveBuild` (`active-builds.hh:26`): what is known when the builder starts."""

    main_pid: int
    """The helper (`builder/child.py`), which runs the builder; for a VM build, the VMM."""
    main_uid: int
    cgroup: Path | None
    start_time: int
    derivation: str
    """The derivation's base name: `StorePath::to_string` (`active-builds.cc:146`)."""


def _user(uid: int) -> dict[str, Any]:
    """`UserInfo` (`active-builds.cc:39`): a uid without a name in the password database has a null one."""
    try:
        name: str | None = pwd.getpwuid(uid).pw_name
    except KeyError:
        name = None
    return {"uid": uid, "name": name}


def _seconds(ticks: str) -> float:
    return int(ticks) / _CLOCK_TICKS


def process_info(pid: int) -> dict[str, Any]:
    """`getProcessInfo` (`local-store-active-builds.cc:22`). Raises `OSError` for a process that is gone."""
    proc = Path(f"/proc/{pid}")
    raw = (proc / "cmdline").read_bytes()
    # The owner of the directory, not of `stat`: the kernel gives a zombie's files to root (`:44`).
    uid = proc.stat().st_uid
    stat = (proc / "stat").read_text()
    # After the last `)`: a command name may hold one, which Nix's regex at `:56` refuses.
    fields = stat.rpartition(") ")[2].split()
    resident = (proc / "statm").read_text().split()
    times: list[float | None] = [_seconds(field) for field in fields[11:15]] if len(fields) > 14 else [None] * 4
    utime, stime, cutime, cstime = times
    return {
        "pid": pid,
        "parentPid": int(fields[1]) if len(fields) > 1 else 0,
        "user": _user(uid),
        # `tokenizeString` drops empty arguments. The JSON must be UTF-8, and nlohmann refuses a lone surrogate.
        "argv": [arg.decode(errors="replace") for arg in raw.split(b"\0") if arg],
        "utime": utime,
        "stime": stime,
        "cutime": cutime,
        "cstime": cstime,
        "rss": int(resident[1]) * _PAGE_SIZE if len(resident) > 1 else None,
    }


def descendants(pid: int) -> set[int]:
    """`getDescendantPids` (`local-store-active-builds.cc:102`): *pid* and every child below it."""
    found: set[int] = set()
    todo = [pid]
    while todo:
        current = todo.pop()
        found.add(current)
        # The process may be gone.
        with contextlib.suppress(OSError):
            children = Path(f"/proc/{current}/task/{current}/children").read_text().split()
            todo.extend(int(child) for child in children if int(child) not in found)
    return found


def _keyed(text: str) -> dict[str, int]:
    pairs = (line.split() for line in text.splitlines())
    return {pair[0]: int(pair[1]) for pair in pairs if len(pair) == 2 and pair[1].isdigit()}


def cgroup_usage(cgroup: Path) -> dict[str, Any]:
    """`getCgroupStats` (`cgroup.cc:52`): CPU from `cpu.stat`, and anonymous plus mapped file memory."""
    usage: dict[str, Any] = {"utime": None, "stime": None, "rss": None}
    with contextlib.suppress(FileNotFoundError):
        cpu = _keyed((cgroup / "cpu.stat").read_text())
        if "user_usec" in cpu:
            usage["utime"] = cpu["user_usec"] / 1_000_000
        if "system_usec" in cpu:
            usage["stime"] = cpu["system_usec"] / 1_000_000
    with contextlib.suppress(FileNotFoundError):
        memory = _keyed((cgroup / "memory.stat").read_text())
        if "anon" in memory and "file_mapped" in memory:
            usage["rss"] = memory["anon"] + memory["file_mapped"]
    return usage


def build_info(build: ActiveBuild) -> dict[str, Any]:
    """`ActiveBuildInfo` as `active-builds.cc:160` writes it. `from_json` (`:115`) needs every key but the usage.

    `clientPid` and `clientUid` are null: Nix fills neither (`derivation-builder.cc:538`), and a build of
    pynixd's queue outlives the client that asked for it.
    """
    usage: dict[str, Any] = {"utime": None, "stime": None, "rss": None}
    if build.cgroup is not None:
        try:
            pids = {int(pid) for pid in (build.cgroup / "cgroup.procs").read_text().split()}
        except FileNotFoundError:
            pids = set()
        usage = cgroup_usage(build.cgroup)
    else:
        pids = descendants(build.main_pid)
    processes = []
    for pid in sorted(pids):
        # The process may have ended since the listing.
        with contextlib.suppress(OSError):
            processes.append(process_info(pid))
    return {
        "type": "build",
        "nixPid": os.getpid(),
        "clientPid": None,
        "clientUid": None,
        "mainPid": build.main_pid,
        "mainUser": _user(build.main_uid),
        "cgroup": None if build.cgroup is None else str(build.cgroup),
        "startTime": float(build.start_time),
        "derivation": build.derivation,
        "processes": processes,
        **usage,
    }


class ActiveBuilds:
    """The builds in progress of one store's builder."""

    def __init__(self) -> None:
        self._builds: dict[int, ActiveBuild] = {}
        self._serial = itertools.count(1)

    @contextlib.contextmanager
    def running(self, build: ActiveBuild) -> Iterator[None]:
        """*build* is active for the body."""
        key = next(self._serial)
        self._builds[key] = build
        try:
            yield
        finally:
            del self._builds[key]

    def snapshot(self) -> tuple[ActiveBuild, ...]:
        return tuple(self._builds.values())


def query(builds: tuple[ActiveBuild, ...]) -> list[dict[str, Any]]:
    """`LocalStore::queryActiveBuilds` (`local-store-active-builds.cc:231`) over *builds*. It reads `/proc`: blocking."""
    return [build_info(build) for build in builds]
