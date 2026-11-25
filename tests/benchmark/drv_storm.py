"""Time storms of trivial derivations through each daemon, inside the guest.

Usage: `drv_storm.py <name>=<socket>:<pid> ... --expression drv_storm.nix`. Each daemon serves a store of its own, and
the daemons take turns, so a slow minute of the host lands on every one. One JSON line per measured storm on stdout.

Instantiation is timed apart from the build: it is the client's evaluation and the daemon's `AddToStore`, and it is
the same work for a graph that a planner writes from inside a build.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Final

ROUNDS: Final = 3


@dataclasses.dataclass(frozen=True, slots=True)
class Daemon:
    """One daemon under test, and the process that serves its socket."""

    name: str
    socket: Path
    pid: int


@dataclasses.dataclass(frozen=True, slots=True)
class Storm:
    """One shape of graph: *count* derivations of *inputs* files each."""

    shape: str
    count: int
    inputs: int


@dataclasses.dataclass(frozen=True, slots=True)
class Row:
    """One storm through one daemon."""

    daemon: str
    shape: str
    count: int
    inputs: int
    round: int
    instantiate_seconds: float
    build_seconds: float
    ms_per_build: float
    own_cpu_ms: float
    """Per build: the daemon's own process. nix-daemon serves in forked workers, so it shows almost nothing here."""
    unit_cpu_ms: float
    """Per build: every process of the daemon's unit."""


STORMS: Final = (
    Storm("bare", 400, 0),
    Storm("files", 100, 1),
    Storm("files", 100, 100),
    Storm("tree", 100, 2000),
    Storm("files", 50, 2000),
)


def _daemon(text: str) -> Daemon:
    name, _, rest = text.partition("=")
    socket, _, pid = rest.rpartition(":")
    return Daemon(name, Path(socket), int(pid))


@dataclasses.dataclass(frozen=True, slots=True)
class Cpu:
    own: float
    """The daemon's own process."""
    unit: float
    """Every process of the daemon's unit: workers, helpers and builders."""

    @classmethod
    def of(cls, pid: int) -> Cpu:
        fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
        group = Path(f"/proc/{pid}/cgroup").read_text().strip().removeprefix("0::")
        # pynixd may move itself into a leaf below its unit (`builder/cgroup.py`).
        unit = next(
            p
            for p in (Path(f"/sys/fs/cgroup{group}"), *Path(f"/sys/fs/cgroup{group}").parents)
            if p.suffix == ".service"
        )
        usage = dict(line.split() for line in (unit / "cpu.stat").read_text().splitlines())
        return cls(own=sum(map(int, fields[11:13])) / os.sysconf("SC_CLK_TCK"), unit=int(usage["usage_usec"]) / 1e6)


def _nix(daemon: Daemon, expression: Path, storm: Storm, nonce: str, tree: Path, *flags: str) -> float:
    command = [
        *("nix", "build", "--store", f"unix://{daemon.socket}", "--no-link", "--impure"),
        *("--file", str(expression), "--argstr", "shell", "/bin/sh", "--argstr", "nonce", nonce),
        *("--arg", "count", str(storm.count), "--arg", "inputs", str(storm.inputs)),
        *("--argstr", "shape", storm.shape, "--arg", "tree", str(tree), *flags),
    ]
    started = time.monotonic()
    done = subprocess.run(command, capture_output=True, text=True, check=False)
    took = time.monotonic() - started
    if done.returncode != 0:
        raise RuntimeError(f"{daemon.name}, {storm}: {' '.join(command)}\n{done.stderr[-4000:]}")
    return took


def _measure(daemon: Daemon, expression: Path, storm: Storm, number: int, tree: Path) -> Row:
    nonce = uuid.uuid4().hex
    instantiate = _nix(daemon, expression, storm, nonce, tree, "--dry-run")
    before = Cpu.of(daemon.pid)
    build = _nix(daemon, expression, storm, nonce, tree)
    after = Cpu.of(daemon.pid)
    return Row(
        daemon=daemon.name,
        shape=storm.shape,
        count=storm.count,
        inputs=storm.inputs,
        round=number,
        instantiate_seconds=round(instantiate, 3),
        build_seconds=round(build, 3),
        ms_per_build=round(build * 1000 / storm.count, 1),
        own_cpu_ms=round((after.own - before.own) * 1000 / storm.count, 1),
        unit_cpu_ms=round((after.unit - before.unit) * 1000 / storm.count, 1),
    )


def _tree(directory: Path, files: int) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for index in range(files):
        (directory / f"h{index}.h").write_text(f"/* {index} */\n")
    return directory


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("daemons", nargs="+", type=_daemon)
    parser.add_argument("--expression", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=ROUNDS)
    options = parser.parse_args()
    daemons: list[Daemon] = options.daemons
    for storm in STORMS:
        tree = _tree(options.scratch / f"tree-{storm.inputs}", storm.inputs)
        # The inputs themselves are copied before the first measured round.
        for daemon in daemons:
            _nix(daemon, options.expression, dataclasses.replace(storm, count=1), uuid.uuid4().hex, tree)
        for number in range(options.rounds):
            order = daemons if number % 2 == 0 else daemons[::-1]
            for daemon in order:
                row = _measure(daemon, options.expression, storm, number, tree)
                print(json.dumps(dataclasses.asdict(row)), flush=True)
                print(row, file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
