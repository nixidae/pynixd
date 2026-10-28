"""Growth benchmark probe: one impure build, repeated, nothing may grow.

Runs on the bench guest against the system pynixd. The first build is
warmup; every later build must cost about the same as the first measured
one: wall time, daemon CPU, Python allocations, live objects, RSS, and wire
bytes. An absolute cap also bounds a single build, so a quadratic blowup
fails even when it is stable across iterations.

Usage: `growth_probe.py <socket> <nixpkgs> <expression>`, with
`PYNIXD_UNIX_PATH` naming the same socket for `pynixd state`.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

WARMUP = 1
MEASURED = 5
LOG_LINES = 50_000
BLOB_BYTES = 1_048_576
SETTLE_SECONDS = 2.0
WALL_CAP = 120.0
CPU_CAP = 120.0
"""Absolute backstops for one build. Clean builds take ~20 s; the growth
rule above only sees change between iterations, so a constant-cost blowup
(flat but 10x too slow) needs a cap to fail on. Six times headroom."""

REL_TOL = {
    "wall": 0.25,
    "cpu": 0.30,
    "tm_current": 0.10,
    "tm_peak": 0.05,
    "gc_objects": 0.10,
}
RSS_SLACK = 50 * 2**20
"""Bytes of RSS headroom over the baseline. Arenas retain; they must not grow."""


def assess(rows: list[dict[str, float]]) -> list[str]:
    """Violations of the growth rule against the first measured row.

    Pure, so the unit suite pins it with synthetic rows. Relative metrics
    must stay within tolerance of the baseline, RSS within an absolute
    slack, and the exact metrics (wire bytes, log lines, output size)
    must match the baseline to the byte: identical work costs identical
    bytes.
    """
    violations: list[str] = []
    if not rows:
        return ["no measured builds"]
    baseline = rows[0]
    for index, row in enumerate(rows[1:], start=2):
        for name, tol in REL_TOL.items():
            if row[name] > baseline[name] * (1.0 + tol):
                violations.append(f"build {index}: {name} grew {baseline[name]:.0f} -> {row[name]:.0f} (tol {tol:.0%})")
        if row["rss"] > baseline["rss"] + RSS_SLACK:
            violations.append(f"build {index}: rss grew {baseline['rss'] / 2**20:.0f} -> {row['rss'] / 2**20:.0f} MiB")
        for name in ("wire_in", "wire_out", "log_lines", "out_bytes"):
            if row[name] != baseline[name]:
                violations.append(f"build {index}: {name} {baseline[name]:.0f} != {row[name]:.0f}")
        if row["wall"] > WALL_CAP:
            violations.append(f"build {index}: wall {row['wall']:.1f}s over cap {WALL_CAP:.0f}s")
        if row["cpu"] > CPU_CAP:
            violations.append(f"build {index}: cpu {row['cpu']:.1f}s over cap {CPU_CAP:.0f}s")
    return violations


def _daemon_pid() -> int:
    out = subprocess.run(
        ["systemctl", "show", "-p", "MainPID", "--value", "pynixd.service"],
        capture_output=True,
        text=True,
        check=True,
    )
    return int(out.stdout.strip())


def _cpu_seconds(pid: int) -> float:
    with open(f"/proc/{pid}/stat") as handle:
        fields = handle.read().split()
    ticks = int(fields[13]) + int(fields[14])
    return ticks / os.sysconf("SC_CLK_TCK")


def _rss_bytes(pid: int) -> int:
    with open(f"/proc/{pid}/status") as handle:
        for line in handle:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError(f"no VmRSS for pid {pid}")


def _state(socket: str) -> dict:
    env = dict(os.environ, PYNIXD_UNIX_PATH=socket)
    out = subprocess.run(
        ["pynixd", "state", "--section", "benchmark", "--section", "transfers"],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    return json.loads(out.stdout)


def _wire_totals(state: dict) -> tuple[int, int]:
    wire_in = wire_out = 0
    for store in state["transfers"]["stores"].values():
        wire_in += store.get("wire_bytes_in", 0)
        wire_out += store.get("wire_bytes_out", 0)
    return wire_in, wire_out


def _one_build(socket: str, nixpkgs: str, expression: str, iteration: int) -> tuple[str, int]:
    """Build once as tester. Returns the output path and the log line count.

    `--print-build-logs`: `nix build` prints builder output only then, and
    only the marker lines are counted, not nix's own chatter.
    """
    inner = (
        f"NIX_PATH=nixpkgs={nixpkgs} NIX_REMOTE=unix://{socket} "
        f"GROWTH_ITER={iteration:03d} "
        f"nix build --impure --print-build-logs --file {expression} --no-link --print-out-paths"
    )
    try:
        proc = subprocess.run(
            ["su", "tester", "-c", inner],
            capture_output=True,
            text=True,
            timeout=WALL_CAP,
        )
    except subprocess.TimeoutExpired:
        raise AssertionError(f"build {iteration} exceeded cap {WALL_CAP:.0f}s") from None
    if proc.returncode != 0:
        raise AssertionError(f"build {iteration} failed rc={proc.returncode}:\n{proc.stderr[-2000:]}")
    lines = sum(1 for line in proc.stderr.splitlines() if "growth log line " in line)
    return proc.stdout.strip().split()[-1], lines


def main(socket: str, nixpkgs: str, expression: str) -> int:
    pid = _daemon_pid()
    tracing = _state(socket)["benchmark"].get("tracing", False)
    if not tracing:
        print("benchmark section is not tracing: restart pynixd with PYNIXD_BENCH=1", flush=True)
        return 1

    rows: list[dict[str, float]] = []
    for iteration in range(WARMUP + MEASURED):
        before_cpu = _cpu_seconds(pid)
        before_state = _state(socket)
        start = time.monotonic()
        out_path, log_lines = _one_build(socket, nixpkgs, expression, iteration)
        wall = time.monotonic() - start
        after_state = _state(socket)
        cpu = _cpu_seconds(pid) - before_cpu
        bench = after_state["benchmark"]
        before_wire_in, before_wire_out = _wire_totals(before_state)
        after_wire_in, after_wire_out = _wire_totals(after_state)
        blob = os.path.join(out_path, "blob")
        out_bytes = float(os.path.getsize(blob))
        if log_lines != LOG_LINES:
            print(f"build {iteration}: got {log_lines} log lines, want {LOG_LINES}", flush=True)
            return 1
        row = {
            "wall": wall,
            "cpu": cpu,
            "tm_current": float(bench["tracemalloc_current"]),
            "tm_peak": float(bench["tracemalloc_peak"]),
            "gc_objects": float(bench["gc_objects"]),
            "rss": float(_rss_bytes(pid)),
            "wire_in": float(after_wire_in - before_wire_in),
            "wire_out": float(after_wire_out - before_wire_out),
            "log_lines": float(log_lines),
            "out_bytes": out_bytes,
        }
        tag = "warmup" if iteration == 0 else f"measure {iteration}"
        print(
            f"[{tag}] wall={wall:7.1f}s cpu={cpu:7.1f}s "
            f"tm_cur={row['tm_current'] / 2**20:7.1f}MiB tm_peak={row['tm_peak'] / 2**20:7.1f}MiB "
            f"gc={row['gc_objects']:.0f} rss={row['rss'] / 2**20:6.0f}MiB "
            f"wire={row['wire_in'] + row['wire_out']:.0f}B",
            flush=True,
        )
        if iteration > 0:
            rows.append(row)
        time.sleep(SETTLE_SECONDS)

    violations = assess(rows)
    if violations:
        print("[growth] FAILED:", flush=True)
        for violation in violations:
            print(f"  {violation}", flush=True)
        return 1
    print(f"[growth] passed: {MEASURED} builds, nothing grew", flush=True)
    return 0


if __name__ == "__main__":
    _, socket_arg, nixpkgs_arg, expression_arg = sys.argv
    sys.exit(main(socket_arg, nixpkgs_arg, expression_arg))
