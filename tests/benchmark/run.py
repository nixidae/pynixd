"""Run a benchmark inside the guest, and keep what it measured.

One script serves every phase, and `vms.phase` picks the benchmark. `decode`
and `raw` are micro-measurements; `system` is the whole pipeline through one
client, and is the one that would notice a fault the other two cannot see.
`storm` is the same pipeline under parallel clients. `growth` repeats one
impure build and fails when any cost grows between iterations.
"""

from __future__ import annotations

import shlex
import statistics
import time
from typing import TYPE_CHECKING

from vivarium_runner import Machine, Machines

if TYPE_CHECKING:
    from collections.abc import Mapping

TIMEOUT = 1800
"""Seconds for one phase."""

BUILD_TIMEOUT = 240
"""Seconds for one role's build.

The goal is a thousand trivial derivations. One build is one `execute` call,
and this cap keeps a wedged transport from holding the session open: a timed-out
build is a result, and a faster one than the phase's own timeout.
"""

ACTIVITIES = 100_000
ROUNDS = 7
OPERATIONS = 20_000
"""How many of each op the raw pump sends through a daemon."""

HOT_PASSES = 5
"""How many hot builds each daemon runs. The best is reported.

The container shares the host's CPU, so one pass carries whatever else the
host was doing. The passes interleave -- nix-daemon, pynixd, nix-daemon --
so a slow minute lands on both daemons and not on one of them. A cold build
stays one pass: it is mostly moving data, and the host's load moves it less
than it moves a few seconds of daemon traffic.
"""

STORM_CLIENTS = 8
"""Parallel `nix` clients in a storm round: a direnv, an editor plugin and a
rebuild landing on the daemon at the same time."""

STORM_ITERS = 3
"""Hot build plus closure query per client per round."""

STORM_ROUNDS = 2
"""Storm rounds per daemon, alternating order to cancel host-load bias."""


async def test(vms: Machines) -> None:
    if vms.phase == "decode":
        await _decode(vms)
    elif vms.phase == "raw":
        await _raw(vms)
    elif vms.phase == "profile":
        await _profile(vms)
    elif vms.phase == "system":
        await _system(vms)
    elif vms.phase == "storm":
        await _storm(vms)
    elif vms.phase == "growth":
        await _growth(vms)
    else:
        raise RuntimeError(f"no benchmark for phase {vms.phase!r}")


async def _decode(vms: Machines) -> None:
    """Decode the activity stream a build makes, without a build.

    The environment installs `nix_daemon_protocol` editable from /work, so
    it imports the edited tree; `stream_decode.py` prints the module it
    imported.
    """
    [vm] = vms.values()
    await vm.succeed("chmod 0777 /artifacts")

    src = vms.settings["src"]
    await vm.succeed(f"rm -rf /work && cp -r {src} /work && chmod -R u+w /work")

    command = f"cd /work && python tests/benchmark/stream_decode.py {ACTIVITIES} {ROUNDS} > /artifacts/decode.log 2>&1"
    rc, _ = await vm.execute(command, timeout=TIMEOUT, label="decode benchmark")
    print(f"[benchmark] decode exit {rc}\n{(await vm.succeed('cat /artifacts/decode.log')).strip()}")
    if rc != 0:
        raise AssertionError(f"decode benchmark exited {rc}")


async def _raw(vms: Machines) -> None:
    """Pump IsValidPath and AddTempRoot through each daemon, and count.

    The same client sends to both, so the daemon is the only thing that
    differs. It runs as root on purpose: the client opens the socket the
    benchmark names, rather than the `auto` store that root would take.
    """
    [vm] = vms.values()
    await vm.succeed("chmod 0777 /artifacts")

    src = vms.settings["src"]
    await vm.succeed(f"rm -rf /work && cp -r {src} /work && chmod -R u+w /work")

    command = (
        "cd /work && "
        f"python tests/benchmark/raw_ops.py {OPERATIONS} "
        f"{vms.settings['upstream']} {vms.settings['socket']} "
        "> /artifacts/raw.log 2>&1"
    )
    rc, _ = await vm.execute(command, timeout=TIMEOUT, label="raw ops")
    print(f"[benchmark] raw exit {rc}\n{(await vm.succeed('cat /artifacts/raw.log')).strip()}")
    if rc != 0:
        raise AssertionError(f"raw ops benchmark exited {rc}")


PROFILE_WRAPPER = """import atexit
import os
import signal
import sys

import pyinstrument

prof = pyinstrument.Profiler(interval=0.0005)
_done = [False]


def dump():
    if _done[0]:
        return
    _done[0] = True
    prof.stop()
    with open("/artifacts/profile.txt", "w") as handle:
        handle.write(prof.output_text(unicode=True, color=False, show_all=True))


atexit.register(dump)


def _on_term(_signum, _frame):
    # `atexit` does not run when a signal kills the interpreter, so turn the
    # SIGTERM of `systemctl stop` into a normal exit.
    sys.exit(0)


signal.signal(signal.SIGTERM, _on_term)

prof.start()
os.environ.setdefault("PYNIXD_CONFIG", "/etc/pynixd/pynixd.json")
sys.argv = ["pynixd", "daemon"]
from pynixd.__main__ import main

main()
"""


async def _profile(vms: Machines) -> None:
    """Profile pynixd while a raw pump drives it, and keep the flame text.

    The numbers of `raw` say how fast an operation is; this says where the
    time went. `pyinstrument` samples the daemon while the pump runs, and the
    wrapper writes a text report when the daemon shuts down on SIGTERM. The
    daemon is started by hand here, so the profiler wraps it: the systemd unit
    starts pynixd before the phase can.

    The phase is not a gate. Read `/artifacts/profile.txt`.
    """
    [vm] = vms.values()
    await vm.succeed("chmod 0777 /artifacts")

    src = vms.settings["src"]
    await vm.succeed(f"rm -rf /work && cp -r {src} /work && chmod -R u+w /work")

    # The profiled daemon takes the socket itself, so the unit must let it go.
    # The `finally` below hands the socket back: `raw` declares `after` on
    # this phase, and a stopped `pynixd.socket` leaves no socket file for it
    # to pump.
    await vm.succeed("systemctl stop pynixd.service pynixd.socket; rm -f " + vms.settings["socket"])

    wrapper = "/work/profile_daemon.py"
    await vm.succeed(f"cat > {wrapper} <<'PYEOF'\n{PROFILE_WRAPPER}\nPYEOF")

    # Start the profiled daemon as its own systemd unit. `--no-block` returns
    # at once; without it `systemd-run` waits for the unit to become active,
    # and a daemon never does. A shell job cannot be used here: it inherits
    # this command's stdout, and the runner reads stdout until the last writer
    # closes. systemd owns the process, and `systemctl stop` sends the SIGTERM
    # that makes the wrapper write its report through the `atexit` handler.
    await vm.succeed(
        "systemd-run --unit=profiled-pynixd --collect --no-block "
        "--working-directory=/work "
        "--setenv=PATH=/run/current-system/sw/bin "
        "--setenv=PYNIXD_CONFIG=/etc/pynixd/pynixd.json "
        f"python {wrapper}"
    )

    try:
        # Wait for the socket, then pump it with fewer operations than `raw`:
        # sampling makes the daemon several times slower. Only the profiled
        # socket: the pump takes which socket to drive, and driving
        # nix-daemon here would double the pump for no profile.
        await vm.succeed(
            f"for _ in $(seq 1 100); do [ -S {vms.settings['socket']} ] && break; sleep 0.2; done",
            timeout=60,
        )
        pump = (
            "cd /work && "
            f"python tests/benchmark/raw_ops.py {OPERATIONS // 4} "
            f"{vms.settings['upstream']} {vms.settings['socket']} second "
            "> /artifacts/profile_pump.log 2>&1"
        )
        rc, _ = await vm.execute(pump, timeout=TIMEOUT, label="profile pump")
        print((await vm.succeed("cat /artifacts/profile_pump.log")).strip())
        if rc != 0:
            raise AssertionError(f"profile pump exited {rc}")

        # Stop the unit, which sends the SIGTERM that makes the wrapper write its
        # report through the `atexit` handler.
        await vm.succeed(
            "systemctl stop profiled-pynixd; "
            "for _ in $(seq 1 50); do [ -f /artifacts/profile.txt ] && break; sleep 0.2; done"
        )
        profile = await vm.succeed("cat /artifacts/profile.txt")
        print("[benchmark] pynixd profile (top):\n" + "\n".join(profile.splitlines()[:60]))
        lines = profile.splitlines()
        for index, line in enumerate(lines):
            if "IsValidPath" in line:
                print("[benchmark] pynixd profile (IsValidPath branch):\n" + "\n".join(lines[index : index + 50]))
                break
        if not profile.strip():
            raise AssertionError("the profiler wrote no report")
    finally:
        await vm.succeed(
            "systemctl start pynixd.socket; "
            f"for _ in $(seq 1 100); do [ -S {vms.settings['socket']} ] && break; sleep 0.2; done"
        )


async def _one_build(
    vm: Machine,
    settings: Mapping[str, str],
    expression: str,
    stamp: str,
    label: str,
    socket: str,
    host_name: str,
    what: str,
    failed: list[str],
) -> float:
    inner = (
        f"NIX_PATH=nixpkgs={settings['nixpkgs']} NIX_REMOTE=unix://{socket} "
        f"nix build --argstr hostName {host_name} --argstr stamp {stamp} "
        f"--file {expression} --no-link > /dev/null 2>&1"
    )
    start = time.monotonic()
    rc, _ = await vm.execute(f"su - tester -c {shlex.quote(inner)}", timeout=BUILD_TIMEOUT, label=label)
    elapsed = time.monotonic() - start
    print(f"[benchmark] {label:10} {what} {elapsed:7.2f}s rc={rc}")
    if rc != 0:
        failed.append(f"{label} {what}")
    return elapsed


async def _print_breakdown(vm: Machine) -> None:
    # What the client actually asked pynixd for, and how long pynixd spent on
    # each operation. A tester who cannot write the store's database reaches
    # the daemon for all of it, so this is the load on the front-end.
    #
    # pynixd logs one JSON record per session at close. `python -c` parses
    # each one, sorts by `total_ops`, and prints every session big enough to
    # be a build or a pump. Small sessions are evals, and there are many.
    breakdown = await vm.succeed(
        "journalctl -u pynixd --no-pager -o cat "
        "| grep client_op_timing "
        r"""| python3 -c 'import sys, json
rows = []
for line in sys.stdin:
    line = line.strip()
    if not line.startswith("{"):
        continue
    rec = json.loads(line)
    if rec.get("total_ops", 0) >= 1000:
        rows.append((
            rec.get("total_ops", 0),
            rec.get("total_time", ""),
            rec.get("total_encode", ""),
            rec.get("breakdown", {}),
            rec.get("encode_breakdown", {}),
        ))
rows.sort(key=lambda row: row[0], reverse=True)
for total_ops, total_time, total_encode, ops, encode in rows:
    print(f"session total_ops={total_ops} dispatch={total_time} encode={total_encode}")
    for name, value in sorted(ops.items(), key=lambda kv: kv[1], reverse=True):
        print(f"    {name:28} {value}")
    top = sorted(encode.items(), key=lambda kv: kv[1], reverse=True)[:5]
    if top:
        print("    encode:")
        for name, value in top:
            print(f"        {name:24} {value}")
'"""
        " || true"
    )
    print(f"[benchmark] pynixd op breakdown:\n{breakdown.strip()}")


async def _system(vms: Machines) -> None:
    """Build a whole closure through each daemon, cold and hot, and time both.

    Evaluating nixpkgs asks the daemon about every derivation, the realisation
    substitutes the closure, and the impure noise of `system.nix` makes it
    build a thousand things on top. This is the whole pipeline: a fault the
    op pump cannot see shows here.

    **The hot run is the number that matters, and the only one compared.**
    A cold build is mostly moving data: it fetches the closure and writes a
    thousand outputs. A hot build of the same goal has every path already, so
    what is left is the traffic the client sends to the daemon and the answer
    it reads back. That is the incremental cost of the front-end, and the only
    part the two daemons can differ in.

    **The cold runs are warmup, and their times are not compared.** Both
    daemons front the same store, and both build the same minimal system from
    the same nixpkgs: all but a handful of hostname-specific paths are
    identical store paths. Whoever builds first fetches the shared closure,
    and whoever builds second reuses it. nix-daemon always runs first here,
    so its cold time carries the fetch and pynixd's does not. Comparing them
    would measure run order, not daemon speed. `hostName` keeps the noise
    goals distinct so the hot builds are each daemon's own, but it cannot
    unshare the system closure that dominates the cold time.

    Each daemon builds its own goal, once cold and then `HOT_PASSES` times
    hot. The **best** hot pass is reported, because the container shares the
    host's CPU.
    """
    [vm] = vms.values()
    expression = f"{vms.settings['src']}/tests/benchmark/system.nix"

    roles = {
        "nix-daemon": (vms.settings["upstream"], "nixdaemon"),
        "pynixd": (vms.settings["socket"], "pynixd"),
    }

    cold_times: dict[str, float] = {}
    hot_times: dict[str, list[float]] = {label: [] for label in roles}
    failed: list[str] = []
    # One stamp for the whole phase. It is fresh to the store, so each daemon's
    # first build really builds, and it is the same for that daemon's later
    # builds, so those really are hot.
    stamp = str(int(time.time()))

    # One cold build per daemon, and only the cold one is timed alone. Then
    # the hot passes, interleaved so host noise lands on both daemons.
    for label, (socket, host_name) in roles.items():
        cold_times[label] = await _one_build(
            vm, vms.settings, expression, stamp, label, socket, host_name, "cold", failed
        )

    for pass_index in range(HOT_PASSES):
        for label, (socket, host_name) in roles.items():
            elapsed = await _one_build(
                vm, vms.settings, expression, stamp, label, socket, host_name, f"hot {pass_index + 1}", failed
            )
            hot_times[label].append(elapsed)

    if failed:
        raise AssertionError(f"system build failed through {', '.join(failed)}")

    for label in roles:
        cold = cold_times[label]
        hot = hot_times[label]
        print(
            f"[benchmark] {label:10} warmup {cold:7.2f}s (not compared: second daemon reuses the shared closure)"
            f"  hot best {min(hot):7.2f}s median {statistics.median(hot):7.2f}s ({HOT_PASSES} passes)"
        )

    # What the client actually asked pynixd for, and how long pynixd spent on
    # each operation. A tester who cannot write the store's database reaches
    # the daemon for all of it, so this is the load on the front-end.
    await _print_breakdown(vm)


async def _storm(vms: Machines) -> None:
    """Storm each daemon with parallel clients, and time the wall.

    Eight `nix` clients at once, each looping a hot build and a closure
    query. One client is polite: it waits for its own answer before asking
    again. Eight at once are a direnv, an editor plugin and a rebuild landing
    together, and the daemon answers on eight sessions at once. The
    per-session synchronous reader keeps one session's queries off the pooled
    connection the others share; a storm is where sharing would show.

    Each daemon storms the same goal, built cold once up front so every
    stormed build is hot. Two rounds, alternating order, best wall reported:
    the container shares the host's CPU, and alternation keeps a slow minute
    from landing on one daemon twice.
    """
    [vm] = vms.values()
    expression = f"{vms.settings['src']}/tests/benchmark/system.nix"

    roles = {
        "nix-daemon": (vms.settings["upstream"], "nixdaemon"),
        "pynixd": (vms.settings["socket"], "pynixd"),
    }

    stamp = str(int(time.time()))
    failed: list[str] = []
    goals: dict[str, str] = {}
    for label, (socket, host_name) in roles.items():
        await _one_build(vm, vms.settings, expression, stamp, label, socket, host_name, "cold", failed)
        goals[label] = await _goal_path(vm, vms.settings, expression, stamp, label, socket, host_name, failed)

    walls: dict[str, list[float]] = {label: [] for label in roles}
    order = list(roles)
    for _ in range(STORM_ROUNDS):
        for label in order:
            socket, host_name = roles[label]
            walls[label].append(
                await _one_storm(vm, vms.settings, expression, stamp, goals[label], label, socket, host_name, failed)
            )
        order.reverse()

    if failed:
        raise AssertionError(f"storm failed: {', '.join(failed)}")

    for label in roles:
        wall = walls[label]
        print(
            f"[benchmark] {label:10} storm best {min(wall):7.2f}s "
            f"({STORM_ROUNDS} rounds, {STORM_CLIENTS} clients x{STORM_ITERS})"
        )

    await _print_breakdown(vm)


async def _growth(vms: Machines) -> None:
    """Repeat one impure build, and fail when any cost grows.

    The build shouts 50,000 log lines and writes a 1 MiB output, so the
    log path and the transfer path both work per iteration. The first
    build is warmup; the probe compares every later build against the
    first measured one and fails on growth. Tracing runs only for this
    phase: the unit restarts with `PYNIXD_BENCH=1` and hands stock
    settings back in the `finally`, so no other phase pays for it.
    """
    [vm] = vms.values()
    await vm.succeed("chmod 0777 /artifacts")

    src = vms.settings["src"]
    await vm.succeed(f"rm -rf /work && cp -r {src} /work && chmod -R u+w /work")

    await vm.succeed("systemctl set-environment PYNIXD_BENCH=1 && systemctl restart pynixd.service")
    try:
        await vm.succeed(
            f"for _ in $(seq 1 100); do [ -S {vms.settings['socket']} ] && break; sleep 0.2; done",
            timeout=60,
        )
        command = (
            "cd /work && "
            f"python tests/benchmark/growth_probe.py {vms.settings['socket']} {vms.settings['nixpkgs']} "
            f"{vms.settings['src']}/tests/benchmark/growth.nix "
            "> /artifacts/growth.log 2>&1"
        )
        rc, _ = await vm.execute(command, timeout=TIMEOUT, label="growth benchmark")
        print(f"[benchmark] growth exit {rc}\n{(await vm.succeed('cat /artifacts/growth.log')).strip()}")
        if rc != 0:
            raise AssertionError(f"growth benchmark exited {rc}")
    finally:
        await vm.succeed(
            "systemctl unset-environment PYNIXD_BENCH; systemctl restart pynixd.service; "
            f"for _ in $(seq 1 100); do [ -S {vms.settings['socket']} ] && break; sleep 0.2; done"
        )


async def _goal_path(
    vm: Machine,
    settings: Mapping[str, str],
    expression: str,
    stamp: str,
    label: str,
    socket: str,
    host_name: str,
    failed: list[str],
) -> str:
    """The store path of the cold-built goal, for the closure queries."""
    inner = (
        f"NIX_PATH=nixpkgs={settings['nixpkgs']} NIX_REMOTE=unix://{socket} "
        f"nix build --argstr hostName {host_name} --argstr stamp {stamp} "
        f"--file {expression} --no-link --print-out-paths 2>/dev/null"
    )
    rc, out = await vm.execute(f"su - tester -c {shlex.quote(inner)}", timeout=BUILD_TIMEOUT, label=f"{label} goal")
    if rc != 0:
        failed.append(f"{label} goal")
        return ""
    return out.strip().split()[-1]


async def _one_storm(
    vm: Machine,
    settings: Mapping[str, str],
    expression: str,
    stamp: str,
    goal: str,
    label: str,
    socket: str,
    host_name: str,
    failed: list[str],
) -> float:
    job = (
        f"for _ in $(seq 1 {STORM_ITERS}); do "
        f"NIX_PATH=nixpkgs={settings['nixpkgs']} NIX_REMOTE=unix://{socket} "
        f"nix build --argstr hostName {host_name} --argstr stamp {stamp} "
        f"--file {expression} --no-link >/dev/null 2>&1 || exit 1; "
        f"NIX_REMOTE=unix://{socket} nix path-info -r {goal} >/dev/null 2>&1 || exit 1; "
        f"done"
    )
    script = (
        "rm -f /tmp/storm-*.rc; "
        f"for i in $(seq 1 {STORM_CLIENTS}); do ( {job}; echo $? > /tmp/storm-$i.rc ) & done; "
        "wait; "
        "grep -Hqv '^0$' /tmp/storm-*.rc && exit 1; exit 0"
    )
    start = time.monotonic()
    rc, _ = await vm.execute(f"su - tester -c {shlex.quote(script)}", timeout=900, label=f"{label} storm")
    elapsed = time.monotonic() - start
    print(f"[benchmark] {label:10} storm {elapsed:7.2f}s rc={rc}")
    if rc != 0:
        failed.append(f"{label} storm")
    return elapsed
