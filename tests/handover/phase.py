"""One phase of tests.handover: what an upgrade of pynixd does to work in flight.

`vms.phase` names the measurement. Each prints `[measure]` lines, the facts
that docs/design/upgrade-handover.md cites, and fails only when the
harness itself breaks: a measured outcome is a result, not a failure.
"""

from __future__ import annotations

import shlex
import time
from collections.abc import Awaitable, Callable
from typing import NamedTuple

import anyio
from daemon_helpers import as_tester, store_path
from vivarium_runner import Machine, Machines

BUILD_SECONDS = 40
TOY = "/run/current-system/sw/bin/toy"


def measure(*parts: object) -> None:
    print("[measure] " + " ".join(str(part) for part in parts), flush=True)


async def long_drv(vm: Machine, settings: dict, job: str, seconds: int = BUILD_SECONDS) -> tuple[str, str]:
    """A derivation that ticks once a second for `seconds`: its `.drv` and its output."""
    script = (
        f"i=0; while [ $i -lt {seconds} ]; do echo tick {job} $i;"
        f" ${{bb}}/bin/sleep 1; i=$((i+1)); done; echo {job} > $out"
    )
    expression = (
        f'let bb = builtins.storePath "{settings["busybox"]}"; in derivation {{'
        f' name = "handover-{job}";'
        f' system = "{settings["system"]}";'
        ' builder = "${bb}/bin/sh";'
        f' args = [ "-c" {nix_string(script)} ];'
        " }"
    )
    drv = store_path(await as_tester(vm, f"nix-instantiate --expr {shlex.quote(expression)}"))
    out = (await vm.succeed(f"nix-store -q --outputs {drv}")).strip()
    return drv, out


def nix_string(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


async def builder_alive(vm: Machine, job: str) -> bool:
    """Whether the build's shell still runs. Its arguments carry the job's name."""
    # `-ww`: off a terminal, ps cuts `args` at 80 columns, before the job's name.
    processes = await vm.succeed("ps -ww -eo pid,args")
    return any(line.split()[1:2] == ["sh"] and f"tick {job} " in line for line in processes.splitlines())


async def until(check: Callable[[], Awaitable[bool]], timeout: float, step: float = 0.5) -> float | None:
    """Seconds until `check` holds, or None when it never did."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if await check():
            return time.monotonic() - start
        await anyio.sleep(step)
    return None


async def read(vm: Machine, path: str) -> str:
    return (await vm.succeed(f"cat {path} 2>/dev/null || true")).strip()


async def background(vm: Machine, unit: str, command: str, *, user: str | None = None) -> None:
    """Start `command` in a transient unit; its exit status lands in /tmp/<unit>.rc.

    `KillMode=process`: a child the command forks outlives it. Under the
    default, the unit's end killed the child that held the connection.
    """
    wrapped = f"{command}; echo $? > /tmp/{unit}.rc"
    uid = f" --uid={user}" if user else ""
    await vm.succeed(
        f"rm -f /tmp/{unit}.rc /tmp/{unit}.out; systemd-run --quiet --unit={unit}{uid} -p KillMode=process"
        f" -p StandardOutput=file:/tmp/{unit}.out -p StandardError=append:/tmp/{unit}.out"
        f" /bin/sh -c {shlex.quote(wrapped)}"
    )


async def client_build(vm: Machine, unit: str, drv: str) -> None:
    """A user's `nix-store --realise` through the default socket, which pynixd holds."""
    await background(vm, unit, f"/run/current-system/sw/bin/nix-store --realise {drv}", user="tester")


async def client_outcome(vm: Machine, unit: str, timeout: float) -> tuple[str, str]:
    """The client's exit status and the last lines of its output."""
    await until(lambda: _exists(vm, f"/tmp/{unit}.rc"), timeout)
    rc = await read(vm, f"/tmp/{unit}.rc") or "still running"
    tail = await vm.succeed(f"tail -n 4 /tmp/{unit}.out 2>/dev/null || true")
    return rc, " | ".join(line.strip() for line in tail.splitlines() if line.strip())


async def _exists(vm: Machine, path: str) -> bool:
    rc, _ = await vm.execute(f"test -e {path}")
    return rc == 0


async def valid(vm: Machine, path: str) -> bool:
    rc, _ = await vm.execute(f"nix-store --check-validity {path}")
    return rc == 0


async def main_pid(vm: Machine, unit: str) -> str:
    return (await vm.succeed(f"systemctl show -P MainPID {unit}")).strip()


async def restart(vm: Machine, settings: dict) -> None:
    """M0: `systemctl restart pynixd` with a user's build in flight."""
    drv, out = await long_drv(vm, settings, "restart")
    await vm.succeed("touch /run/pynixd/marker")
    await client_build(vm, "client-restart", drv)
    started = await until(lambda: builder_alive(vm, "restart"), 90)
    measure("restart: builder running after", started)
    before = await main_pid(vm, "pynixd.service")
    t0 = time.monotonic()
    await vm.succeed("systemctl restart pynixd.service")
    measure(
        "restart: systemctl restart took",
        round(time.monotonic() - t0, 2),
        "s; pid",
        before,
        "->",
        await main_pid(vm, "pynixd.service"),
    )
    rc, tail = await client_outcome(vm, "client-restart", 15)
    measure("restart: client rc", rc, "output:", tail)
    await anyio.sleep(3)
    measure("restart: builder alive 3 s after the restart:", await builder_alive(vm, "restart"))
    measure("restart: /run/pynixd/marker survives:", await _exists(vm, "/run/pynixd/marker"))
    await anyio.sleep(BUILD_SECONDS)
    measure("restart: output valid after the build's whole length:", await valid(vm, out))
    journal = await vm.succeed(
        "journalctl --no-pager -u nix-daemon-upstream.service -g 'interrupt|restart' -n 5 || true"
    )
    measure("restart: upstream journal:", " | ".join(journal.splitlines()[-5:]))


async def hangup(vm: Machine, settings: dict) -> None:
    """M3: nix-daemon and a client that closes, or keeps, its connection mid-build."""
    drv, out = await long_drv(vm, settings, "close")
    await background(vm, "toy-close", f"{TOY} direct --drv {drv} --then close --out /var/lib/toy/close.result")
    await until(lambda: _exists(vm, "/tmp/toy-close.rc"), 30)
    await anyio.sleep(3)
    measure("hangup: client closed mid-build; builder alive 3 s later:", await builder_alive(vm, "close"))
    measure("hangup: closed client's output valid:", await valid(vm, out))

    drv, out = await long_drv(vm, settings, "hold")
    hold = 15
    await background(
        vm, "toy-hold", f"{TOY} direct --drv {drv} --then fork-hold --hold {hold} --out /var/lib/toy/hold.result"
    )
    await until(lambda: _exists(vm, "/tmp/toy-hold.rc"), 30)
    measure("hangup: process that opened the connection exited, rc", await read(vm, "/tmp/toy-hold.rc"))
    await anyio.sleep(5)
    measure("hangup: a forked child holds the fd unread; builder alive 5 s later:", await builder_alive(vm, "hold"))
    took = await until(lambda: _exists(vm, "/var/lib/toy/hold.result"), BUILD_SECONDS + 30)
    measure("hangup: the child read the result after", took, ":", await read(vm, "/var/lib/toy/hold.result"))
    measure("hangup: held client's output valid:", await valid(vm, out))
    await chatty(vm, settings)


async def chatty(vm: Machine, settings: dict) -> None:
    """A build that logs fast while nobody reads its connection: stalled or killed?"""
    script = (
        "i=0; while [ $i -lt 200000 ]; do echo chatty-line-padding-padding-padding-padding $i;"
        " i=$((i+1)); done; echo done > $out"
    )
    expression = (
        f'let bb = builtins.storePath "{settings["busybox"]}"; in derivation {{'
        ' name = "handover-chatty";'
        f' system = "{settings["system"]}";'
        ' builder = "${bb}/bin/sh";'
        f' args = [ "-c" {nix_string(script)} ];'
        " }"
    )
    drv = store_path(await as_tester(vm, f"nix-instantiate --expr {shlex.quote(expression)}"))
    out = (await vm.succeed(f"nix-store -q --outputs {drv}")).strip()
    hold = 20
    await background(
        vm, "toy-chatty", f"{TOY} direct --drv {drv} --then fork-hold --hold {hold} --out /var/lib/toy/chatty.result"
    )
    await anyio.sleep(hold / 2)
    measure("chatty: output valid while its log is unread:", await valid(vm, out))
    took = await until(lambda: _exists(vm, "/var/lib/toy/chatty.result"), 120)
    measure("chatty: result after", took, ":", (await read(vm, "/var/lib/toy/chatty.result"))[:80])
    measure("chatty: output valid at the end:", await valid(vm, out))


class Parked(NamedTuple):
    unit: str
    job: str
    out: str


async def park(vm: Machine, settings: dict, unit: str, job: str) -> Parked:
    """Start `unit`, a toy that parks a building connection in its fd store."""
    drv, out = await long_drv(vm, settings, job)
    # A unit left running by an earlier phase would make `start` a no-op.
    await vm.succeed(f"systemctl stop {unit}.service")
    await vm.succeed(f"echo {drv} > /run/toy/{unit}.drv && rm -f /var/lib/toy/{unit}.result")
    await vm.succeed(f"systemctl start {unit}.service")
    await until(lambda: builder_alive(vm, job), 30)
    alive = await builder_alive(vm, job)
    measure(f"{unit}: parked, builder alive: {alive};", await read(vm, f"/var/lib/toy/{unit}.result"))
    return Parked(unit, job, out)


async def parked_outcome(vm: Machine, parked: Parked, label: str) -> None:
    await anyio.sleep(3)
    measure(f"{label} {parked.unit}: builder alive 3 s later:", await builder_alive(vm, parked.job))
    await until(lambda: valid(vm, parked.out), BUILD_SECONDS + 10)
    await anyio.sleep(2)
    result = await read(vm, f"/var/lib/toy/{parked.unit}.result")
    measure(f"{label} {parked.unit}: result: {result}; output valid:", await valid(vm, parked.out))


async def fdstore(vm: Machine, settings: dict) -> None:
    """M4: the fd store across `systemctl restart`, and across stop then start."""
    parked = await park(vm, settings, "toy-fdstore", "fdstore-restart")
    await vm.succeed("systemctl restart toy-fdstore.service")
    await parked_outcome(vm, parked, "fdstore restart:")

    parked = await park(vm, settings, "toy-fdstore-nostop", "fdstore-stop")
    await vm.succeed("systemctl stop toy-fdstore-nostop.service")
    await anyio.sleep(3)
    alive = await builder_alive(vm, parked.job)
    measure("fdstore stop: default Preserve; builder alive 3 s after stop:", alive)

    parked = await park(vm, settings, "toy-fdstore-preserve", "fdstore-preserve")
    await vm.succeed("systemctl stop toy-fdstore-preserve.service")
    await anyio.sleep(3)
    alive = await builder_alive(vm, parked.job)
    measure("fdstore stop: Preserve=yes; builder alive 3 s after stop:", alive)
    await vm.succeed("systemctl start toy-fdstore-preserve.service")
    await parked_outcome(vm, parked, "fdstore stop+start Preserve=yes:")


async def probe_start(vm: Machine, busybox: str) -> None:
    """A user's daemon query five times a second, each line `time ok|fail`."""
    loop = (
        "while :; do t=$(date +%s.%N);"
        f" if nix-store --store daemon -q --hash {busybox} >/dev/null 2>/tmp/probe-err;"
        ' then echo "$t ok"; else echo "$t fail $(tail -n 1 /tmp/probe-err)"; fi; sleep 0.2; done'
    )
    await vm.succeed("rm -f /tmp/probe.log")
    await vm.succeed(
        "systemd-run --quiet --unit=probe --uid=tester -p StandardOutput=file:/tmp/probe.log"
        f" -E PATH=/run/current-system/sw/bin /bin/sh -c {shlex.quote(loop)}"
    )


async def probe_stop(vm: Machine, label: str, since: float) -> None:
    await vm.succeed("systemctl stop probe.service")
    lines = (await read(vm, "/tmp/probe.log")).splitlines()
    fails = [line for line in lines if " fail" in line and float(line.split()[0]) >= since]
    oks = [line for line in lines if line.endswith(" ok") and float(line.split()[0]) >= since]
    measure(f"{label}: probe queries during the switch: ok {len(oks)}, failed {len(fails)}")
    if fails:
        span = float(fails[-1].split()[0]) - float(fails[0].split()[0])
        measure(f"{label}: failures span {span:.2f} s; first: {fails[0]}")


async def pid_alive(vm: Machine, pid: str) -> bool:
    rc, _ = await vm.execute(f"kill -0 {pid}")
    return rc == 0


async def overlap_start(vm: Machine, settings: dict, unit: str, job: str) -> str:
    """Start the overlap toy and a client whose request is in flight in it. Answers the main pid."""
    drv, _ = await long_drv(vm, settings, job)
    await vm.succeed(f"systemctl stop {unit}.service; systemctl start {unit}.service")
    await vm.succeed(f"rm -f /var/lib/toy/client-{unit}.result")
    sock = f"/run/toy/{unit}.sock"
    result = f"/var/lib/toy/client-{unit}.result"
    await background(vm, f"client-{unit}", f"{TOY} overlap-client --sock {sock} --drv {drv} --out {result}")
    await until(lambda: builder_alive(vm, job), 30)
    pid = await main_pid(vm, f"{unit}.service")
    measure(f"{unit}: client in flight: {await builder_alive(vm, job)}; main pid {pid}")
    return pid


async def overlap_outcome(vm: Machine, settings: dict, unit: str, before: str, label: str) -> None:
    after = await main_pid(vm, f"{unit}.service")
    state = await vm.unit_state(f"{unit}.service")
    measure(f"{label} {unit}: main pid {before} -> {after}; old alive: {await pid_alive(vm, before)}; unit {state}")
    result = f"/var/lib/toy/client-{unit}.result"
    took = await until(lambda: _exists(vm, result), BUILD_SECONDS + 20)
    measure(f"{label} {unit}: in-flight client answered after {took}: {await read(vm, result)}")
    await anyio.sleep(1)
    state = await vm.unit_state(f"{unit}.service")
    after = await main_pid(vm, f"{unit}.service")
    measure(f"{label} {unit}: old alive after its client: {await pid_alive(vm, before)}; unit {state}; main {after}")
    drv, _ = await long_drv(vm, settings, f"{unit}-{label}-after", 2)
    sock = f"/run/toy/{unit}.sock"
    await vm.succeed(f"{TOY} overlap-client --sock {sock} --drv {drv} --out /var/lib/toy/after-{unit}.result")
    measure(f"{label} {unit}: a new client is answered by: {await read(vm, f'/var/lib/toy/after-{unit}.result')}")


async def switch(vm: Machine, settings: dict, to: str, label: str) -> None:
    """M2: a switch-to-configuration with every kind of work in flight."""
    drv, out = await long_drv(vm, settings, f"client-{label}")
    await client_build(vm, f"client-{label}", drv)
    started = await until(lambda: builder_alive(vm, f"client-{label}"), 30)
    measure(f"{label}: pynixd client's builder running after {started}")
    parked = [
        await park(vm, settings, unit, f"{unit}-{label}")
        for unit in ("toy-fdstore", "toy-fdstore-nostop", "toy-fdstore-preserve")
    ]
    overlap = {
        unit: await overlap_start(vm, settings, unit, f"{unit}-{label}") for unit in ("toy-overlap", "toy-overlap-old")
    }
    await probe_start(vm, settings["busybox"])
    await anyio.sleep(2)
    since = time.time()
    t0 = time.monotonic()
    rc, output = await vm.switch_to(to, check=False)
    measure(f"{label}: switch to {to}: exit {rc} in {time.monotonic() - t0:.1f} s")
    for line in output.splitlines():
        if line.startswith(("stopping", "starting", "restarting", "reloading", "NOT restarting")):
            measure(f"{label}: s-t-c said: {line}")
    await anyio.sleep(1)
    await probe_stop(vm, label, since)
    rc, tail = await client_outcome(vm, f"client-{label}", 10)
    measure(f"{label}: pynixd client rc {rc}: {tail}")
    measure(f"{label}: pynixd client's builder alive:", await builder_alive(vm, f"client-{label}"))
    for each in parked:
        await parked_outcome(vm, each, f"{label}:")
    for unit, pid in overlap.items():
        await overlap_outcome(vm, settings, unit, pid, label)
    for unit in (*(each.unit for each in parked), *overlap):
        await vm.execute(f"systemctl stop {unit}.service")
    measure(f"{label}: pynixd client's output valid:", await valid(vm, out))


async def overlap_restart(vm: Machine, settings: dict) -> None:
    """The next restart while an old main process still drains."""
    unit = "toy-overlap"
    old = await overlap_start(vm, settings, unit, "overlap-restart")
    await vm.succeed(f"systemctl reload {unit}.service")
    new = await main_pid(vm, f"{unit}.service")
    measure(f"overlap-restart: reloaded, main {old} -> {new}; old alive: {await pid_alive(vm, old)}")
    await vm.succeed(f"systemctl restart {unit}.service")
    await anyio.sleep(1)
    measure(f"overlap-restart: after a restart, the draining old pid alive: {await pid_alive(vm, old)}")
    result = f"/var/lib/toy/client-{unit}.result"
    took = await until(lambda: _exists(vm, result), 10)
    rc = await read(vm, f"/tmp/client-{unit}.rc")
    measure(f"overlap-restart: its client after {took}: {await read(vm, result)} (rc {rc})")
    await vm.execute(f"systemctl stop {unit}.service")


async def overlap_crash(vm: Machine, settings: dict) -> None:
    """A main handed over by MAINPID= dies uncommanded: does systemd notice?

    systemd logs "Supervising process ... which is not our child. We'll most
    likely not notice when it exits." for such a main.
    """
    unit = "toy-overlap"
    old = await overlap_start(vm, settings, unit, "overlap-crash")
    await vm.succeed(f"systemctl reload {unit}.service")
    new = await main_pid(vm, f"{unit}.service")
    restarts = (await vm.succeed(f"systemctl show -P NRestarts {unit}.service")).strip()
    measure(f"overlap-crash: reloaded, main {old} -> {new}; NRestarts {restarts}")
    await vm.succeed(f"kill -9 {new}")
    for wait in (1, 3, 6):
        await anyio.sleep(wait)
        state = await vm.unit_state(f"{unit}.service")
        pid = await main_pid(vm, f"{unit}.service")
        restarts = (await vm.succeed(f"systemctl show -P NRestarts {unit}.service")).strip()
        measure(f"overlap-crash: +{wait}s after kill -9 of {new}: unit {state}, main {pid}, NRestarts {restarts}")
    journal = await vm.succeed(f"journalctl --no-pager -u {unit} -n 8 -o cat | grep -v 'toy '")
    measure("overlap-crash: journal: " + " | ".join(journal.splitlines()))
    await vm.execute(f"systemctl stop {unit}.service")


async def test(vms: Machines) -> None:
    vm = vms.machine
    settings = vms.settings
    name = vms.phase or ""
    if name == "boot":
        await vm.wait_for_unit("multi-user.target")
        return
    await vm.succeed("mkdir -p /run/toy /var/lib/toy")
    if name == "restart":
        await restart(vm, settings)
    elif name == "hangup":
        await hangup(vm, settings)
    elif name == "fdstore":
        await fdstore(vm, settings)
    elif name == "switch-stop":
        await switch(vm, settings, "b", "switch-stop")
    elif name == "switch-nostop":
        await switch(vm, settings, "a-nostop", "switch-nostop")
    elif name == "overlap-restart":
        await overlap_restart(vm, settings)
    elif name == "overlap-crash":
        await overlap_crash(vm, settings)
    else:
        raise AssertionError(f"unknown phase {name}")
