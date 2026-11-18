"""One phase of tests.upgrade: a switch with a user's build in flight. `vms.phase` names it.

docs/design/upgrade-handover.md, "Implementation (B)", numbers the checks.
"""

from __future__ import annotations

from collections.abc import Mapping

from build_helpers import (
    builder_alive,
    client_build,
    client_outcome,
    long_drv,
    main_pid,
    read,
    until,
    valid,
)
from vivarium_runner import Machine, Machines

BUILD_SECONDS = 30


async def alive(vm: Machine, pid: str) -> bool:
    return (await vm.execute(f"test -d /proc/{pid}")).returncode == 0


async def environ(vm: Machine, pid: str) -> str:
    return (await vm.succeed(f"tr '\\0' '\\n' < /proc/{pid}/environ")).stdout


async def serves_a_client(vm: Machine, pid: str) -> bool:
    """Whether *pid* holds an accepted connection on the daemon socket. pynixd logs no info line by default."""
    listen = (await vm.succeed("systemctl show -P Listen pynixd.socket")).stdout.split()[0]
    sockets = (await vm.succeed("ss -xpH")).stdout
    return any(
        f" {listen} " in line and f"pid={pid}," in line and "LISTEN" not in line for line in sockets.splitlines()
    )


async def unit_config(vm: Machine) -> str:
    environment = (await vm.succeed("systemctl show -P Environment pynixd.service")).stdout.split()
    return next(word for word in environment if word.startswith("PYNIXD_CONFIG="))


async def starts(vm: Machine, unit: str, job: str) -> int:
    """How often the build began: each start prints its first tick."""
    return (await read(vm, f"/tmp/{unit}.out")).count(f"tick {job} 0\n")


async def handover(vm: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    drv, out = await long_drv(vm, settings, "handover", BUILD_SECONDS)
    await client_build(vm, "client-handover", drv)
    if await until(lambda: builder_alive(vm, "handover"), 90) is None:
        problems.append("the build never started")
        return
    old = await main_pid(vm, "pynixd.service")
    done = await vm.switch_to("b", check=False)
    print(f"[test] switch to b: exit {done.returncode}\n{done.stdout}\n{done.stderr}")
    if done.returncode != 0:
        problems.append(f"switch exited {done.returncode}")
    new = await main_pid(vm, "pynixd.service")
    print(f"[test] main pid {old} -> {new}")
    if new == old:
        problems.append("the main pid did not move")
    elif await unit_config(vm) not in (await environ(vm, new)).splitlines():
        problems.append("the new main does not run configuration B's settings")
    if not await alive(vm, old):
        problems.append("the old main died at the switch")
    # 3: a new client reaches the new main while the old one drains.
    second, _ = await long_drv(vm, settings, "second", 15)
    await client_build(vm, "client-second", second)
    if await until(lambda: serves_a_client(vm, new), 30) is None:
        problems.append("the new main served no client")
    if not await alive(vm, old):
        problems.append("the old main was gone before the second client")
    # 1: the build completes, started once.
    rc, tail = await client_outcome(vm, "client-handover", BUILD_SECONDS + 120)
    print(f"[test] client: rc {rc}: {tail}")
    if rc != "0":
        problems.append(f"the build failed: rc {rc}: {tail}")
    if not await valid(vm, out):
        problems.append("the output is not valid")
    count = await starts(vm, "client-handover", "handover")
    if count != 1:
        problems.append(f"the build started {count} times")
    # 5: the old process ends once its work does, and the unit stays with the new main.
    if await until(lambda: _gone(vm, old), 60) is None:
        problems.append(f"the old process {old} still runs after its build")
    state = (await vm.succeed("systemctl show -P ActiveState pynixd.service")).stdout.strip()
    if state != "active" or await main_pid(vm, "pynixd.service") != new:
        problems.append(f"after the drain the unit is {state} with main {await main_pid(vm, 'pynixd.service')}")


async def _gone(vm: Machine, pid: str) -> bool:
    return not await alive(vm, pid)


async def broken(vm: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    """7: a successor that cannot start fails the reload, and the old main serves on."""
    old = await main_pid(vm, "pynixd.service")
    done = await vm.switch_to("broken", check=False)
    print(f"[test] switch to broken: exit {done.returncode}\n{done.stdout}\n{done.stderr}")
    if done.returncode == 0:
        problems.append("the switch to a broken successor passed")
    if await main_pid(vm, "pynixd.service") != old:
        problems.append("the main pid moved to a broken successor")
    drv, _ = await long_drv(vm, settings, "after-broken", 1)
    await client_build(vm, "client-after-broken", drv)
    rc, tail = await client_outcome(vm, "client-after-broken", 60)
    if rc != "0":
        problems.append(f"the old main did not serve after the failed reload: rc {rc}: {tail}")


async def control(vm: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    """6: without the hand-over, the same switch kills the build."""
    drv, _ = await long_drv(vm, settings, "control", BUILD_SECONDS)
    await client_build(vm, "client-control", drv)
    if await until(lambda: builder_alive(vm, "control"), 90) is None:
        problems.append("the build never started")
        return
    done = await vm.switch_to("control", check=False)
    print(f"[test] switch to control: exit {done.returncode}")
    rc, tail = await client_outcome(vm, "client-control", BUILD_SECONDS + 60)
    print(f"[test] client: rc {rc}: {tail}")
    if rc == "0" or "server is shutting down" not in tail:
        problems.append(f"the control build did not fail as a restart fails it: rc {rc}: {tail}")


PHASES = {"handover": handover, "broken": broken, "control": control}


async def test(vms: Machines) -> None:
    vm = vms.machine
    problems: list[str] = []
    await PHASES[vms.phase or ""](vm, vms.settings, problems)
    if problems:
        raise AssertionError("\n".join(problems))
