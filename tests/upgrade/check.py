"""One phase of tests.upgrade: a switch with a user's build in flight. `vms.phase` names it.

docs/design/upgrade-handover.md, "Implementation (B)", numbers the checks.
"""

from __future__ import annotations

from collections.abc import Mapping

from build_helpers import (
    background,
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


async def handover(vm: Machine, builder: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    drv, out = await long_drv(vm, settings, "handover", BUILD_SECONDS)
    await client_build(vm, "client-handover", drv)
    if await until(lambda: running(vm, builder, "handover"), 90) is None:
        problems.append("the build never started")
        return
    # One connection reused for 40 requests, 2 a second: a keep-alive scraper across the switch.
    urls = " ".join(["http://127.0.0.1:8080/nix-cache-info"] * 40)
    await background(vm, "scraper", f"/run/current-system/sw/bin/curl --fail --silent --rate 2/s {urls}")
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
    rc, tail = await client_outcome(vm, "scraper", 60)
    if rc != "0":
        problems.append(f"the keep-alive HTTP client failed across the switch: rc {rc}: {tail}")
    # 5: the old process ends once its work does, and the unit stays with the new main.
    if await until(lambda: _gone(vm, old), 60) is None:
        problems.append(f"the old process {old} still runs after its build")
    state = (await vm.succeed("systemctl show -P ActiveState pynixd.service")).stdout.strip()
    if state != "active" or await main_pid(vm, "pynixd.service") != new:
        problems.append(f"after the drain the unit is {state} with main {await main_pid(vm, 'pynixd.service')}")


async def running(vm: Machine, builder: Machine, job: str) -> bool:
    """Whether the build runs on this guest or on the reverse builder: the scheduler picks either."""
    return await builder_alive(vm, job) or await builder_alive(builder, job)


async def _gone(vm: Machine, pid: str) -> bool:
    return not await alive(vm, pid)


async def remote(vm: Machine, builder: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    """2: a build on a reverse builder completes across the switch, and the builder then serves the new main."""
    features = [settings["remoteFeature"]]
    drv, out = await long_drv(vm, settings, "remote", BUILD_SECONDS, features)
    await client_build(vm, "client-remote", drv)
    if await until(lambda: builder_alive(builder, "remote"), 120) is None:
        problems.append("the remote build never started on the builder")
        return
    old = await main_pid(vm, "pynixd.service")
    done = await vm.switch_to("c", check=False)
    print(f"[test] switch to c: exit {done.returncode}")
    if done.returncode != 0:
        problems.append(f"switch exited {done.returncode}")
    rc, tail = await client_outcome(vm, "client-remote", BUILD_SECONDS + 120)
    print(f"[test] remote client: rc {rc}: {tail}")
    if rc != "0" or not await valid(vm, out):
        problems.append(f"the remote build failed: rc {rc}: {tail}")
    count = await starts(vm, "client-remote", "remote")
    if count != 1:
        problems.append(f"the remote build started {count} times")
    if await until(lambda: _gone(vm, old), 60) is None:
        problems.append(f"the old process {old} still runs after its remote build")
    # The builder left the old process and registered with the new one.
    after, after_out = await long_drv(vm, settings, "remote-after", 2, features)
    await client_build(vm, "client-remote-after", after)
    rc, tail = await client_outcome(vm, "client-remote-after", 120)
    if rc != "0" or not await valid(vm, after_out):
        problems.append(f"the new main got no build onto the builder: rc {rc}: {tail}")


async def broken(vm: Machine, _builder: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
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


async def control(vm: Machine, builder: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    """6: without the hand-over, the same switch kills the build."""
    drv, _ = await long_drv(vm, settings, "control", BUILD_SECONDS)
    await client_build(vm, "client-control", drv)
    if await until(lambda: running(vm, builder, "control"), 90) is None:
        problems.append("the build never started")
        return
    done = await vm.switch_to("control", check=False)
    print(f"[test] switch to control: exit {done.returncode}")
    rc, tail = await client_outcome(vm, "client-control", BUILD_SECONDS + 60)
    print(f"[test] client: rc {rc}: {tail}")
    if rc == "0" or "server is shutting down" not in tail:
        problems.append(f"the control build did not fail as a restart fails it: rc {rc}: {tail}")


async def idle(vm: Machine, _builder: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    """4: a client connected before the switch, idle across it, still gets answers. A stock client never reconnects."""
    await vm.succeed("rm -f /tmp/repl.in; touch /tmp/repl.in; chmod 666 /tmp/repl.in")
    await background(
        vm,
        "repl",
        "/run/current-system/sw/bin/tail -f /tmp/repl.in | /run/current-system/sw/bin/nix repl --store daemon",
        user="tester",
    )
    await vm.succeed(f"echo 'builtins.storePath \"{settings['busybox']}\"' >> /tmp/repl.in")
    if await until(lambda: _says(vm, settings["busybox"]), 60) is None:
        problems.append(f"the repl did not answer before the switch: {await read(vm, '/tmp/repl.out')}")
        return
    old = await main_pid(vm, "pynixd.service")
    done = await vm.switch_to("d", check=False)
    if done.returncode != 0:
        problems.append(f"switch exited {done.returncode}")
    system = (await vm.succeed("readlink -f /run/current-system")).stdout.strip()
    await vm.succeed(f"echo 'builtins.storePath \"{system}\"' >> /tmp/repl.in")
    if await until(lambda: _says(vm, system), 60) is None:
        problems.append(f"the idle client got no answer after the switch: {await read(vm, '/tmp/repl.out')}")
    if not await alive(vm, old):
        problems.append("the old process left while its client was connected")
    # `kill`, not `stop`: the unit has KillMode=process, and its pipeline outlives a stop.
    await vm.succeed("systemctl kill repl.service")
    if await until(lambda: _gone(vm, old), 60) is None:
        problems.append(f"the old process {old} still runs after its last client left")


async def _says(vm: Machine, text: str) -> bool:
    return text in await read(vm, "/tmp/repl.out")


async def again(vm: Machine, builder: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    """8: a second switch while the first old process drains."""
    drv, out = await long_drv(vm, settings, "again", BUILD_SECONDS)
    await client_build(vm, "client-again", drv)
    if await until(lambda: running(vm, builder, "again"), 90) is None:
        problems.append("the build never started")
        return
    first = await main_pid(vm, "pynixd.service")
    for name in ("e", "f"):
        done = await vm.switch_to(name, check=False)
        print(f"[test] switch to {name}: exit {done.returncode}")
        if done.returncode != 0:
            problems.append(f"switch to {name} exited {done.returncode}")
    second = await main_pid(vm, "pynixd.service")
    if second == first:
        problems.append("the main pid did not move")
    rc, tail = await client_outcome(vm, "client-again", BUILD_SECONDS + 120)
    if rc != "0" or not await valid(vm, out):
        problems.append(f"the build failed across two switches: rc {rc}: {tail}")
    if await until(lambda: _gone(vm, first), 60) is None:
        problems.append(f"the first old process {first} still runs")
    if await main_pid(vm, "pynixd.service") != second:
        problems.append("the main pid moved after the second switch")


async def refused(vm: Machine, builder: Machine, settings: Mapping[str, str], problems: list[str]) -> None:
    """A pynixd that cannot overlap restarts, so the new configuration applies."""
    old = await main_pid(builder, "pynixd.service")
    done = await builder.switch_to("changed", check=False)
    print(f"[test] builder switch: exit {done.returncode}")
    if done.returncode != 0:
        problems.append(f"the builder's switch exited {done.returncode}")
    if await until(lambda: _moved(builder, old), 60) is None:
        problems.append("the refused hand-over did not restart the builder's pynixd")
    journal = (await builder.succeed("journalctl --no-pager -o cat -u pynixd.service")).stdout
    if "refuses: a reverse initiator" not in journal:
        problems.append("the builder's journal names no refusal")
    # The builder registers again; a remote build runs.
    drv, out = await long_drv(vm, settings, "after-refused", 2, [settings["remoteFeature"]])
    await client_build(vm, "client-after-refused", drv)
    rc, tail = await client_outcome(vm, "client-after-refused", 120)
    if rc != "0" or not await valid(vm, out):
        problems.append(f"no remote build after the builder restarted: rc {rc}: {tail}")


async def rollout(vm: Machine, _builder: Machine, _settings: Mapping[str, str], problems: list[str]) -> None:
    """The first switch from a pynixd with no control socket restarts it, as before the hand-over."""
    for name in ("legacy", "g"):
        old = await main_pid(vm, "pynixd.service")
        done = await vm.switch_to(name, check=False)
        print(f"[test] switch to {name}: exit {done.returncode}")
        if done.returncode != 0:
            problems.append(f"switch to {name} exited {done.returncode}")
        if await until(lambda old=old: _moved(vm, old), 60) is None:
            problems.append(f"the main pid did not move on the switch to {name}")
    state = (await vm.succeed("systemctl show -P ActiveState pynixd.service")).stdout.strip()
    if state != "active":
        problems.append(f"after the rollout the unit is {state}")
    journal = (await vm.succeed("journalctl --no-pager -o cat -u pynixd.service")).stdout
    if "no pynixd answers on" not in journal:
        problems.append("the switch from the legacy pynixd did not fall back to a restart")


async def _moved(vm: Machine, pid: str) -> bool:
    current = await main_pid(vm, "pynixd.service")
    return current not in ("0", pid)


PHASES = {
    "handover": handover,
    "remote": remote,
    "idle": idle,
    "again": again,
    "refused": refused,
    "rollout": rollout,
    "broken": broken,
    "control": control,
}


async def test(vms: Machines) -> None:
    problems: list[str] = []
    await PHASES[vms.phase or ""](vms.machine, vms.builder, vms.settings, problems)
    if problems:
        raise AssertionError("\n".join(problems))
