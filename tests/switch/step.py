"""One step of tests.switch: switch the machine, then check it is in its mode.

`vms.phase` names the step, and the settings say where it goes and what the
machine is after it. A switch that exits 0 proves nothing about sockets:
switch-to-configuration does not restart a changed `.socket` unit
(switch-to-configuration-ng `main.rs`, "FIXME: do something?"), so each
check asks the machine who holds what.
"""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict

from daemon_helpers import as_tester, instantiate, store_path
from vivarium_runner import Machine, Machines

if TYPE_CHECKING:
    from collections.abc import Mapping

DEFAULT = "/nix/var/nix/daemon-socket/socket"
UPSTREAM = "/nix/var/nix/daemon-socket/upstream"
BESIDE = "/run/pynixd/pynixd.sock"


class Step(TypedDict):
    """One entry of the spec's `steps`: the switch target, or `False` for the booted system."""

    name: str
    mode: str
    to: str | Literal[False]
    nix: NotRequired[str]


async def holder(vm: Machine, socket: str) -> str:
    return (await vm.succeed(f"ss -xlpnH src {socket}")).strip()


def is_pynixd(line: str) -> bool:
    return "pynixd" in line or "python" in line


async def build(vm: Machine, settings: Mapping[str, str], job: str, store: str | None = None) -> str:
    drv = await instantiate(vm, settings, job)
    where = f" --store {shlex.quote(store)}" if store else ""
    return store_path(await as_tester(vm, f"nix-store{where} --realise {drv}"))


async def test(vms: Machines) -> None:
    vm = vms.machine
    settings = vms.settings
    name = vms.phase or ""
    steps: list[Step] = settings["steps"]
    step = next(each for each in steps if each["name"] == name)
    mode = step["mode"]
    problems: list[str] = []

    if step["to"] is False:
        await vm.wait_for_unit("multi-user.target")
    else:
        rc, out = await vm.switch_to(step["to"], check=False)
        print(f"[test] switch to {step['to'] or 'the booted system'}: exit {rc}")
        if rc != 0:
            problems.append(f"switch-to-configuration exited {rc}:\n{out}")

    failed = (await vm.succeed("systemctl --failed --no-legend --plain")).strip()
    if failed:
        problems.append(f"failed units: {failed}")

    default = await holder(vm, DEFAULT)
    upstream = await holder(vm, UPSTREAM)
    print(f"[test] {DEFAULT}: {default or 'nobody'}")
    print(f"[test] {UPSTREAM}: {upstream or 'nobody'}")
    if mode == "replace":
        if not is_pynixd(default):
            problems.append(f"pynixd does not hold {DEFAULT}: {default!r}")
        if "systemd" not in upstream:
            problems.append(f"nix-daemon.socket is not on {UPSTREAM}: {upstream!r}")
    else:
        if "systemd" not in default:
            problems.append(f"nix-daemon.socket does not hold {DEFAULT}: {default!r}")
        if upstream:
            problems.append(f"something still listens on {UPSTREAM}: {upstream!r}")

    if nix := step.get("nix"):
        # The daemon that answered the build above, not the one configured.
        pid = (await vm.succeed("systemctl show -P MainPID nix-daemon-upstream.service")).strip()
        running = await vm.succeed(f"$(readlink /proc/{pid}/exe) --version")
        if nix not in running:
            problems.append(f"nix-daemon-upstream runs {running.strip()!r}, not {nix}")
        else:
            print(f"[test] nix-daemon-upstream runs {running.strip()}")

    pynixd = await vm.unit_state("pynixd.service")
    want = "inactive" if mode == "stock" else "active"
    if pynixd != want:
        problems.append(f"pynixd.service is {pynixd}, not {want}")

    if mode == "beside":
        beside = await holder(vm, BESIDE)
        if not is_pynixd(beside):
            problems.append(f"pynixd does not hold {BESIDE}: {beside!r}")

    # A user's build through the default socket, whoever holds it. Root
    # would open the store directly and reach no daemon at all.
    try:
        print(f"[test] tester built {await build(vm, settings, name)}")
        if mode == "beside":
            out = await build(vm, settings, f"{name}-beside", f"unix://{BESIDE}")
            print(f"[test] tester built {out} through {BESIDE}")
    except Exception as error:
        problems.append(f"a user's build failed: {error}")

    # The machine's own daemon is described by its nix.conf: probe builds
    # before READY=1 wait for build slots on a busy machine (#58).
    # Each probe leaves its output, `<hash>-probe-system-<system>` and
    # `-probe-feature-<feature>`. pynixd's journal holds warnings only.
    probes = (await vm.succeed("ls /nix/store | grep -- '-probe-' || true")).split()
    if probes:
        problems.append(f"pynixd probed the machine's own daemon with {len(probes)} builds: {probes[:3]}")

    # A client that comes while pynixd is down waits for it rather than
    # failing: the socket stays bound and starts pynixd (#59).
    if mode == "replace" and not problems:
        await vm.succeed("systemctl stop pynixd.service")
        try:
            out = await build(vm, settings, f"{name}-early")
            print(f"[test] with pynixd.service stopped, tester built {out}")
        except Exception as error:
            problems.append(f"a build while pynixd was stopped failed: {error}")
        state = await vm.unit_state("pynixd.service")
        if state != "active":
            problems.append(f"the socket did not start pynixd.service: {state}")

    if problems:
        journal = await vm.succeed(
            "journalctl --no-pager -n 60 -u pynixd.service -u pynixd.socket -u nix-daemon.socket"
            " -u nix-daemon.service -u nix-daemon-upstream.socket -u nix-daemon-upstream.service"
        )
        raise AssertionError(f"{name}:\n- " + "\n- ".join(problems) + f"\n\n{journal}")
