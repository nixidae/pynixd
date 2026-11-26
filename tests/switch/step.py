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

from daemon_helpers import as_tester, attempt, instantiate, store_path
from vivarium_runner import Machine, Machines

if TYPE_CHECKING:
    from collections.abc import Mapping

DEFAULT = "/nix/var/nix/daemon-socket/socket"
UPSTREAM = "/nix/var/nix/daemon-socket/upstream"
BESIDE = "/run/pynixd/pynixd.sock"
FALLBACK = "fallback"
"""A step's `to` for the daemonless system's specialisation with nix-daemon alone (`services.pynixd.fallback`)."""
FALLBACK_NAME = "nix-daemon"
PYNIXD_HOLDS_DEFAULT = frozenset({"replace", "daemonless"})


class Step(TypedDict):
    """One entry of the spec's `steps`: the switch target, or `False` for the booted system."""

    name: str
    mode: str
    to: str | Literal[False]
    nix: NotRequired[str]


async def probes(vm: Machine) -> list[str]:
    """The outputs of probe builds: `<hash>-probe-system-<system>` and `-probe-feature-<feature>`."""
    return (await vm.succeed("ls /nix/store | grep -- '-probe-' || true")).stdout.split()


async def holder(vm: Machine, socket: str) -> str:
    return (await vm.succeed(f"ss -xlpnH src {socket}")).stdout.strip()


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
    probes_before = set(await probes(vm))

    if step["to"] is False:
        await vm.wait_for_unit("multi-user.target")
    elif step["to"] == FALLBACK:
        # As an operator would with a broken pynixd: no build, and the boot entry stays.
        target = (await vm.succeed(f"readlink -f /run/current-system/specialisation/{FALLBACK_NAME}")).stdout.strip()
        done = await vm.switch_to(target, "test", check=False)
        print(f"[test] switch to the fallback {target}: exit {done.returncode}")
        if done.returncode != 0:
            problems.append(f"switch-to-configuration exited {done.returncode}:\n{done.stdout}\n{done.stderr}")
    else:
        done = await vm.switch_to(step["to"], check=False)
        print(f"[test] switch to {step['to'] or 'the booted system'}: exit {done.returncode}")
        if done.returncode != 0:
            problems.append(f"switch-to-configuration exited {done.returncode}:\n{done.stdout}\n{done.stderr}")

    failed = (await vm.succeed("systemctl --failed --no-legend --plain")).stdout.strip()
    if failed:
        problems.append(f"failed units: {failed}")

    default = await holder(vm, DEFAULT)
    upstream = await holder(vm, UPSTREAM)
    print(f"[test] {DEFAULT}: {default or 'nobody'}")
    print(f"[test] {UPSTREAM}: {upstream or 'nobody'}")
    if mode in PYNIXD_HOLDS_DEFAULT:
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
        pid = (await vm.succeed("systemctl show -P MainPID nix-daemon-upstream.service")).stdout.strip()
        running = (await vm.succeed(f"$(readlink /proc/{pid}/exe) --version")).stdout
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

    if mode == "daemonless":
        # An earlier mode's client left it running; its socket starts it again only for a client.
        await vm.succeed("systemctl stop nix-daemon-upstream.service")

    # A user's build through the default socket, whoever holds it. Root
    # would open the store directly and reach no daemon at all.
    try:
        built = await build(vm, settings, name)
        print(f"[test] tester built {built}")
        vms.shared.setdefault("built", []).append((name, mode, built))
        if mode == "beside":
            out = await build(vm, settings, f"{name}-beside", f"unix://{BESIDE}")
            print(f"[test] tester built {out} through {BESIDE}")
    except Exception as error:
        problems.append(f"a user's build failed: {error}")

    # The machine's own daemon is described by its nix.conf: probe builds
    # before READY=1 wait for build slots on a busy machine (#58). A
    # daemonless pynixd proves its own sandbox with them (nixidae/nixidae#1541).
    # pynixd's journal holds warnings only.
    new_probes = sorted(set(await probes(vm)) - probes_before)
    if new_probes and mode != "daemonless":
        problems.append(f"pynixd probed the machine's own daemon with {len(new_probes)} builds: {new_probes[:3]}")

    if mode == "daemonless":
        # No client came to the upstream socket, so pynixd built alone.
        upstream_daemon = await vm.unit_state("nix-daemon-upstream.service")
        if upstream_daemon != "inactive":
            problems.append(f"nix-daemon-upstream.service is {upstream_daemon} under a daemonless pynixd")

    # Whoever serves the store now reads what each earlier mode built, and its files are intact.
    for earlier, by, path in vms.shared.get("built", []):
        if by == mode or earlier == name:
            continue
        # As tester, so the daemon of this mode answers; root would open the store itself.
        invalid, why = await attempt(vm, f"nix-store --check-validity {path}")
        intact = await vm.execute(f"nix-store --verify-path {path}")
        if invalid or intact.returncode:
            problems.append(
                f"{path}, built in {earlier} ({by}), is not valid and intact in {mode}: {why}{intact.stderr}"
            )
    print(f"[test] {mode} reads every path the other modes built")

    # A client that comes while pynixd is down waits for it rather than
    # failing: the socket stays bound and starts pynixd (#59).
    if mode in PYNIXD_HOLDS_DEFAULT and not problems:
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
        journal = (
            await vm.succeed(
                "journalctl --no-pager -n 60 -u pynixd.service -u pynixd.socket -u nix-daemon.socket"
                " -u nix-daemon.service -u nix-daemon-upstream.socket -u nix-daemon-upstream.service"
            )
        ).stdout
        raise AssertionError(f"{name}:\n- " + "\n- ".join(problems) + f"\n\n{journal}")
