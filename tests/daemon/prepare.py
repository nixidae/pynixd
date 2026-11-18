"""Both servers answer SSH, and each daemon socket is held by what it should be.

`ss` names the process listening on /nix/var/nix/daemon-socket/socket. On
`daemon` that must be pynixd, and on `control` it is systemd, which holds
nix-daemon's socket for activation. Without this, a `replace` mode that
quietly did nothing would pass every later phase: nix-daemon answers all
of them correctly on its own.
"""

from __future__ import annotations

from daemon_helpers import SERVERS, as_tester
from vivarium_runner import Machines

SOCKET = "/nix/var/nix/daemon-socket/socket"


async def holder(vms: Machines, name: str) -> str:
    return (await vms[name].succeed(f"ss -xlpnH src {SOCKET}")).stdout.strip()


async def test(vms: Machines) -> None:
    await vms.daemon.wait_for_unit("pynixd.service")
    for name in SERVERS:
        await vms[name].wait_for_unit("sshd.service")
    # `replace` masks nix-daemon.socket; see nix/nixos/default.nix.
    await vms.daemon.wait_for_unit("nix-daemon-upstream.socket")
    await vms.control.wait_for_unit("nix-daemon.socket")

    daemon = await holder(vms, "daemon")
    control = await holder(vms, "control")
    print(f"[test] daemon socket on daemon: {daemon}")
    print(f"[test] daemon socket on control: {control}")
    if "python" not in daemon and "pynixd" not in daemon:
        raise AssertionError(f"pynixd does not hold {SOCKET} on daemon: {daemon!r}")
    if "systemd" not in control:
        raise AssertionError(f"systemd does not hold {SOCKET} on control: {control!r}")

    upstream = (await vms.daemon.succeed("ss -xlpnH src /nix/var/nix/daemon-socket/upstream")).stdout.strip()
    if "systemd" not in upstream:
        raise AssertionError(f"nix-daemon's socket did not move behind pynixd: {upstream!r}")
    print(f"[test] nix-daemon behind it: {upstream}")

    for name in SERVERS:
        await as_tester(vms.client, f"ssh {name} true", timeout=60)
    print("[test] the client reaches both servers over SSH as tester")
