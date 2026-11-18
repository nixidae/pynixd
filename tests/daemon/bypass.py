"""Without pynixd, a user on `daemon` has no daemon: the requests needed it.

The causal half of `prepare`'s check that pynixd holds the socket. With
pynixd stopped and nix-daemon still running behind it, a user's request
must fail; started again, it must work. A `replace` mode that left some
path around pynixd -- a client reading `upstream` directly -- would pass
the earlier phases and fail here.

The same over OpenSSH: `ssh-ng://` runs `nix-daemon --stdio` on `daemon`,
which must reach the store only through pynixd. The request after the
restart is the control: it proves the ssh path itself works.
"""

from __future__ import annotations

from daemon_helpers import as_tester
from vivarium_runner import Machines

PROBE = "nix-store --query --hash {busybox}"
FAR = "nix path-info --store ssh-ng://tester@daemon {busybox}"


async def test(vms: Machines) -> None:
    vm = vms.daemon
    probe = PROBE.format(busybox=vms.settings["busybox"])
    far = FAR.format(busybox=vms.settings["busybox"])
    # The socket too: it would start pynixd again for the request.
    await vm.succeed("systemctl stop pynixd.socket pynixd.service")
    try:
        state = (await vm.succeed("systemctl is-active nix-daemon-upstream.socket")).stdout.strip()
        done = await vm.execute(f"su - tester -c {probe!r}")
        print(f"[test] pynixd stopped, nix-daemon-upstream.socket {state}: exit {done.returncode}")
        if done.returncode == 0:
            raise AssertionError(f"a user's request succeeded with pynixd stopped: {done}")
        done = await vms.client.execute(f"su - tester -c {far!r}", timeout=60)
        print(f"[test] pynixd stopped, the client over ssh-ng: exit {done.returncode}")
        if done.returncode == 0:
            raise AssertionError(f"a request over ssh-ng succeeded with pynixd stopped: {done}")
    finally:
        await vm.succeed("systemctl start pynixd.socket pynixd.service")
    await vm.wait_for_unit("pynixd.service")
    print(f"[test] pynixd started again: {(await as_tester(vm, probe)).strip()}")
    print(f"[test] and over ssh-ng: {(await as_tester(vms.client, far)).strip()}")
