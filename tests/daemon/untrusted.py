"""An untrusted user on each server: what Nix refuses, pynixd refuses the same way.

`tester` is not in `trusted-users` on either server, and `stranger` is not in
`allowed-users`. Each probe hits one of the rules pynixd ports from
`src/libstore/daemon.cc` (issue #56). The exit status and the client's
`error:` and `warning:` lines must match between `daemon` and `control`.
"""

from __future__ import annotations

import json

from daemon_helpers import SERVERS, as_tester, attempt, errors, instantiate, store_path
from vivarium_runner import Machines

PROBES = {
    "restricted setting": "nix-store --option require-sigs false --query --hash {busybox}",
    "repair build": "nix-store --realise --repair {busybox}",
    "repair verify": "nix-store --verify --repair",
    "verify": "nix-store --verify",
    "optimise": "nix-store --optimise",
    "dead paths": "nix-store --gc --print-dead",
    "roots": "nix-store --gc --print-roots",
}


async def test(vms: Machines) -> None:
    busybox = vms.settings["busybox"]
    answers: dict[str, dict[str, object]] = {name: {} for name in SERVERS}

    for name in SERVERS:
        vm = vms[name]
        info = json.loads(await as_tester(vm, "nix store info --json"))
        answers[name]["trusted"] = info.get("trusted")
        for probe, command in PROBES.items():
            rc, stderr = await attempt(vm, command.format(busybox=busybox))
            answers[name][probe] = [rc, errors(stderr)]
        rc, stderr = await attempt(vm, f"nix-store --query --hash {busybox}", user="stranger")
        answers[name]["stranger"] = [rc != 0]
        print(f"[test] {name}: stranger refused with {errors(stderr)}")
        # The roots of another user's processes are not tester's to read.
        roots = (await vm.execute(f"su - tester -c {'nix-store --gc --print-roots'!r} 2>/dev/null")).stdout
        answers[name]["uncensored roots"] = [
            line for line in roots.splitlines() if "{temp:" in line or "/proc/" in line
        ]

    # An unsigned, input-addressed path from the client, where tester is trusted.
    drv = await instantiate(vms.client, vms.settings, "unsigned")
    out = store_path(await as_tester(vms.client, f"nix-store --realise {drv}"))
    for name in SERVERS:
        rc, stderr = await attempt(vms.client, f"nix copy --no-check-sigs --to ssh-ng://tester@{name} {out}")
        answers[name]["unsigned import"] = [rc, errors(stderr)]

    (vms.artifacts / "daemon" / "untrusted.json").write_text(json.dumps(answers, indent=2) + "\n")
    daemon, control = answers["daemon"], answers["control"]
    for probe in daemon:
        print(f"[test] {probe}: daemon {daemon[probe]} control {control[probe]}")
    diff = {probe: (daemon[probe], control[probe]) for probe in daemon if daemon[probe] != control[probe]}
    if diff:
        raise AssertionError(f"pynixd and nix-daemon treat an untrusted user differently: {diff}")
    if control["trusted"] != 0:
        raise AssertionError(f"tester is trusted on control, so this phase proves nothing: {control['trusted']}")
    print("[test] every probe agrees, and tester is untrusted on both")
