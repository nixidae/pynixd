"""One pytest suite, inside the guest. One script serves every suite phase.

Which suite is `vms.phase`, and `settings.suites` in
`tests/derivations/guest/default.nix` says what each one is. One process
per suite: `tests/unit` and `nix-daemon-protocol/tests` interfere, and one
process for both is how 57 failures once hid behind a green run of 684.
Issue #33.

The suite writes JUnit to `/artifacts/junit/`, and vivarium reads
it back when the phase ends, so every test is a case of this run -- in its
`junit.xml` and in `events.jsonl` -- and not only a line in a log.
"""

from __future__ import annotations

import os
import shlex

from vivarium_runner import Machines

TIMEOUT = 2400
"""Seconds for one suite. Measured in a QEMU guest on an idle host:
`tests/unit` 27.2s, `nix-daemon-protocol/tests` 4.8s, `tests/parity`
122.2s. `tests/unit` takes 87s under UML. The rest is for a loaded
builder."""

SELECT_ENV = "PYNIXD_SUITE_ARGS"
"""Extra pytest arguments for the suite inside the guest, such as `-k name`.

Not the driver's own `-- ...`: vivarium hands those to the pytest that runs
this file, and a `-k` there deselects `test` below. Pair it with `--only`, as
a suite that the selection does not reach exits 5 and fails its phase."""


async def test(vms: Machines) -> None:
    # The phase declares one guest, named after the suite, and `vms`
    # holds only that one.
    [vm] = vms.values()
    name = vms.phase
    if name is None or name not in vms.settings["suites"]:
        raise RuntimeError(f"no suite for phase {name!r}; settings.suites has {list(vms.settings['suites'])}")
    suite = vms.settings["suites"][name]
    select = shlex.join(shlex.split(os.environ.get(SELECT_ENV, "")))

    # Redirected to /artifacts rather than returned: pytest's output is
    # thousands of lines and the way back is a serial line.
    # `PYNIXD_TEST_LOG_DIR` puts the per-test logs there too -- without it
    # a failure names `/tmp/pynixd-logs/...`, which dies with the guest.
    # The environment installs both packages editable from /work:
    # `tests/differential` refuses a pynixd from one tree and a
    # nix_daemon_protocol from another.
    command = (
        f"cd /work && "
        f"NIX_BIN=$(command -v nix) "
        f"NIX_GIT_BIN={vms.settings['nixGit']}/bin/nix "
        f"NIX_FLOOR_BIN={vms.settings['nixFloor']}/bin/nix "
        f"PYNIXD_TEST_LOG_DIR=/artifacts/{name}-logs "
        f"pytest -p no:cacheprovider --tb=short -q {' '.join(suite['flags'])} "
        f"--junitxml=/artifacts/junit/{name}.xml {suite['path']} {select} "
        f"> /artifacts/{name}.log 2>&1"
    )
    user = suite.get("user", "tester")
    # The agent's own cgroup belongs to root, so pynixd could make no cgroup per build (`builder/cgroup.py`).
    # This one is delegated to the suite's user, as `Delegate=yes` delegates a unit's. `su` does not move a
    # process. Root needs one too: in the agent's cgroup, pynixd would move the agent into its leaf.
    cgroup = f"/sys/fs/cgroup/pynixd-tests-{name}"
    # systemd enables the controllers on the path to a delegated unit; the guest's root enables none.
    await vm.succeed(
        "echo '+cpu +memory +io' > /sys/fs/cgroup/cgroup.subtree_control && "
        f"mkdir {cgroup} && "
        f"chown {user} {cgroup} {cgroup}/cgroup.procs {cgroup}/cgroup.threads {cgroup}/cgroup.subtree_control"
    )
    # `su -` and not `su`: a login shell gives the user its own HOME, and
    # pytest writes there. A non-login su leaves HOME as root's.
    rc, _ = await vm.execute(
        f"echo $$ > {cgroup}/cgroup.procs && su - {user} -c {command!r}", timeout=TIMEOUT, label=f"pytest {name}"
    )
    tail = (await vm.succeed(f"tail -n 3 /artifacts/{name}.log")).strip()
    print(f"[test] {name}: exit {rc}\n[test]   {tail}")
    if rc != 0:
        raise AssertionError(f"{suite['path']} exited {rc}: {tail.splitlines()[-1] if tail else ''}")
