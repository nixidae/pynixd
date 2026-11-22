"""What a build sees in a build pod, against Nix's own sandbox and pynixd's on the node. nixidae/nixidae#1560.

One derivation, `sandbox_probe.py` as its builder, built three ways with a nonce each, so no arm reuses
another's output:

  nix      Nix's own sandbox: the guest's `nix-store` into a chroot store, as root with build users
  pynixd   pynixd's helper, on the node: central's `helper` store, whose builds pynixd runs itself
  pod      pynixd's helper in a build pod's sandbox container

Each arm's JSON goes to `<artifacts>/sandbox-probe/<arm>.json`, and every fact on which the arms disagree
goes to `differences.json`. pynixd against Nix says what pynixd's copy of the sandbox changes; the pod
against pynixd says what the container around it changes.
"""

from __future__ import annotations

import json
import shlex
from collections.abc import Mapping
from typing import Any

import anyio
from daemon_helpers import as_tester, store_path
from vivarium_runner import Machine, Machines

ARMS = ("nix", "pynixd", "pod")
# Not under /tmp: Nix refuses a build directory below a world-writable one.
NIX_ROOT = "/var/lib/nix-arm"
NIX_STORE = f"local?root={NIX_ROOT}"

type Probe = dict[str, Any]
"""One arm's JSON: nested facts, each a string, a number or a list."""

# Every fact the arms disagree on, and why. Measured 2026-11-22 (UML guest, containerd, Kubernetes 1.3x).
# pynixd's helper on the node agrees with Nix on everything but the first two; the rest are the pod's.
KNOWN = {
    "cgroup": "each arm's build runs under another unit: the agent's, pynixd's, the container's namespace",
    "environment.requiredSystemFeatures": "each arm asks for the feature that sends it to its builder",
    # The container's profile under the helper's own filter: see `nix/builder-seccomp.nix`.
    "status.Seccomp_filters": "the pod's build has the container's profile under Nix's filter",
    "syscalls.add_key user": "the container's profile refuses the keyring calls; Nix's filter allows them",
    "syscalls.keyctl session": "the container's profile refuses the keyring calls; Nix's filter allows them",
    "syscalls.fanotify_init": "the profile refuses it with EPERM; on the node the kernel answers ENOSYS",
    "syscalls.get_mempolicy": "the profile refuses the NUMA policy calls; the node's kernel has no NUMA",
    "syscalls.kcmp self": "the profile refuses it; the node's kernel has no kcmp",
    "syscalls.open_by_handle_at null": "the profile refuses it; on the node the kernel reads the bad handle",
    "syscalls.perf_event_open null": "the profile refuses it; the node's kernel has no perf events",
    "syscalls.syslog size": "the profile refuses it; on the node an unprivileged build reads the size",
    "syscalls.userfaultfd": "the profile refuses it; the node's kernel has no userfaultfd",
    # The helper keeps the container's pid namespace, and so binds its masked /proc (`builder/sibling.py`).
    "pid": "the build is not pid 1: the sibling runner and the helper are in its pid namespace",
    "ppid": "the build's parent is the helper, in the same pid namespace",
    "pids visible": "the sibling runner and the helper are visible to the build",
    "pid 1": "pid 1 is the sibling runner, not the build",
    "mounts": "the container's masked /proc and its tmpfs /dev nodes; no busybox store path for /bin/sh",
    # The pod's user, which has no build users and no CAP_SETGID.
    "uid_map": "outside, the build is the pod's uid 1000, not a build user",
    "gid_map": "outside, the build is the pod's gid 1000, not the build users' group",
    "groups": "the pod's fsGroup stays a supplementary group: the helper cannot drop it without CAP_SETGID",
    # containerd's limits for a container, not the node's for a service.
    "limits.RLIMIT_NOFILE": "containerd gives a container a soft limit of 1024",
    "limits.RLIMIT_OFILE": "the same limit as RLIMIT_NOFILE, by its old name",
    "limits.RLIMIT_NPROC": "a container has no process limit",
}


def expression(settings: Mapping[str, str], nonce: str, feature: str) -> str:
    python = f'builtins.storePath "{settings["probePython"]}"'
    script = f'builtins.storePath "{settings["sandboxProbe"]}"'
    return (
        "derivation {"
        ' name = "sandbox-probe";'
        f' system = "{settings["system"]}";'
        f' builder = "${{{python}}}/bin/python3";'
        f" args = [ ({script}) ];"
        f' nonce = "{nonce}";'
        f" requiredSystemFeatures = [ {f'{json.dumps(feature)}' if feature else ''} ];"
        " }"
    )


async def instantiate(cp: Machine, settings: Mapping[str, str], nonce: str, feature: str) -> tuple[str, str]:
    drv = store_path(await as_tester(cp, f"nix-instantiate --expr {shlex.quote(expression(settings, nonce, feature))}"))
    out = (await cp.succeed(f"nix-store -q --outputs {drv}")).stdout.strip()
    return drv, out


async def built(cp: Machine, settings: Mapping[str, str], arm: str, nonce: str) -> Probe:
    feature = {"nix": "", "pynixd": settings["helperFeature"], "pod": settings["podBuildFeature"]}[arm]
    drv, out = await instantiate(cp, settings, nonce, feature)
    if arm == "nix":
        await cp.succeed(
            f"nix --extra-experimental-features nix-command copy --no-check-sigs --to {shlex.quote(NIX_STORE)} {drv}"
        )
        await cp.succeed(f"nix-store --store {shlex.quote(NIX_STORE)} --realise {drv}", timeout=600)
        text = (await cp.succeed(f"cat {NIX_ROOT}{out}")).stdout
    else:
        await as_tester(cp, f"nix-store --realise {drv}", timeout=600)
        text = (await cp.succeed(f"cat {out}")).stdout
    probe: Probe = json.loads(text)
    return probe


def flattened(value: object, prefix: str = "") -> dict[str, str]:
    """Each leaf of *value* under its dotted path; a list stays one leaf, as JSON."""
    if isinstance(value, dict):
        leaves: dict[str, str] = {}
        for key, inner in value.items():
            leaves |= flattened(inner, f"{prefix}{key}.")
        return leaves
    return {prefix.removesuffix("."): value if isinstance(value, str) else json.dumps(value)}


def differences(probes: Mapping[str, Probe]) -> dict[str, dict[str, str]]:
    """Every fact the arms do not all agree on, with each arm's value."""
    flat = {arm: flattened(probe) for arm, probe in probes.items()}
    keys = sorted(set().union(*flat.values()))
    return {
        key: {arm: leaves.get(key, "<absent>") for arm, leaves in flat.items()}
        for key in keys
        if len({leaves.get(key, "<absent>") for leaves in flat.values()}) > 1
    }


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    await cp.wait_for_unit("pynixd.service")
    run = (await cp.succeed("cat /proc/sys/kernel/random/uuid")).stdout.strip()
    probes = {arm: await built(cp, settings, arm, f"{arm}-{run}") for arm in ARMS}

    directory = anyio.Path(vms.artifacts) / "sandbox-probe"
    await directory.mkdir(parents=True, exist_ok=True)
    for arm, probe in probes.items():
        await (directory / f"{arm}.json").write_text(json.dumps(probe, indent=2, sort_keys=True) + "\n")
    found = differences(probes)
    await (directory / "differences.json").write_text(json.dumps(found, indent=2) + "\n")
    print(f"[test] {len(found)} facts differ between {', '.join(ARMS)}:")
    for key, values in found.items():
        print(f"[test]   {key}: " + " | ".join(f"{arm}={value[:160]}" for arm, value in values.items()))
    new = sorted(set(found) - set(KNOWN))
    stale = sorted(set(KNOWN) - set(found))
    assert not new, f"facts that differ and KNOWN does not name: {new}"
    assert not stale, f"KNOWN names facts that no longer differ: {stale}"
