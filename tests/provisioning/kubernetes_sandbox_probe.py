"""What a build sees in a build pod, against Nix's own sandbox and pynixd's on the node. nixidae/nixidae#1560.

One derivation, `sandbox_probe.py` as its builder, built three ways with a nonce each, so no arm reuses
another's output:

  nix         Nix's own sandbox: the guest's `nix-store` into a chroot store, as root with build users
  pynixd      pynixd's helper, on the node: central's `helper` store, whose builds pynixd runs itself
  pod         pynixd's helper in a build pod's sandbox container, at Pod Security `restricted`
  pod-userns  the same in a user-namespaced pod with an unmasked /proc, at `baseline`: the helper makes a
              pid namespace and a /proc of its own (`sibling.fresh_proc_allowed`)

Each arm's JSON goes to `<artifacts>/sandbox-probe/<arm>.json`. Each arm but `nix` is compared with
`nix`, fact by fact, and what differs goes to `differences.json`. It must equal the arm's `KNOWN`: a new
difference and a stale entry both fail.
"""

from __future__ import annotations

import json
import shlex
from collections.abc import Mapping
from typing import Any

import anyio
from daemon_helpers import as_tester, store_path
from vivarium_runner import Machine, Machines

REFERENCE = "nix"
ARMS = (REFERENCE, "pynixd", "pod", "pod-userns")
# Not under /tmp: Nix refuses a build directory below a world-writable one.
NIX_ROOT = "/var/lib/nix-arm"
NIX_STORE = f"local?root={NIX_ROOT}"

type Probe = dict[str, Any]
"""One arm's JSON: nested facts, each a string, a number or a list."""

# What every arm differs from Nix's in by design.
EVERY_ARM = {
    "cgroup": "each arm's build runs under another unit: the agent's, pynixd's, the container's namespace",
    "environment.requiredSystemFeatures": "each arm asks for the feature that sends it to its builder",
}
# What a pod's build differs in, with any template: the pod's user and the container's /dev and limits.
EVERY_POD = {
    **EVERY_ARM,
    # `/bin/sh` is Nix's built-in sandbox shell in every arm; the pod's store does not hold its closure.
    "mounts": "the container's tmpfs /dev nodes; /bin/sh without its store path",
    "uid_map": "outside, the build is the pod's uid 1000, not a build user",
    "gid_map": "outside, the build is the pod's gid 1000, not the build users' group",
    "groups": "the pod's fsGroup stays a supplementary group: the helper cannot drop it without CAP_SETGID",
    "limits.RLIMIT_NPROC": "a container has no process limit; a service has the node's, sized by its memory",
    # The container's profile allows every call (`nix/builder-seccomp.nix`), but it is a filter.
    "status.Seccomp_filters": "the container's allow-all profile is a filter under Nix's",
}
# Measured 2026-11-22: UML guest, containerd, Kubernetes 1.37.
KNOWN: dict[str, dict[str, str]] = {
    "pynixd": EVERY_ARM,
    "pod": {
        **EVERY_POD,
        "mounts": "the container's masked /proc and tmpfs /dev nodes; /bin/sh without its store path",
        # The helper keeps the container's pid namespace, and so binds its masked /proc (`builder/sibling.py`).
        "pid": "the build is not pid 1: the sibling runner and the helper are in its pid namespace",
        "ppid": "the build's parent is the helper, in the same pid namespace",
        "pids visible": "the sibling runner and the helper are visible to the build",
        "pid 1": "pid 1 is the sibling runner, not the build",
    },
    "pod-userns": EVERY_POD,
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
    feature = {
        REFERENCE: "",
        "pynixd": settings["helperFeature"],
        "pod": settings["podBuildFeature"],
        "pod-userns": settings["podBuildUsernsFeature"],
    }[arm]
    drv, out = await instantiate(cp, settings, nonce, feature)
    if arm == REFERENCE:
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


def differences(reference: Probe, probe: Probe) -> dict[str, tuple[str, str]]:
    """Every fact *probe* does not share with *reference*: the reference's value, then the probe's."""
    ours, theirs = flattened(reference), flattened(probe)
    return {
        key: (ours.get(key, "<absent>"), theirs.get(key, "<absent>"))
        for key in sorted(ours.keys() | theirs.keys())
        if ours.get(key) != theirs.get(key)
    }


async def test(vms: Machines) -> None:
    cp, settings = vms.cp, vms.settings
    await cp.wait_for_unit("pynixd.service")
    # The user-namespaced pod's store, a copy the node makes at boot.
    await cp.wait_for_unit("pynixd-podstore.service")
    run = (await cp.succeed("cat /proc/sys/kernel/random/uuid")).stdout.strip()
    probes = {arm: await built(cp, settings, arm, f"{arm}-{run}") for arm in ARMS}

    directory = anyio.Path(vms.artifacts) / "sandbox-probe"
    await directory.mkdir(parents=True, exist_ok=True)
    for arm, probe in probes.items():
        await (directory / f"{arm}.json").write_text(json.dumps(probe, indent=2, sort_keys=True) + "\n")
    found = {arm: differences(probes[REFERENCE], probes[arm]) for arm in ARMS if arm != REFERENCE}
    await (directory / "differences.json").write_text(json.dumps(found, indent=2) + "\n")
    problems = []
    for arm, facts in found.items():
        print(f"[test] {arm} differs from {REFERENCE} in {len(facts)} facts:")
        for key, (want, got) in facts.items():
            print(f"[test]   {key}: {REFERENCE}={want[:120]} | {arm}={got[:120]}")
        if new := sorted(set(facts) - set(KNOWN[arm])):
            problems.append(f"{arm}: facts that differ and KNOWN does not name: {new}")
        if stale := sorted(set(KNOWN[arm]) - set(facts)):
            problems.append(f"{arm}: KNOWN names facts that no longer differ: {stale}")
    assert not problems, "\n".join(problems)
