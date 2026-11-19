"""Run `copy_offload.py` in the guest on each filesystem, keep its results, and check them.

`vms.phase` picks the filesystem. Every approach that ran must give a copy whose NAR hash is
the source's. `stream` and `copy` must run everywhere; `clone` must run, and share its extents,
on btrfs and XFS. The rest is reported, not checked: the numbers decide, not this script.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from vivarium_runner import Machine, Machines

TIMEOUT: Final = 3600
FILESYSTEMS: Final = {
    "btrfs": "mkfs.btrfs -q -f",
    "xfs": "mkfs.xfs -q -f -m reflink=1",
    "ext4": "mkfs.ext4 -q -F",
}
REFLINKS: Final = {"btrfs", "xfs"}
SHARED: Final = 0.1
"""A clone shares its extents when it takes at most this share of the tree's bytes."""


async def test(vms: Machines) -> None:
    [vm] = vms.values()
    if vms.phase == "disks":
        await _disks(vm, vms.settings["src"])
        return
    if vms.phase in FILESYSTEMS:
        fs = vms.phase
        results = await _bench(vm, fs, f"/mnt/{fs}/src", f"/mnt/{fs}/dst")
        _check(results, clone_expected=fs in REFLINKS)
        return
    if vms.phase == "cross":
        # The source on ext4, the target on btrfs: no filesystem can share extents across that.
        results = await _bench(vm, "cross", "/mnt/ext4/src", "/mnt/btrfs/cross")
        _check(results, clone_expected=False)
        return
    raise RuntimeError(f"no copy-offload phase {vms.phase!r}")


async def _disks(vm: Machine, src: str) -> None:
    await vm.succeed("chmod 0777 /artifacts")
    await vm.succeed(f"rm -rf /work && cp -r {src} /work && chmod -R u+w /work")
    for fs, mkfs in FILESYSTEMS.items():
        device = f"/dev/disk/by-id/virtio-{fs}"
        await vm.succeed(f"{mkfs} {device} && mkdir -p /mnt/{fs} && mount {device} /mnt/{fs}")
    print((await vm.succeed("findmnt --list --output TARGET,FSTYPE,SOURCE --types btrfs,xfs,ext4")).stdout)


async def _bench(vm: Machine, scenario: str, src: str, dst: str) -> list[dict[str, object]]:
    out = f"/artifacts/copy-offload-{scenario}.jsonl"
    command = (
        f"cd /work && python -m tests.benchmark.copy_offload --src {src} --dst {dst} --scenario {scenario} "
        f"--drop-caches > {out} 2> /artifacts/copy-offload-{scenario}.log"
    )
    rc = (await vm.execute(command, timeout=TIMEOUT, label=f"copy offload on {scenario}")).returncode
    print((await vm.succeed(f"cat /artifacts/copy-offload-{scenario}.log")).stdout)
    if rc != 0:
        raise AssertionError(f"copy_offload.py on {scenario} exited {rc}")
    return [json.loads(line) for line in (await vm.succeed(f"cat {out}")).stdout.splitlines() if line.strip()]


def _check(results: list[dict[str, object]], *, clone_expected: bool) -> None:
    bad = [r for r in results if r["error"] is None and not r["hash_ok"]]
    if bad:
        raise AssertionError(f"a copy's NAR hash differs from its source's: {bad}")
    failed = [r for r in results if r["approach"] != "clone" and r["error"] is not None]
    if failed:
        raise AssertionError(f"stream, copy or auto failed: {failed}")
    if not clone_expected:
        return
    clones = [r for r in results if r["approach"] in {"clone", "auto"}]
    refused = [r for r in clones if r["error"] is not None]
    if refused:
        raise AssertionError(f"clone refused on a filesystem with reflinks: {refused}")
    unshared = [r for r in clones if _ratio(r) > SHARED]
    if unshared:
        raise AssertionError(f"a clone took more than {SHARED:.0%} of the tree's bytes: {unshared}")


def _ratio(result: dict[str, object]) -> float:
    used, total = result["used_bytes"], result["tree_bytes"]
    if not isinstance(used, int) or not isinstance(total, int):
        raise TypeError(f"used_bytes {used!r}, tree_bytes {total!r}")
    return used / total
