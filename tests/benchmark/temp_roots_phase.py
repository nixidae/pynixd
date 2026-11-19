"""Run `temp_roots.py` in the guest on each disk model, keep its results, and check each model.

`vms.phase` picks the model. A model counts only when the shipped inline write stalls the
event loop under it: that is the fault sg1's pynixd-0 died of, and a model where inline does
not stall says nothing about which approach avoids it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from vivarium_runner import Machine, Machines

TIMEOUT: Final = 1800
DISK: Final = "/dev/disk/by-id/virtio-bench"
DELAY_MS: Final = 50
"""dm-delay on every read and write of the slow half: a network block device under load.

Reads too: with fast reads the kernel reclaims clean page cache for free, and memory pressure
never stalls anything (memory PSI stayed 0 with 91 MB of 2 GB available)."""
FREEZE_SECONDS: Final = 2.0
STALLED_MS: Final = 100.0
"""A model reproduces the fault when inline stalls the loop at least this long."""
SQUEEZED: Final = 0.2
"""The pressure model holds when at most this share of memory is still available."""
OFF_LOOP_STALL_MS: Final = 500.0
"""Under a freeze, an approach that keeps the disk off the loop must stay under this."""
DIRTY_LOAD: Final = "while :; do dd if=/dev/zero of=/mnt/slow/fill bs=1M count=256 conv=notrunc status=none; done"
READ_LOAD: Final = "while :; do cat /mnt/slow/fill > /dev/null; done"
HOG_HEADROOM_KB: Final = 200 * 1024
"""What the static hog leaves of the memory available when it starts; the churn takes more than that."""
CHURN: Final = "choom -n 1000 -- /run/current-system/sw/bin/stress-ng --vm 2 --vm-bytes {kb}k --quiet"
"""Allocation faster than kswapd frees: stress-ng without `--vm-keep` maps and unmaps its memory over
and over, together more than the hog left. Every allocation then enters direct reclaim, and the
OOM killer takes the churn workers (stress-ng restarts them), as on sg1-n3 with 31 OOM kills."""
# `systemd-run` gives the unit no system PATH, so the hog names its binary.
HOG: Final = (
    "choom -n 1000 -- /run/current-system/sw/bin/python3 -c "
    "'import time; hold = b\"\\x01\" * ({kb} * 1024); time.sleep(86400)'"
)
"""Anonymous memory up to the edge of OOM, so every page the kernel hands out comes from reclaim.

With no swap, anonymous memory cannot be reclaimed, so the kernel takes every page from the
page cache, which a reader on the slow disk keeps filling. Python, not stress-ng: stress-ng
filled 735 MB in 5 s, and its `--vm-bytes N%` left 20% of the guest available.

The hog takes the OOM killer's attention (`choom`), not the benchmark. sg1-n3 at the stall:
memory PSI full 0.64-0.84 s/s and IO PSI full 0.55-0.80 s/s, with dirty bytes low.
"""


async def test(vms: Machines) -> None:
    [vm] = vms.values()
    if vms.phase == "disks":
        await _disks(vm, vms.settings["src"])
        return
    if vms.phase == "fast":
        results = await _bench(vm, "fast", "/mnt/fast/bench")
        _report(results)
        return
    if vms.phase == "frozen":
        results = await _bench(vm, "frozen", "/mnt/fast/bench", f"--freeze /mnt/fast --freeze-seconds {FREEZE_SECONDS}")
        _report(results)
        _check_frozen(results)
        return
    if vms.phase == "dirty":
        await vm.succeed("sysctl -w vm.dirty_bytes=16777216 vm.dirty_background_bytes=4194304")
        await vm.succeed(f"systemd-run --collect --unit=dirty-load sh -c '{DIRTY_LOAD}'")
        try:
            await vm.succeed("sleep 3")
            results = await _bench(vm, "dirty", "/mnt/slow/bench")
        finally:
            await vm.succeed("systemctl stop dirty-load")
        _report(results)
        _check_reproduces(results, "dirty")
        return
    if vms.phase == "pressure":
        await vm.succeed(
            # Writing a ratio clears the byte limit the `dirty` phase set.
            "sysctl -w vm.dirty_ratio=20 vm.dirty_background_ratio=10"
        )
        await vm.succeed("dd if=/dev/zero of=/mnt/slow/fill bs=1M count=256 status=none")
        await vm.succeed(f"systemd-run --collect --unit=read-load sh -c '{READ_LOAD}'")
        available = (await _meminfo(vm))["MemAvailable"]
        hog = HOG.format(kb=available - HOG_HEADROOM_KB)
        await vm.succeed(f"systemd-run --collect --unit=hog {hog}")
        await vm.succeed("sleep 3")
        churn = CHURN.format(kb=HOG_HEADROOM_KB * 3 // 4)
        await vm.succeed(f"systemd-run --collect --unit=churn {churn}")
        try:
            await vm.succeed("sleep 5")
            await _check_squeezed(vm)
            print(
                (
                    await vm.succeed(
                        "cat /proc/pressure/memory /proc/pressure/io; grep -E 'MemAvailable|Dirty' /proc/meminfo"
                    )
                ).stdout
            )
            results = await _bench(vm, "pressure", "/mnt/slow/bench")
            print((await vm.succeed("cat /proc/pressure/memory /proc/pressure/io")).stdout)
        finally:
            await vm.execute("systemctl stop churn hog read-load")
        _report(results)
        _check_reproduces(results, "pressure")
        return
    raise RuntimeError(f"no temp-roots model for phase {vms.phase!r}")


async def _disks(vm: Machine, src: str) -> None:
    """Two filesystems on one disk: `fast` straight through, `slow` behind dm-delay."""
    await vm.succeed("chmod 0777 /artifacts")
    await vm.succeed(f"rm -rf /work && cp -r {src} /work && chmod -R u+w /work")
    await vm.succeed("modprobe dm_delay")
    sectors = int((await vm.succeed(f"blockdev --getsz {DISK}")).stdout.strip())
    half = sectors // 2
    await vm.succeed(f"echo '0 {half} linear {DISK} 0' | dmsetup create fast")
    await vm.succeed(f"echo '0 {half} delay {DISK} {half} {DELAY_MS}' | dmsetup create slow")
    for name in ("fast", "slow"):
        await vm.succeed(
            f"mkfs.ext4 -q /dev/mapper/{name} && mkdir -p /mnt/{name} && mount /dev/mapper/{name} /mnt/{name}"
        )
    print((await vm.succeed("dmsetup table && findmnt --types ext4")).stdout)


async def _bench(vm: Machine, scenario: str, directory: str, extra: str = "") -> list[dict[str, object]]:
    out = f"/artifacts/temp-roots-{scenario}.jsonl"
    command = (
        f"cd /work && python tests/benchmark/temp_roots.py --dir {directory} --scenario {scenario} {extra} "
        f"> {out} 2> /artifacts/temp-roots-{scenario}.log"
    )
    rc = (await vm.execute(command, timeout=TIMEOUT, label=f"temp roots on {scenario}")).returncode
    print((await vm.succeed(f"cat /artifacts/temp-roots-{scenario}.log")).stdout)
    if rc != 0:
        raise AssertionError(f"temp_roots.py on {scenario} exited {rc}")
    return [json.loads(line) for line in (await vm.succeed(f"cat {out}")).stdout.splitlines() if line.strip()]


def _report(results: list[dict[str, object]]) -> None:
    print(f"[temp-roots] {len(results)} runs")


def _stall(result: dict[str, object]) -> float:
    value = result["stall_max_ms"]
    if not isinstance(value, (int, float)):
        raise TypeError(f"stall_max_ms is {value!r}")
    return float(value)


async def _meminfo(vm: Machine) -> dict[str, int]:
    """/proc/meminfo in kB, the lines that carry a number."""
    fields = {}
    for line in (await vm.succeed("cat /proc/meminfo")).stdout.splitlines():
        name, _, value = line.partition(":")
        fields[name] = int(value.split()[0])
    return fields


async def _check_squeezed(vm: Machine) -> None:
    """The model's own control: the hog runs and holds the memory, or the phase measures nothing."""
    await vm.succeed("systemctl is-active hog")
    meminfo = await _meminfo(vm)
    available, total = meminfo["MemAvailable"], meminfo["MemTotal"]
    if available > total * SQUEEZED:
        raise AssertionError(f"pressure: {available} of {total} kB still available; the hog holds too little")


def _check_reproduces(results: list[dict[str, object]], scenario: str) -> None:
    """The negative control: inline must stall here, or this model is not the fault."""
    inline = [_stall(r) for r in results if r["approach"] == "inline"]
    if max(inline) < STALLED_MS:
        raise AssertionError(
            f"{scenario}: inline stalled the loop {max(inline):.1f} ms at most, under {STALLED_MS} ms: "
            "this model does not reproduce the fault, so its other numbers decide nothing"
        )


def _check_frozen(results: list[dict[str, object]]) -> None:
    _check_reproduces(results, "frozen")
    floor = FREEZE_SECONDS * 1000 * 0.75
    short = [r for r in results if r["approach"] == "inline" and _stall(r) < floor]
    if short:
        raise AssertionError(f"frozen: inline stalled less than {floor:.0f} ms in {len(short)} runs: {short}")
    stalled = [r for r in results if r["approach"] != "inline" and _stall(r) >= OFF_LOOP_STALL_MS]
    if stalled:
        raise AssertionError(f"frozen: an off-loop approach stalled the loop {OFF_LOOP_STALL_MS} ms or more: {stalled}")
