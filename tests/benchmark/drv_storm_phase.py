"""Run `drv_storm.py` in the guest: pynixd with no Nix daemon under it, and `nix daemon`, over a store each.

The control says what the machine and the sandbox cost. The numbers decide, not this script: it fails when a storm
fails.
"""

from __future__ import annotations

import dataclasses
import json
import statistics
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from vivarium_runner import Machine, Machines

TIMEOUT: Final = 3600
DISK: Final = "/dev/disk/by-id/virtio-bench"
MOUNT: Final = "/mnt/bench"
SOCKETS: Final = {"nix": "/run/bench/nix.sock", "pynixd": "/run/bench/pynixd.sock"}


async def test(vms: Machines) -> None:
    vm = vms["machine"]
    if vms.phase == "daemons":
        await _daemons(vm, vms.settings["src"], vms.settings["nix"])
        return
    if vms.phase == "storm":
        await _storm(vm)
        return
    raise RuntimeError(f"no drv-storm phase {vms.phase!r}")


def _pynixd_config(nix: str) -> dict[str, object]:  # pynixd's own JSON configuration, passed through untouched
    return {
        "stores": {
            "local": {
                "type": "local-socket",
                "store_path": f"{MOUNT}/pynixd",
                "nix_bin": f"{nix}/bin/nix",
                "monitor": False,
                "probe": False,
                "pynixd_writes": True,
                "daemon": False,
            }
        },
        "ranking": {"min_free_bytes": 0},
        # One storm of 400 builds ends about 2000 spans at once, which is the default queue.
        "tracing": {
            "endpoint": "http://traces:10428/insert/opentelemetry/v1/traces",
            "queue_size": 100_000,
            "schedule_delay": 1.0,
        },
        "unix_path": SOCKETS["pynixd"],
        "ssh_port": None,
        "http_port": None,
    }


async def _daemons(vm: Machine, src: str, nix: str) -> None:
    await vm.succeed("chmod 0777 /artifacts")
    await vm.succeed(f"rm -rf /work && cp -r {src} /work && chmod -R u+w /work")
    await vm.succeed(f"mkfs.ext4 -q -F {DISK} && mkdir -p {MOUNT} /run/bench && mount {DISK} {MOUNT}")
    await vm.succeed(f"mkdir -p {MOUNT}/nix {MOUNT}/pynixd")
    config = json.dumps(_pynixd_config(nix))
    await vm.succeed(f"cat > /run/bench/pynixd.json <<'JSON'\n{config}\nJSON")
    await vm.succeed(
        "systemd-run --unit bench-nix --setenv PATH=/run/current-system/sw/bin "
        f"--setenv NIX_DAEMON_SOCKET_PATH={SOCKETS['nix']} "
        f"{nix}/bin/nix --extra-experimental-features nix-command daemon --store {MOUNT}/nix"
    )
    await vm.succeed(
        "systemd-run --unit bench-pynixd --setenv PATH=/run/current-system/sw/bin "
        "--setenv PYNIXD_CONFIG=/run/bench/pynixd.json --setenv PYNIXD_LOG_LEVEL=INFO "
        "/run/current-system/sw/bin/pynixd daemon"
    )
    for name, socket in SOCKETS.items():
        await vm.wait_until_succeeds(f"nix store info --store unix://{socket}", timeout=60)
        print(f"{name}: up on {socket}")


async def _storm(vm: Machine) -> None:
    daemons = " ".join(
        f"{name}={socket}:$(systemctl show --property MainPID --value bench-{name})" for name, socket in SOCKETS.items()
    )
    command = (
        f"python /work/tests/benchmark/drv_storm.py {daemons} --expression /work/tests/benchmark/drv_storm.nix "
        f"--scratch {MOUNT}/scratch > /artifacts/drv-storm.jsonl 2> /artifacts/drv-storm.log"
    )
    rc = (await vm.execute(command, timeout=TIMEOUT, label="derivation storms")).returncode
    await vm.execute("journalctl --unit bench-pynixd --output cat > /artifacts/pynixd.log")
    await vm.execute("journalctl --unit bench-nix --output cat > /artifacts/nix-daemon.log")
    log = (await vm.execute("tail -n 60 /artifacts/drv-storm.log")).stdout
    if rc != 0:
        raise RuntimeError(f"the storms failed ({rc}):\n{log}")
    text = (await vm.succeed("cat /artifacts/drv-storm.jsonl")).stdout
    _report([_row(line) for line in text.splitlines()])


@dataclasses.dataclass(frozen=True, slots=True)
class Row:
    """What the report reads of a line that `drv_storm.py` prints."""

    daemon: str
    graph: str
    ms_per_build: float
    instantiate_seconds: float
    own_cpu_ms: float
    unit_cpu_ms: float


def _row(line: str) -> Row:
    fields = json.loads(line)
    return Row(
        daemon=fields["daemon"],
        graph=f"{fields['shape']} x{fields['count']}, {fields['inputs']} inputs",
        ms_per_build=fields["ms_per_build"],
        instantiate_seconds=fields["instantiate_seconds"],
        own_cpu_ms=fields["own_cpu_ms"],
        unit_cpu_ms=fields["unit_cpu_ms"],
    )


def _report(rows: list[Row]) -> None:
    for graph in dict.fromkeys(row.graph for row in rows):
        for daemon in SOCKETS:
            mine = [row for row in rows if row.daemon == daemon and row.graph == graph]
            print(
                f"{graph}, {daemon}: {statistics.median(r.ms_per_build for r in mine):.1f} ms per build, "
                f"instantiate {statistics.median(r.instantiate_seconds for r in mine):.2f} s, "
                f"CPU per build {statistics.median(r.own_cpu_ms for r in mine):.0f} ms in the daemon, "
                f"{statistics.median(r.unit_cpu_ms for r in mine):.0f} ms in its whole unit"
            )
