"""A compile runs on another machine, which fetches the compiler before it runs.

hub runs the storage and the scheduler, reached over TCP with mutual TLS.
builder runs pynixd, the frontend, and nix-serve. worker runs bb_worker
behind `pynixd reapi-runner`, which makes the closure of each action's
program valid on worker before bb_runner starts it. Without it, bb_runner
fails: "fork/exec .../bin/cc: no such file or directory".

1. With nix-serve stopped, the build fails on the closure, and bb_runner
   never runs the compiler.
2. With nix-serve started, the compile runs on worker, and the compiler is
   valid there afterwards.
3. builder drops its client certificate, and a build fails: the hub
   refuses a client its CA did not sign.
"""

from __future__ import annotations

from remote import build, counts
from vivarium_runner import Machine, Machines


async def valid(vm: Machine, path: str) -> bool:
    done = await vm.execute(f"nix-store --check-validity {path}")
    return done.returncode == 0


async def test(vms: Machines) -> None:
    hub, builder, worker = vms.hub, vms.builder, vms.worker
    cc = vms.settings["cc"]
    await hub.wait_for_unit("buildbarn-storage.service")
    await hub.wait_for_unit("buildbarn-scheduler.service")
    await builder.wait_for_unit("pynixd.service")
    await builder.wait_for_unit("buildbarn-frontend.service")
    await builder.wait_for_unit("nix-serve.service")
    await worker.wait_for_unit("buildbarn-closures.service")
    await worker.wait_for_unit("buildbarn-worker.service")

    if await valid(worker, cc):
        raise AssertionError(f"{cc} is valid on worker before any build: the test proves nothing")

    await builder.succeed("systemctl stop nix-serve.service")
    rc, _, err = await build(builder, vms.settings, "unavailable")
    if rc == 0:
        raise AssertionError("a build succeeded with a compiler worker could not fetch")
    if "pynixd could not make" not in err:
        raise AssertionError(f"the build failed, but not on the closure:\n{err[-4000:]}")
    print(f"[test] with nix-serve stopped, the build fails on the closure:\n{err[-1500:]}")

    await builder.succeed("systemctl start nix-serve.service")
    await builder.wait_for_unit("nix-serve.service")
    rc, out, err = await build(builder, vms.settings, "split")
    if rc != 0:
        raise AssertionError(f"the build failed:\n{err}")
    first = await counts(builder, out.strip())
    print(f"[test] split build: local, remote, cache = {first}")
    if first != (0, 1, 0):
        raise AssertionError(f"the compile did not run on Buildbarn: {first}")
    if not await valid(worker, cc):
        raise AssertionError(f"{cc} is not valid on worker after the compile ran there")
    print("[test] worker fetched the compiler before the compile")

    await builder.switch_to("anonymous")
    rc, _, err = await build(builder, vms.settings, "anonymous")
    if rc == 0:
        raise AssertionError("a build succeeded with no client certificate: the hub admits anyone")
    print(f"[test] with no client certificate, the build fails:\n{err[-2000:]}")
