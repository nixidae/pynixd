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
3. A C++ compile through `remote_wrapper` (lib.buildbarn.remoteCompile)
   gets the cc-wrapper's environment on worker. A second derivation with
   another random seed hits the cache. Without the define in the
   environment, the compile fails on its `#error`.
4. builder drops its client certificate, and a build fails: the hub
   refuses a client its CA did not sign.
"""

from __future__ import annotations

from remote import build, build_cxx, counts
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

    rc, out, err = await build_cxx(builder, vms.settings, "cxx-env", "-DNIX_REMOTE_ENV_OK")
    if rc != 0:
        raise AssertionError(f"the C++ compile with the environment failed:\n{err}")
    cxx = await counts(builder, out.strip())
    print(f"[test] C++ through remote_wrapper: local, remote, cache = {cxx}")
    if cxx != (0, 1, 0):
        raise AssertionError(f"the C++ compile did not run on Buildbarn: {cxx}")

    rc, out, err = await build_cxx(builder, vms.settings, "cxx-env-again", "-DNIX_REMOTE_ENV_OK")
    if rc != 0:
        raise AssertionError(f"the second C++ derivation failed:\n{err}")
    again = await counts(builder, out.strip())
    print(f"[test] another derivation, another seed: local, remote, cache = {again}")
    if again != (0, 0, 1):
        raise AssertionError(f"the second derivation missed the cache: the environment differs per derivation: {again}")

    rc, _, err = await build_cxx(builder, vms.settings, "cxx-no-env", "-DUNRELATED")
    if rc == 0:
        raise AssertionError("a compile without NIX_REMOTE_ENV_OK succeeded: the source's #error never ran")
    if "NIX_REMOTE_ENV_OK is not defined" not in err:
        raise AssertionError(f"the compile without the define failed, but not on its #error:\n{err[-4000:]}")
    print("[test] without the define in the environment, the compile fails on its #error")

    await builder.switch_to("anonymous")
    rc, _, err = await build(builder, vms.settings, "anonymous")
    if rc == 0:
        raise AssertionError("a build succeeded with no client certificate: the hub admits anyone")
    print(f"[test] with no client certificate, the build fails:\n{err[-2000:]}")
