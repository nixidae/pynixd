"""A sandboxed build runs its compile on Buildbarn, and a second build hits Buildbarn's cache.

Each job is its own derivation that requires `reapi`, built by tester
through pynixd. Its builder runs Siso against the frontend socket the
sandbox has at /run/reapi.sock (services.buildbarn.nixSandbox). Siso's
summary line says where each step ran: `remote:1` the first time,
`cache:1` for a second derivation with the same compile. With the frontend
stopped, the build fails: the step may not run locally.
"""

from __future__ import annotations

import re
import shlex
from collections.abc import Mapping

from vivarium_runner import Machine, Machines

SUMMARY = re.compile(r"noexec:\d+ local:(\d+) remote:(\d+) cache:(\d+)")


async def as_tester(vm: Machine, command: str) -> tuple[int, str, str]:
    done = await vm.execute(f"su - tester -c {shlex.quote(command)}", timeout=300)
    return done.returncode, done.stdout, done.stderr


def expression(settings: Mapping[str, str], job: str) -> str:
    def path(name: str) -> str:
        return f'builtins.storePath "{settings[name]}"'

    return (
        "derivation {"
        f' name = "buildbarn-{job}";'
        f' system = "{settings["system"]}";'
        f' builder = "${{{path("bash")}}}/bin/bash";'
        f" args = [ ({path('script')}) ];"
        f" siso = {path('siso')}; cc = {path('cc')}; coreutils = {path('coreutils')};"
        ' requiredSystemFeatures = [ "reapi" ];'
        " }"
    )


async def build(vm: Machine, settings: Mapping[str, str], job: str) -> tuple[int, str, str]:
    command = f"nix-build --no-out-link --expr {shlex.quote(expression(settings, job))}"
    return await as_tester(vm, command)


async def counts(vm: Machine, out: str) -> tuple[int, int, int]:
    log = (await vm.succeed(f"cat {out}/siso.log")).stdout
    found = SUMMARY.search(log)
    if found is None:
        raise AssertionError(f"no step summary in Siso's log:\n{log}")
    local, remote, cache = (int(n) for n in found.groups())
    return local, remote, cache


async def test(vms: Machines) -> None:
    vm = vms.builder
    await vm.wait_for_unit("pynixd.service")
    await vm.wait_for_unit("buildbarn-worker.service")
    await vm.wait_for_unit("buildbarn-frontend.service")

    rc, out, err = await build(vm, vms.settings, "first")
    if rc != 0:
        raise AssertionError(f"the first build failed:\n{err}")
    first = await counts(vm, out.strip())
    print(f"[test] first build: local, remote, cache = {first}")
    if first != (0, 1, 0):
        raise AssertionError(f"the compile did not run on Buildbarn: {first}")

    rc, out, err = await build(vm, vms.settings, "second")
    if rc != 0:
        raise AssertionError(f"the second build failed:\n{err}")
    second = await counts(vm, out.strip())
    print(f"[test] second build: local, remote, cache = {second}")
    if second != (0, 0, 1):
        raise AssertionError(f"the second derivation did not hit Buildbarn's cache: {second}")

    await vm.succeed("systemctl stop buildbarn-frontend.service")
    rc, _, err = await build(vm, vms.settings, "unreachable")
    if rc == 0:
        raise AssertionError("a build succeeded with the frontend stopped: the step ran somewhere else")
    print("[test] with the frontend stopped, the build fails")
