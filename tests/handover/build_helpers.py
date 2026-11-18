"""Builds a user starts through the daemon socket, and what became of them: shared by tests.handover and tests.upgrade."""

from __future__ import annotations

import shlex
import time
from collections.abc import Awaitable, Callable, Mapping

import anyio
from daemon_helpers import as_tester, store_path
from vivarium_runner import Machine

BUILD_SECONDS = 40


async def long_drv(vm: Machine, settings: Mapping[str, str], job: str, seconds: int = BUILD_SECONDS) -> tuple[str, str]:
    """A derivation that ticks once a second for `seconds`: its `.drv` and its output."""
    script = (
        f"i=0; while [ $i -lt {seconds} ]; do echo tick {job} $i;"
        f" ${{bb}}/bin/sleep 1; i=$((i+1)); done; echo {job} > $out"
    )
    expression = (
        f'let bb = builtins.storePath "{settings["busybox"]}"; in derivation {{'
        f' name = "handover-{job}";'
        f' system = "{settings["system"]}";'
        ' builder = "${bb}/bin/sh";'
        f' args = [ "-c" {nix_string(script)} ];'
        " }"
    )
    drv = store_path(await as_tester(vm, f"nix-instantiate --expr {shlex.quote(expression)}"))
    out = (await vm.succeed(f"nix-store -q --outputs {drv}")).stdout.strip()
    return drv, out


def nix_string(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


async def builder_alive(vm: Machine, job: str) -> bool:
    """Whether the build's shell still runs. Its arguments carry the job's name."""
    # `-ww`: off a terminal, ps cuts `args` at 80 columns, before the job's name.
    processes = (await vm.succeed("ps -ww -eo pid,args")).stdout
    return any(line.split()[1:2] == ["sh"] and f"tick {job} " in line for line in processes.splitlines())


async def until(check: Callable[[], Awaitable[bool]], timeout: float, step: float = 0.5) -> float | None:
    """Seconds until `check` holds, or None when it never did."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if await check():
            return time.monotonic() - start
        await anyio.sleep(step)
    return None


async def read(vm: Machine, path: str) -> str:
    return (await vm.succeed(f"cat {path} 2>/dev/null || true")).stdout.strip()


async def background(vm: Machine, unit: str, command: str, *, user: str | None = None) -> None:
    """Start `command` in a transient unit; its exit status lands in /tmp/<unit>.rc.

    `KillMode=process`: a child the command forks outlives it. Under the
    default, the unit's end killed the child that held the connection.
    """
    wrapped = f"{command}; echo $? > /tmp/{unit}.rc"
    uid = f" --uid={user}" if user else ""
    await vm.succeed(
        f"rm -f /tmp/{unit}.rc /tmp/{unit}.out; systemd-run --quiet --unit={unit}{uid} -p KillMode=process"
        f" -p StandardOutput=file:/tmp/{unit}.out -p StandardError=append:/tmp/{unit}.out"
        f" /bin/sh -c {shlex.quote(wrapped)}"
    )


async def client_build(vm: Machine, unit: str, drv: str) -> None:
    """A user's `nix-store --realise` through the default socket, which pynixd holds."""
    await background(vm, unit, f"/run/current-system/sw/bin/nix-store --realise {drv}", user="tester")


async def client_outcome(vm: Machine, unit: str, timeout: float) -> tuple[str, str]:
    """The client's exit status and the last lines of its output."""
    await until(lambda: exists(vm, f"/tmp/{unit}.rc"), timeout)
    rc = await read(vm, f"/tmp/{unit}.rc") or "still running"
    tail = (await vm.succeed(f"tail -n 4 /tmp/{unit}.out 2>/dev/null || true")).stdout
    return rc, " | ".join(line.strip() for line in tail.splitlines() if line.strip())


async def exists(vm: Machine, path: str) -> bool:
    rc = (await vm.execute(f"test -e {path}")).returncode
    return rc == 0


async def valid(vm: Machine, path: str) -> bool:
    rc = (await vm.execute(f"nix-store --check-validity {path}")).returncode
    return rc == 0


async def main_pid(vm: Machine, unit: str) -> str:
    return (await vm.succeed(f"systemctl show -P MainPID {unit}")).stdout.strip()
