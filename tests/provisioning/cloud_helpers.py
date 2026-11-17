"""What the provisioning phases share: vivarium's cloud API, the provisioner's table, a build only the pool takes."""

from __future__ import annotations

import json
import shlex
from typing import TYPE_CHECKING, Any

import anyio
from daemon_helpers import as_tester, store_path

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from vivarium_runner import Machine

POOL_TAG = "pynixd-pool"
DB = "/nix/var/nix/db/db.sqlite"
TABLE = "PynixdProvisionedInstance"
API = "curl --silent --show-error --unix-socket /run/vivarium/cloud.sock --write-out '\\n%{http_code}'"


async def call(vm: Machine, method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, Any]:
    data = "" if body is None else f" --header 'Content-Type: application/json' --data {shlex.quote(json.dumps(body))}"
    out = await vm.succeed(f"{API} --request {method}{data} http://cloud{path}")
    text, _, status = out.rpartition("\n")
    return int(status), json.loads(text)


async def pool_instances(vm: Machine) -> list[dict[str, Any]]:
    _, listed = await call(vm, "GET", f"/v1/instances?tag:{POOL_TAG}=big")
    return list(listed["instances"])


async def rows(vm: Machine) -> list[dict[str, Any]]:
    """The provisioner's rows, oldest first."""
    out = await vm.succeed(
        f"sqlite3 -json {DB} 'SELECT id, state, storeId, endReason FROM {TABLE} ORDER BY launchedAtMs, id'"
    )
    return list(json.loads(out or "[]"))


async def exists(vm: Machine, path: str) -> bool:
    rc, _ = await vm.execute(f"test -e {path}")
    return rc == 0


def nix_string(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


async def feature_drv(vm: Machine, settings: Mapping[str, str], name: str, script: str) -> tuple[str, str]:
    """A derivation only a store with the pool's feature builds: its `.drv` and its output. `${bb}` is busybox."""
    expression = (
        f'let bb = builtins.storePath "{settings["busybox"]}"; in derivation {{'
        f" name = {nix_string(name)};"
        f' system = "{settings["system"]}";'
        ' builder = "${bb}/bin/sh";'
        f' args = [ "-c" {nix_string(script)} ];'
        f' requiredSystemFeatures = [ "{settings["feature"]}" ];'
        " }"
    )
    drv = store_path(await as_tester(vm, f"nix-instantiate --expr {shlex.quote(expression)}"))
    out = (await vm.succeed(f"nix-store -q --outputs {drv}")).strip()
    return drv, out


async def until(check: Callable[[], Awaitable[bool]], timeout: float) -> None:
    """Until *check* holds, asking each second: a guest's state sends no notification."""
    with anyio.fail_after(timeout):
        while not await check():
            await anyio.sleep(1)


async def logged(vm: Machine, event: str, timeout: float) -> str:
    """The first pynixd journal line that names *event*; only warnings and errors reach the journal."""
    found = ""

    async def present() -> bool:
        nonlocal found
        rc, out = await vm.execute(f"journalctl -u pynixd -o cat | grep -m1 -F {shlex.quote(event)}")
        found = out.strip()
        return rc == 0

    await until(present, timeout)
    return found
