"""pynixd launches a builder for a build no store can take, and ends it once idle. nixidae/nixidae#1198.

A builder with a wrong token goes first, as the negative control: it dials
the same acceptor and is refused. Then a build that needs the pool's feature
waits for a store, the provisioner launches one machine, its builder joins
by the ticket in its user data, builds, and the machine ends after the
pool's idle time.
"""

from __future__ import annotations

import json
import shlex
import time
from collections.abc import Mapping
from typing import Any

import anyio
from daemon_helpers import as_tester, store_path
from vivarium_runner import Machine, Machines

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


def nix_string(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


async def feature_drv(vm: Machine, settings: Mapping[str, str]) -> tuple[str, str]:
    """A derivation only a store with the pool's feature builds: its `.drv` and its output."""
    expression = (
        f'let bb = builtins.storePath "{settings["busybox"]}"; in derivation {{'
        ' name = "provisioned";'
        f' system = "{settings["system"]}";'
        ' builder = "${bb}/bin/sh";'
        f' args = [ "-c" {nix_string("echo provisioned > $out")} ];'
        f' requiredSystemFeatures = [ "{settings["feature"]}" ];'
        " }"
    )
    drv = store_path(await as_tester(vm, f"nix-instantiate --expr {shlex.quote(expression)}"))
    out = (await vm.succeed(f"nix-store -q --outputs {drv}")).strip()
    return drv, out


async def logged(vm: Machine, event: str, timeout: float) -> str:
    """The first pynixd journal line that names *event*. The journal has no wait API; this reads it each second."""
    with anyio.fail_after(timeout):
        while True:
            rc, out = await vm.execute(f"journalctl -u pynixd -o cat | grep -m1 -F {shlex.quote(event)}")
            if rc == 0:
                return out.strip()
            await anyio.sleep(1)


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    settings = vms.settings
    await controller.wait_for_unit("pynixd.service")

    ticket = {"token": "not-a-token", "acceptor_host": "10.9.0.1", "acceptor_port": 2235}
    status, rogue = await call(
        controller, "POST", "/v1/instances", {"template": "builder", "user_data": json.dumps(ticket)}
    )
    assert status == 202, rogue
    await cloud.wait_running(rogue["id"])
    refused = await logged(controller, "reverse_join_refused", timeout=180)
    print(f"[test] {rogue['id']} with a wrong token is refused: {refused}")
    await cloud.terminate(rogue["id"])
    assert await pool_instances(controller) == [], "nothing waits, so nothing launches"

    drv, out = await feature_drv(controller, settings)
    started = time.monotonic()
    await as_tester(controller, f"nix-store --realise {drv}", timeout=600)
    built = (await controller.succeed(f"cat {out}")).strip()
    print(f"[test] {out} built in {time.monotonic() - started:.0f} s: {built}")

    [launched] = await pool_instances(controller)
    assert built == "provisioned", built
    # The sandbox names every build's host `localhost`; the builder's store says where it ran.
    builder = await cloud.wait_running(launched["id"])
    await builder.succeed(f"nix-store --check-validity {out}")
    print(f"[test] {launched['id']} was launched for it, and holds the output")

    idle_from = time.monotonic()
    await cloud.wait_ended(launched["id"], timeout=180)
    rows = await controller.succeed(f"sqlite3 -json {DB} 'SELECT id, state, storeId, endReason FROM {TABLE}'")
    [row] = json.loads(rows)
    assert (row["state"], row["endReason"], row["storeId"]) == ("ended", "idle", f"provisioned-{row['id']}"), row
    print(f"[test] {launched['id']} ended {time.monotonic() - idle_from:.0f} s after the build: {row}")
    assert [(one["id"], one["state"]) for one in await pool_instances(controller)] == [(launched["id"], "terminated")]
