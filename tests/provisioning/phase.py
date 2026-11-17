"""pynixd launches a builder for a build no store can take, and ends it once idle. nixidae/nixidae#1198.

A builder with a wrong token goes first, as the negative control: it dials
the same acceptor and is refused. Then a build that needs the pool's feature
waits for a store, the provisioner launches one machine, its builder joins
by the ticket in its user data, builds, and the machine ends after the
pool's idle time.
"""

from __future__ import annotations

import json
import time

from cloud_helpers import call, feature_drv, logged, pool_instances, rows, until
from daemon_helpers import as_tester
from vivarium_runner import Machines


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

    drv, out = await feature_drv(controller, settings, "provisioned", "echo provisioned > $out")
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

    # The provisioner writes the end after the provider answers the terminate.
    async def row_ended() -> bool:
        return all(row["state"] == "ended" for row in await rows(controller))

    await until(row_ended, 30)
    [row] = await rows(controller)
    assert (row["state"], row["endReason"], row["storeId"]) == ("ended", "idle", f"provisioned-{row['id']}"), row
    print(f"[test] {launched['id']} ended {time.monotonic() - idle_from:.0f} s after the build: {row}")
    assert [(one["id"], one["state"]) for one in await pool_instances(controller)] == [(launched["id"], "terminated")]
