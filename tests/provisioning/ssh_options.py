"""An `ssh-ng://` client names a pool on its SSH command. nixidae/nixidae#1198.

Nix sends no `SetOptions` over ssh-ng (`ssh-store.cc:92-99`), so `--option builders` cannot steer such a
client. It chooses the command: `remote-program` is split on whitespace, then `--stdio` follows
(`ssh-store.cc:227-233`). pynixd's SSH server reads `--option pynixd-pool big` there. The same URL with an
untrusted key is the control: the setting is refused with Nix's warning, and the build fails as unsteered.
"""

from __future__ import annotations

import json
import shlex
import time

from cloud_helpers import feature_drv, feature_expression, pool_instances
from vivarium_runner import Machines

URL = "ssh-ng://root@127.0.0.1?remote-program=nix-daemon%20--option%20pynixd-pool%20big%20--option%20max-jobs%200"
KEYS = "/etc/pynixd-ssh"


def _ssh(key: str) -> str:
    return f"NIX_SSHOPTS='-p 2234 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /root/{key}'"


async def test(vms: Machines) -> None:
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    settings = vms.settings
    await controller.succeed(
        f"mkdir -p {KEYS} && for k in admin user; do ssh-keygen -q -t ed25519 -N '' -f /root/$k; done"
        f" && cp /root/admin.pub {KEYS}/admin && cp /root/user.pub {KEYS}/users"
    )
    before = {one["id"] for one in await pool_instances(controller)}

    untrusted, untrusted_out = await feature_drv(
        controller, settings, "ssh-untrusted", "echo u > $out", settings["spare"]
    )
    done = await controller.execute(f"{_ssh('user')} timeout 60 nix-store --store '{URL}' --realise {untrusted} 2>&1")
    rc, out = done.returncode, done.stdout
    assert rc not in (0, 124), f"an untrusted ssh-ng build ended with {rc}: {out}"
    assert "'pynixd-pool', because it is a restricted setting" in out, out
    assert "missing system features" in out, out
    assert {one["id"] for one in await pool_instances(controller)} == before, "an untrusted key launched one"
    print("[test] an untrusted key that names the pool on its SSH command launches nothing")

    steered, steered_out = await feature_drv(controller, settings, "ssh-steered", "echo s > $out", settings["spare"])
    started = time.monotonic()
    out = (
        await controller.succeed(f"{_ssh('admin')} nix-store --store '{URL}' --realise {steered} 2>&1", timeout=600)
    ).stdout
    assert "ignoring the setting 'max-jobs' on the SSH command" in out, out
    [launched] = [one for one in await pool_instances(controller) if one["id"] not in before]
    builder = await cloud.wait_running(launched["id"])
    await builder.succeed(f"nix-store --check-validity {steered_out}")
    rc = (await builder.execute(f"nix-store --check-validity {untrusted_out}")).returncode
    assert rc != 0, "the untrusted build ran on the builder"
    print(f"[test] the ssh-ng build launched {launched['id']} and built there in {time.monotonic() - started:.0f} s")

    await cloud.wait_ended(launched["id"], timeout=180)
    await _set_options_by_name(vms, before | {launched["id"]})


async def _set_options_by_name(vms: Machines, before: set[str]) -> None:
    """huggorm's Nix over a plain `ssh-ng://` URL: `SetOptions` carries the settings (`set-options-by-name`).

    The client's `max-jobs 0` arrives too, and pynixd drops it: the plain build runs on the controller. Taken, it
    would leave the build no machine.
    """
    cloud, controller = vms.cloud, vms.controller
    assert cloud is not None
    settings = vms.settings
    pynix = f"{_ssh('admin')} pynix build --store ssh-ng://root@127.0.0.1 --option max-jobs 0"

    await controller.succeed(
        f"echo {shlex.quote(feature_expression(settings, 'by-name-local', 'echo l > $out', ''))} > /root/local.nix"
    )
    out = (await controller.succeed(f"{pynix} --file /root/local.nix 2>&1", timeout=120)).stdout
    assert {one["id"] for one in await pool_instances(controller)} == before, f"a plain build launched one: {out}"
    print("[test] huggorm's max-jobs 0 over ssh-ng left the controller building")

    # `ssh-ng://localhost` runs `remote-program` with no ssh (`fakeSSH`, `ssh.cc:82`): a stock daemon names no
    # `set-options-by-name`, so it gets no `SetOptions`, and the client's max-jobs 0 cannot stop it.
    stock = f"ssh-ng://localhost?remote-program={settings['stockDaemon']}"
    await controller.succeed(
        f"echo {shlex.quote(feature_expression(settings, 'by-name-stock', 'echo s > $out', ''))} > /root/stock.nix"
    )
    await controller.succeed(f"pynix build --store '{stock}' --option max-jobs 0 --file /root/stock.nix", timeout=120)
    print("[test] a stock daemon over ssh-ng got no SetOptions from huggorm's Nix")

    expression = feature_expression(settings, "by-name-steered", "echo n > $out", settings["spare"])
    await controller.succeed(f"echo {shlex.quote(expression)} > /root/steered.nix")
    started = time.monotonic()
    built = await controller.succeed(
        # The second value wins, as for a known setting; the first names no pool and would fail the build.
        f"{pynix} --option pynixd-pool nowhere --option pynixd-pool big --file /root/steered.nix 2>/root/steered.err",
        timeout=600,
    )
    out = built.stdout
    path = json.loads(out)["outputs"]["out"]
    [launched] = [one for one in await pool_instances(controller) if one["id"] not in before]
    builder = await cloud.wait_running(launched["id"])
    await builder.succeed(f"nix-store --check-validity {path}")
    print(
        f"[test] SetOptions over ssh-ng launched {launched['id']} and built there in {time.monotonic() - started:.0f} s"
    )
    await cloud.wait_ended(launched["id"], timeout=180)
