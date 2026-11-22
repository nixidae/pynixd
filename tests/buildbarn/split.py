"""A compile runs on Buildbarn whose storage and scheduler are on another machine.

builder runs pynixd, the frontend and the worker; hub runs the storage and
the scheduler, reached over TCP with mutual TLS. A build on builder uses all
three remote endpoints: the frontend's storage and scheduler, and the
worker's scheduler. Then builder drops its client certificate, and the
same kind of build fails: the hub refuses a client its CA did not sign.
"""

from __future__ import annotations

from remote import build, counts
from vivarium_runner import Machines


async def test(vms: Machines) -> None:
    hub, builder = vms.hub, vms.builder
    await hub.wait_for_unit("buildbarn-storage.service")
    await hub.wait_for_unit("buildbarn-scheduler.service")
    await builder.wait_for_unit("pynixd.service")
    await builder.wait_for_unit("buildbarn-frontend.service")
    await builder.wait_for_unit("buildbarn-worker.service")

    rc, out, err = await build(builder, vms.settings, "split")
    if rc != 0:
        raise AssertionError(f"the build failed:\n{err}")
    first = await counts(builder, out.strip())
    print(f"[test] split build: local, remote, cache = {first}")
    if first != (0, 1, 0):
        raise AssertionError(f"the compile did not run on Buildbarn: {first}")

    await builder.switch_to("anonymous")
    rc, _, err = await build(builder, vms.settings, "anonymous")
    if rc == 0:
        raise AssertionError("a build succeeded with no client certificate: the hub admits anyone")
    print(f"[test] with no client certificate, the build fails:\n{err[-2000:]}")
