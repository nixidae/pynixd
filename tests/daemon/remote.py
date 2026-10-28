"""The client uses each server over OpenSSH, the two ways people do.

`--store ssh-ng://` builds in the far store. `--builders ssh-ng://` builds
there and copies the output back. On the far side `ssh-ng://` runs
`nix-daemon --stdio`, which talks to the daemon socket -- pynixd on
`daemon`, nix-daemon on `control`.
"""

from __future__ import annotations

import json

from daemon_helpers import SERVERS, PathInfo, as_tester, differences, instantiate, path_info, store_path
from vivarium_runner import Machines


def far(name: str) -> str:
    return f"ssh-ng://tester@{name}"


async def test(vms: Machines) -> None:
    client = vms.client
    # In the client's store. `--eval-store auto` below is what copies a
    # derivation to the far store before building it there.
    store_drv = await instantiate(client, vms.settings, "store")
    builder_drv = await instantiate(client, vms.settings, "builder")
    answers: dict[str, dict[str, PathInfo]] = {"store": {}, "builder": {}}

    for name in SERVERS:
        out = store_path(
            await as_tester(
                client,
                f"nix build --no-link --print-out-paths --eval-store auto --store {far(name)} '{store_drv}^out'",
            )
        )
        answers["store"][name] = await path_info(client, out, store=far(name))
        print(f"[test] {name}: built {out} in its own store for the client")

        out = store_path(
            await as_tester(
                client,
                f"nix build --no-link --print-out-paths --max-jobs 0"
                f" --builders '{far(name)} x86_64-linux' '{builder_drv}^out'",
            )
        )
        answers["builder"][name] = await path_info(client, out)
        print(f"[test] {name}: built {out} as the client's builder")
        # Gone again, so the next server's build is a build and not a hit.
        await as_tester(client, f"nix store delete {out}")

    (vms.artifacts / "client" / "remote.json").write_text(json.dumps(answers, indent=2) + "\n")
    diffs = {kind: differences(each["daemon"], each["control"]) for kind, each in answers.items()}
    if any(diffs.values()):
        raise AssertionError(f"pynixd and nix-daemon answered differently: {diffs}")
    print("[test] the far store and the builder agree on both servers")
