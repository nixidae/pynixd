"""A user's build on each server, and the two answers compared.

The same derivation on both, instantiated there by the user. `daemon`
serves it through pynixd: `prepare` proved pynixd holds the socket, and
`bypass` proves the requests need it.
"""

from __future__ import annotations

import json
from typing import TypedDict

from daemon_helpers import SERVERS, PathInfo, as_tester, differences, instantiate, path_info, store_path
from vivarium_runner import Machines


class Answer(TypedDict):
    drv: str
    out: str
    content: str
    info: PathInfo


async def test(vms: Machines) -> None:
    answers: dict[str, Answer] = {}
    for name in SERVERS:
        vm = vms[name]
        drv = await instantiate(vm, vms.settings, "local")
        out = store_path(await as_tester(vm, f"nix-store --realise {drv}"))
        content = (await vm.succeed(f"cat {out}")).strip()
        answers[name] = {"drv": drv, "out": out, "content": content, "info": await path_info(vm, out)}
        print(f"[test] {name}: built {out} ({content})")

    (vms.artifacts / "daemon" / "local.json").write_text(json.dumps(answers, indent=2) + "\n")
    daemon, control = answers["daemon"], answers["control"]
    diff = differences(daemon["info"], control["info"])
    if (daemon["drv"], daemon["out"]) != (control["drv"], control["out"]) or diff:
        raise AssertionError(f"pynixd and nix-daemon answered differently: {diff}")
    print("[test] both instantiated and built the same paths, and path-info agrees")
