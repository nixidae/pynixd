"""`supplementary-groups.sh`'s three builds on one arm, run under `unshare --mount --map-root-user` (`execUnshare`).

The client, pynixd and its daemon all run as root in a user namespace whose `setgroups` the kernel denies.
`python -m tests.functional._unshared_groups ROOT ARM OUT` writes one `{"rc", "stderr"}` per build to OUT as
JSON; stdout carries the log.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import anyio

from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, arm_store, copy_nix_tests

# (store, `require-drop-supplementary-groups`): `supplementary-groups.sh:26`, `:30`, `:35`.
CASES = (("store1", None), ("store2", "true"), ("store3", "false"))


def busybox() -> str:
    """The guest's busybox, as `sandbox-paths` names it for `/bin/sh`."""
    out = subprocess.run([NIX_BIN, "config", "show", "sandbox-paths"], capture_output=True, text=True, check=True)
    for entry in out.stdout.split():
        if entry.startswith("/bin/sh="):
            return entry.removeprefix("/bin/sh=")
    raise RuntimeError(f"no /bin/sh in sandbox-paths: {out.stdout!r}")


async def run(root: Path, arm: Arm, shell: str) -> list[dict[str, object]]:
    src = root / "src"
    src.mkdir()
    copy_nix_tests(src, "hermetic.nix")
    config = for_test_store(substituters=())
    config.sandbox = "true"
    results: list[dict[str, object]] = []
    for name, require in CASES:
        # Root with a `build-users-group` builds as users the namespace does not map; the suite sets none.
        lines = "build-users-group =" + ("" if require is None else f"\nrequire-drop-supplementary-groups = {require}")
        async with arm_store(root / name, arm, config, server_lines=lines) as store:
            rc, _out, err, _both = await run_subproc(
                [NIX_BIN.with_name("nix-build"), "--store", store.url, src / "hermetic.nix", "--arg", "busybox",
                 shell, "--arg", "seed", "1", "--no-out-link"],
                expected_retcode=None,
                env=dict(store.env) | {"NIX_CONFIG": lines},
                nix_config=store.config,
            )  # fmt: skip
        results.append({"rc": rc, "stderr": err})
    return results


def main() -> None:
    root, arm, out = Path(sys.argv[1]), Arm(sys.argv[2]), Path(sys.argv[3])
    out.write_text(json.dumps(anyio.run(run, root, arm, busybox())))


if __name__ == "__main__":
    main()
