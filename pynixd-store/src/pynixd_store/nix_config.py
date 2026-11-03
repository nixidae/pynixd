"""The Nix settings of a local store, as Nix itself resolves them.

`nix config show` opens no store (`CmdConfigShow` is a `Command`, not a
`StoreCommand`), so this reader needs no daemon. It is not a `nix.conf`
parser: Nix resolves includes, `extra-` settings, `NIX_CONFIG` and its own
defaults, and a second implementation of that would drift.
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import TYPE_CHECKING, Any

import anyio

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .store_layout import StoreLayout


def nix_environment(layout: StoreLayout, extra_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """The environment that a `nix` command for *layout* runs in: ours, then *extra_env*, then the layout's."""
    env = os.environ.copy()
    env.update(extra_env or {})
    env.update(layout.daemon_environment())
    return env


async def read_nix_config(nix_bin: str, env: Mapping[str, str]) -> dict[str, Any]:
    """`nix config show --json` in *env*."""
    result = await anyio.run_process(
        [nix_bin, "--extra-experimental-features", "nix-command", "config", "show", "--json"],
        stdin=subprocess.DEVNULL,
        env=dict(env),
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"`nix config show` failed with code {result.returncode}: {result.stderr.decode(errors='replace')}"
        )
    config: dict[str, Any] = json.loads(result.stdout)
    return config
