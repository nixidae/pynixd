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
    from collections.abc import Iterable, Mapping, Sequence

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


async def read_nix_config_text(
    nix_bin: str, env: Mapping[str, str], arguments: Sequence[str], session: Iterable[tuple[str, str]]
) -> str:
    """`GlobalConfig::toKeyValue` (`config-global.cc:46`): `nix config show` prints the same lines.

    *arguments* are the daemon's own command line, and *session* the client's settings, which apply after them.
    """
    options = [arg for name, value in session for arg in ("--option", name, value)]
    result = await anyio.run_process(
        [nix_bin, "--extra-experimental-features", "nix-command", *arguments, *options, "config", "show"],
        stdin=subprocess.DEVNULL,
        env=dict(env),
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"`nix config show` failed with code {result.returncode}: {result.stderr.decode(errors='replace')}"
        )
    # `toKeyValue` ends each line with one line feed, and `nix config show` adds one more at the end.
    return result.stdout.decode().rstrip("\n") + "\n"


async def read_nix_version(nix_bin: str, env: Mapping[str, str]) -> str:
    """`nixVersion`: `nix --version` prints `nix (Nix) <version>` (`main.cc`)."""
    result = await anyio.run_process([nix_bin, "--version"], stdin=subprocess.DEVNULL, env=dict(env), check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"`nix --version` failed with code {result.returncode}: {result.stderr.decode(errors='replace')}"
        )
    return result.stdout.decode().split()[-1]


def nix_at_least(nix_version: str, major: int, minor: int) -> bool:
    """Whether a Nix of *nix_version* is *major*.*minor* or newer. A version that does not parse counts as a new one."""
    parts = nix_version.split(".")[:2]
    if len(parts) < 2 or not all(part.isdigit() for part in parts):
        return True
    return (int(parts[0]), int(parts[1])) >= (major, minor)
