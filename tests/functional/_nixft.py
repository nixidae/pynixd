"""Three stores for a port of a Nix functional script that skips on `needLocalStore`.

Nix runs such a script only against a store the client opens itself, so that arm is the reference. The
others are pynixd with nix-daemon behind it, and pynixd with no daemon. Each arm keeps its store in
`<root>/store`, so a store path is the same in every arm, and in a store that an earlier step filled.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import urllib.parse
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from tests._conftest.helpers import rmtree_robust
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pynixd.nix_config import NixConfig


class Arm(StrEnum):
    local = "local"
    daemon = "daemon"
    pynixd = "pynixd"


@dataclass(frozen=True)
class ArmStore:
    url: str
    """The client's `--store`."""
    env: dict[str, str]
    config: NixConfig | dict[str, str]
    """The client's configuration: the store's own on the local arm."""


@contextlib.asynccontextmanager
async def arm_store(root: Path, arm: Arm, nix_config: NixConfig, server_lines: str = "") -> AsyncIterator[ArmStore]:
    """*arm*'s store in *root*, configured by *nix_config*: the client's own, or pynixd's and its daemon's.

    *server_lines* go in front of the servers' `NIX_CONFIG`, which `NixConfig` writes with `extra-` settings
    that add to the guest's `nix.conf` and cannot replace it.
    """
    store, state, logs = root / "store", root / "var/nix", root / "var/log/nix"
    env = {"NIX_STORE_DIR": str(store), "NIX_STATE_DIR": str(state), "NIX_LOG_DIR": str(logs)}
    if arm is Arm.local:
        yield ArmStore("local", env, nix_config)
        return
    socket = root / f"{arm}.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root / arm,
        no_probe=True,
        nix_config=nix_config,
        daemon=arm is Arm.daemon,
        op_routes={"BuildDerivation": OpRoute.daemon if arm is Arm.daemon else OpRoute.pynixd},
        store_dir=store,
        state_dir=state,
        log_dir=logs,
        extra_env={"NIX_CONFIG": f"{server_lines}\n{nix_config.to_nix_config_env()}"} if server_lines else None,
    )
    env["NIX_STATE_DIR"] = str(root / "client")
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield ArmStore(f"unix://{urllib.parse.quote(str(socket))}", env, {"substituters": ""})


def file_url(path: Path) -> str:
    """`file://` and *path*, quoted: Nix refuses a store URL with `[` in it, and pytest can put one in `tmp_path`."""
    return f"file://{urllib.parse.quote(str(path))}"


def clear_store(root: Path) -> None:
    """`clearStore` (`common/functions.sh:40`): the next store in *root* starts empty, in the same store directory."""
    for directory in ("store", "var"):
        rmtree_robust(root / directory)


def nix_tests() -> Path:
    """The guest's Nix `tests/functional`: the first of `NIX_FUNCTIONAL_TESTS` (`tests/derivations/guest`)."""
    return Path(os.environ["NIX_FUNCTIONAL_TESTS"].split(":")[0])


def copy_nix_tests(root: Path, *names: str) -> None:
    """Nix's *names* into *root*, and `config.nix` from `config.nix.in` with the guest's bash and coreutils.

    A sandboxed builder reads them from the host's store, so a build passes `--sandbox-paths /nix/store`.
    """
    source = nix_tests()
    for name in names:
        shutil.copy(source / name, root / name)
    bash, date = shutil.which("bash"), shutil.which("date")
    if bash is None or date is None:
        raise RuntimeError("the guest has no bash or no coreutils on PATH")
    config = (source / "config.nix.in").read_text()
    config = config.replace("@bash@", str(Path(bash).resolve()))
    config = config.replace("@coreutils@", str(Path(date).resolve().parent))
    config = config.replace('"@system@"', "builtins.currentSystem")
    (root / "config.nix").write_text(config)
