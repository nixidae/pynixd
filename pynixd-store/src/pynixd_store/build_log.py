"""The build log a client stores: Nix's `LocalStore::addBuildLog` (`local-store.cc:1645`).

The function blocks on the file system and on bzip2, so the engine calls it
on a worker thread, as it calls `roots`.
"""

from __future__ import annotations

import bz2
import os
import random
from pathlib import Path

from .errors import PynixdError

DRVS_LOG_DIR = "drvs"
"""`LocalFSStore::drvsLogDir` (`local-fs-store.cc:179`): the subdirectory under `logDir`."""


class BuildLogError(PynixdError):
    """The base name is not a derivation, as `addBuildLog` asserts."""


def log_path(log_dir: Path, drv_base_name: str) -> Path:
    """`<logDir>/drvs/<name[:2]>/<name[2:]>.bz2` (`local-store.cc:1652`).

    *drv_base_name* is `StorePath::to_string`, the base name the wire carries.
    """
    return log_dir / DRVS_LOG_DIR / drv_base_name[:2] / f"{drv_base_name[2:]}.bz2"


def add_build_log(log_dir: Path, drv_base_name: str, log: bytes) -> None:
    """Record one build log, as `LocalStore::addBuildLog` does.

    A log already present is kept: Nix returns before it writes. The write is
    a temporary file and a rename, so a reader never sees a half-written log.
    """
    if not drv_base_name.endswith(".drv"):
        raise BuildLogError(f"not a derivation: '{drv_base_name}'")
    destination = log_path(log_dir, drv_base_name)
    if destination.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    # `.tmp-<pid>-<random>`, not Nix's `.tmp.<pid>`: one pynixd process serves
    # many sessions, so the pid alone does not keep two concurrent adds apart.
    temporary = destination.with_name(f"{destination.name}.tmp-{os.getpid()}-{random.getrandbits(31)}")
    temporary.write_bytes(bz2.compress(log))
    os.rename(temporary, destination)
