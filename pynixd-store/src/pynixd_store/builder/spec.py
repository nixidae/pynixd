"""The job pynixd hands the sandbox helper (`child.py`), as JSON.

Its own module so the daemon names the type without importing the helper and its libseccomp.
"""

from __future__ import annotations

from typing import TypedDict


class ChildSpec(TypedDict):
    builder: str
    args: list[str]
    env: dict[str, str]
    root: str
    """The chroot root, prepared by pynixd: `etc`, `tmp`, the store directory, `/dev` links."""
    binds: list[tuple[str, str, bool]]
    """(path in the sandbox, source on the host, optional)."""
    private_network: bool
    sandbox_uid: int
    sandbox_gid: int
    work_dir: str
    shm_size: str
    no_new_privileges: bool
    filter_syscalls: bool
