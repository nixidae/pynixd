"""The job pynixd hands the sandbox helper (`child.py`), as JSON.

Its own module so the daemon names the type without importing the helper and its libseccomp.
"""

from __future__ import annotations

from typing import TypedDict


class FetchSpec(TypedDict):
    """`builtin:fetchurl`: the derivation's attributes, the file transfer settings, and what `runChild` reads
    on the host (`unix/build/derivation-builder.cc:956`)."""

    main_url: str
    mirrors: list[str]
    """The hashed-mirror URLs to try first, in order."""
    out: str
    unpack: bool
    executable: bool
    tmp_dir: str
    netrc_data: str
    netrc_file: str
    ca_data: str
    ca_file: str
    nix_version: str
    user_agent_suffix: str
    http2: bool
    http3: bool
    connect_timeout: int
    stalled_download_timeout: int
    download_speed: int
    tries: int
    retry_delay_ms: int
    retry_delay_rate_limited_ms: int
    retry_max_delay_ms: int
    retry_jitter: bool


class BuildenvSpec(TypedDict):
    """`builtin:buildenv`: its output and the derivation's own attributes, `None` for one it lacks."""

    out: str
    packages: list[tuple[str, bool, int]] | None
    """The `derivations` attribute, read: a path, whether it is active, its priority."""
    manifest: str | None


class UnpackChannelSpec(TypedDict):
    """`builtin:unpack-channel`: its output and the derivation's own attributes, `None` for one it lacks."""

    out: str
    channel_name: str | None
    src: str | None


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
    fetchurl: FetchSpec | None
    """A builtin to run in the sandbox in place of *builder*."""
    buildenv: BuildenvSpec | None
    unpack_channel: UnpackChannelSpec | None
    store_dir: str
    """The logical store directory: a shared mount in the sandbox, where the helper adds a path late."""
    usage_file: str
    """Where the helper writes what the builder used (`usage.py`), on the host."""
