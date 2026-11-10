"""The job pynixd hands the sandbox helper (`child.py`), as JSON.

Its own module so the daemon names the type without importing the helper and its libseccomp.
"""

from __future__ import annotations

from typing import TypedDict


class FetchSpec(TypedDict):
    """`builtin:fetchurl`: the derivation's attributes, the file transfer settings, and what `runChild` reads
    on the host (`unix/build/derivation-builder.cc:956`)."""

    fixed_or_impure: bool
    """`isFixed() || isImpure()`: `builtinFetchurl` refuses any other derivation (`builtins/fetchurl.cc:28`)."""
    main_url: str | None
    """`None` for a derivation with no `url`: `env.at("url")` throws then (`builtins/fetchurl.cc:32`)."""
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
    chroot: bool
    """`False` runs the builder on the host, with no namespace and no chroot (`LinuxDerivationBuilder`)."""
    root: str
    """With *chroot*, the chroot root, prepared by pynixd: `etc`, `tmp`, the store directory, `/dev` links."""
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
    cgroup: str | None
    """The cgroup the helper joins before anything else (`cgroup.py`), or `None`."""
    cgroup_report: bool
    """The helper reads *cgroup* into *usage_file*: a VM's cgroup is out of the host's reach."""
    sample_seconds: float
    """With *cgroup_report*, how often the helper samples the cgroup; 0 takes no samples."""
    build_user: tuple[int, int] | None
    """The build user's host uid and gid, as root; `None` maps the helper's own ids."""
    build_groups: list[int]
    """Without *chroot*, the build user's supplementary groups, which the builder keeps."""
    map_fd: int | None
    """With *build_user*: the helper writes a byte here after its `unshare`, and reads one back once pynixd has
    written its uid and gid maps. A process in the new namespace cannot map a uid other than its own."""
    id_range: bool
    """A build user of 65536 ids (`getUIDCount() != 1`): the builder is root and gets `/sys` (`:841`), and its
    *cgroup* is the root of a cgroup namespace of its own (`linux-derivation-builder.cc:899`)."""
