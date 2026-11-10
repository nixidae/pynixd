from __future__ import annotations

from pathlib import Path

from pynixd.nix_config import NixConfig

HOST_DAEMON_SOCKET = Path("/nix/var/nix/daemon-socket/socket")
"""The machine's own Nix daemon, which not every machine has."""

HOST_DAEMON_SUBSTITUTER = f"unix://{HOST_DAEMON_SOCKET}?root=/"


def host_daemon_substituters() -> tuple[str, ...]:
    """The host's daemon as a substituter, when the host has one.

    **Named unconditionally, this is a substituter that cannot answer.** A
    multi-user NixOS machine has that socket and a GitHub runner does not, so
    every build in the test suite reported

        error: cannot connect to socket at
        '/nix/var/nix/daemon-socket/socket': No such file or directory

    in CI and nowhere else. It was the first line of the build log of a
    capability probe that then produced no output path, which is how it
    became visible at all. pynixd issue #47.

    Read at call time rather than at import: a test that starts a daemon of
    its own is not required to leave the machine's own one alone.
    """
    return (HOST_DAEMON_SUBSTITUTER,) if HOST_DAEMON_SOCKET.exists() else ()


DEFAULT_SUBSTITUTERS = host_daemon_substituters()
"""No network cache: a test guest has no network, and each query to one waits out its timeout. nixidae/nixidae#1119."""
DEFAULT_TRUSTED_PUBLIC_KEYS: tuple[str, ...] = ()


def for_test_store(
    *,
    substituters: tuple[str, ...] = DEFAULT_SUBSTITUTERS,
    trusted_public_keys: tuple[str, ...] = DEFAULT_TRUSTED_PUBLIC_KEYS,
    require_sigs: bool = False,
    experimental_features: tuple[str, ...] = (),
) -> NixConfig:
    return NixConfig(
        substituters=list(substituters),
        trusted_public_keys=list(trusted_public_keys),
        require_sigs=require_sigs,
        experimental_features=list(experimental_features) or None,
        # A test store is thrown away, so it needs no durability. With /tmp
        # on disk, the fsyncs made `test_delegated_build_runs_on_the_builder`
        # take 55.7 s against 30.8 s without them (`IsValidPath` x692: 13.3 s
        # against 2.4 s). nixidae/nixidae#1258.
        fsync_metadata=False,
    )


def for_dynamic_derivations(
    *,
    substituters: tuple[str, ...] = (),
    trusted_public_keys: tuple[str, ...] = (),
    require_sigs: bool = False,
) -> NixConfig:
    return NixConfig(
        substituters=list(substituters) or None,
        trusted_public_keys=list(trusted_public_keys) or None,
        require_sigs=require_sigs,
        experimental_features=["ca-derivations", "dynamic-derivations"],
        fsync_metadata=False,
    )


def for_ca_derivations(
    *,
    substituters: tuple[str, ...] = (),
    trusted_public_keys: tuple[str, ...] = (),
    require_sigs: bool = False,
) -> NixConfig:
    return NixConfig(
        substituters=list(substituters) or None,
        trusted_public_keys=list(trusted_public_keys) or None,
        require_sigs=require_sigs,
        experimental_features=["ca-derivations"],
        fsync_metadata=False,
    )
