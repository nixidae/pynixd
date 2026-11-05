"""`pynixd_store.builder.local.unsupported` leaves to the daemon what pynixd's sandbox cannot build as Nix does."""

from __future__ import annotations

import pytest

from nix_daemon_protocol.basic_derivation import BasicDerivation
from nix_daemon_protocol.derivation_output import DerivationOutput
from pynixd_store.builder.local import unsupported
from pynixd_store.builder.settings import BuildSettings

FIXED = DerivationOutput(path="/nix/store/00000000000000000000000000000000-x", method="r:sha256", hash_digest="00")
IMPURE = DerivationOutput(path="", method="r:sha256", hash_digest="impure")
PLAIN = DerivationOutput(path="/nix/store/00000000000000000000000000000000-x")


def _reason(output: DerivationOutput, sandbox: str, *, relocated: bool) -> str | None:
    drv = BasicDerivation(outputs={"out": output}, platform="x86_64-linux", builder="/bin/sh")
    settings = BuildSettings.from_nix_config({}, {"system": "x86_64-linux", "sandbox": sandbox})
    return unsupported(drv, settings, store_dir="/nix/store", relocated=relocated)


@pytest.mark.parametrize("output", [FIXED, IMPURE])
def test_relaxed_leaves_an_unsandboxed_derivation_to_the_daemon(output: DerivationOutput) -> None:
    """`makeDerivationBuilder` (`derivation-builder.cc:1777`) builds it without the chroot."""
    assert _reason(output, "relaxed", relocated=False) == "an unsandboxed derivation with sandbox = relaxed"


@pytest.mark.parametrize("output", [FIXED, IMPURE])
def test_relaxed_in_a_relocated_store_still_sandboxes(output: DerivationOutput) -> None:
    """`isRelocatedStore` forces the sandbox (`derivation-builder.cc:1782`)."""
    assert _reason(output, "relaxed", relocated=True) is None


def test_a_git_hashed_output_needs_git_hashing() -> None:
    """`dumpBlobPrefix` requires `Xp::GitHashing` (`libutil/git.cc:233`); the daemon answers without it."""
    drv = BasicDerivation(
        outputs={"out": DerivationOutput(path="", method="git:sha256")}, platform="x86_64-linux", builder="/bin/sh"
    )
    off = BuildSettings.from_nix_config({}, {"system": "x86_64-linux"})
    on = BuildSettings.from_nix_config({}, {"system": "x86_64-linux", "experimental-features": "git-hashing"})
    assert unsupported(drv, off, store_dir="/nix/store", relocated=True) == "a Git-hashed output without git-hashing"
    assert unsupported(drv, on, store_dir="/nix/store", relocated=True) is None


def test_a_sandboxed_type_or_sandbox_true_stays_in_pynixd() -> None:
    assert _reason(PLAIN, "relaxed", relocated=False) is None
    assert _reason(FIXED, "true", relocated=False) is None


@pytest.mark.parametrize(
    ("setting", "value", "reason"),
    [
        ("post-build-hook", "/bin/hook", "a build with a post-build-hook"),
        ("keep-failed", "true", "a build with keep-failed"),
    ],
)
def test_a_hook_or_a_kept_build_directory_goes_to_the_daemon(setting: str, value: str, reason: str) -> None:
    """`runPostBuildHook` (`derivation-building-goal.cc:1079`) and `cleanupBuild` (`derivation-builder.cc:1697`)."""
    drv = BasicDerivation(outputs={"out": PLAIN}, platform="x86_64-linux", builder="/bin/sh")
    settings = BuildSettings.from_nix_config({}, {"system": "x86_64-linux", setting: value})
    assert unsupported(drv, settings, store_dir="/nix/store", relocated=True) == reason
