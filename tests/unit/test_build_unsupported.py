"""`pynixd_store.builder.local.unsupported` leaves to the daemon what pynixd's sandbox cannot build as Nix does."""

from __future__ import annotations

import pytest

from nix_daemon_protocol.basic_derivation import BasicDerivation
from nix_daemon_protocol.derivation_output import DerivationOutput
from nix_daemon_protocol.store_path import StorePath
from pynixd_store.builder.local import _builder_refusal, unsupported, uses_sandbox
from pynixd_store.builder.settings import BuildSettings

FIXED = DerivationOutput(path="/nix/store/00000000000000000000000000000000-x", method="r:sha256", hash_digest="00")
IMPURE = DerivationOutput(path="", method="r:sha256", hash_digest="impure")
PLAIN = DerivationOutput(path="/nix/store/00000000000000000000000000000000-x")


def _drv(output: DerivationOutput, *, no_chroot: bool = False) -> BasicDerivation:
    env = {"__noChroot": "1"} if no_chroot else {}
    return BasicDerivation(outputs={"out": output}, platform="x86_64-linux", builder="/bin/sh", env=env)


def _settings(sandbox: str) -> BuildSettings:
    return BuildSettings.from_nix_config({}, {"system": "x86_64-linux", "sandbox": sandbox})


def _reason(output: DerivationOutput, sandbox: str, *, relocated: bool, no_chroot: bool = False) -> str | None:
    return unsupported(
        _drv(output, no_chroot=no_chroot), _settings(sandbox), store_dir="/nix/store", relocated=relocated
    )


@pytest.mark.parametrize(
    ("output", "sandbox", "no_chroot", "relocated", "sandboxed"),
    [
        (PLAIN, "true", False, False, True),
        (FIXED, "true", False, False, True),
        (PLAIN, "false", False, False, False),
        (PLAIN, "false", False, True, True),
        (PLAIN, "relaxed", False, False, True),
        (PLAIN, "relaxed", True, False, False),
        (PLAIN, "relaxed", True, True, True),
        (FIXED, "relaxed", False, False, False),
        (IMPURE, "relaxed", False, False, False),
        (FIXED, "relaxed", False, True, True),
    ],
)
def test_the_sandbox_is_where_nix_puts_it(
    output: DerivationOutput, sandbox: str, no_chroot: bool, relocated: bool, sandboxed: bool
) -> None:
    """`makeDerivationBuilder` (`derivation-builder.cc:1751`): `isRelocatedStore` forces the sandbox (`:1782`)."""
    assert uses_sandbox(_drv(output, no_chroot=no_chroot), _settings(sandbox), relocated=relocated) is sandboxed
    assert _reason(output, sandbox, relocated=relocated, no_chroot=no_chroot) is None


def test_no_chroot_with_sandbox_true_is_refused_in_nix_words() -> None:
    """`makeDerivationBuilder` (`derivation-builder.cc:1760`) throws; pynixd's build refuses it the same way."""
    assert _reason(PLAIN, "true", relocated=True, no_chroot=True) is None
    drv_path = StorePath("/nix/store/00000000000000000000000000000000-x.drv")
    refused = _builder_refusal(drv_path, _drv(PLAIN, no_chroot=True), _settings("true"), chroot=True)
    assert refused is not None
    assert "has '__noChroot' set, but that's not allowed when 'sandbox' is 'true'" in refused


@pytest.mark.parametrize(
    ("config", "reason"),
    [
        ({}, "auto-allocate-uids without the experimental feature"),
        ({"experimental-features": "auto-allocate-uids"}, None),
        ({"experimental-features": "auto-allocate-uids", "id-count": "65537"}, "start-id or id-count"),
        ({"experimental-features": "auto-allocate-uids", "start-id": "0"}, "start-id or id-count"),
    ],
)
def test_auto_allocate_uids_is_pynixds_where_nix_allocates(
    monkeypatch: pytest.MonkeyPatch, config: dict[str, str], reason: str | None
) -> None:
    """`AutoUserLock::acquire` requires the feature and asserts on the ids (`user-lock.cc:176`)."""
    monkeypatch.setattr("os.geteuid", lambda: 0)
    entries = {name: {"value": value} for name, value in {"auto-allocate-uids": True, **config}.items()}
    settings = BuildSettings.from_nix_config(entries, {"system": "x86_64-linux"})
    got = unsupported(_drv(PLAIN), settings, store_dir="/nix/store", relocated=False)
    assert got is None if reason is None else reason in (got or "")


@pytest.mark.parametrize(
    ("config", "sandbox", "reason"),
    [
        ({"experimental-features": "auto-allocate-uids cgroups"}, "true", None),
        # `makeDerivationBuilder` (`derivation-builder.cc:1797`) refuses it; pynixd's build does the same.
        ({"experimental-features": "auto-allocate-uids cgroups"}, "false", None),
        ({"auto-allocate-uids": False}, "true", "uid-range without auto-allocate-uids as root"),
        ({"experimental-features": "auto-allocate-uids"}, "true", "uid-range without the experimental feature cgroups"),
    ],
)
def test_uid_range_is_pynixds_where_nix_builds_it(
    monkeypatch: pytest.MonkeyPatch, config: dict[str, object], sandbox: str, reason: str | None
) -> None:
    """`makeDerivationBuilder` (`derivation-builder.cc:1810`), `setupBuildChroot` (`chroot.cc:52`) and
    `prepareBuild` (`linux-derivation-builder.cc:450`) refuse the rest."""
    monkeypatch.setattr("os.geteuid", lambda: 0)
    entries = {name: {"value": value} for name, value in {"auto-allocate-uids": True, **config}.items()}
    settings = BuildSettings.from_nix_config(entries, {"system": "x86_64-linux", "sandbox": sandbox})
    drv = BasicDerivation(
        outputs={"out": PLAIN}, platform="x86_64-linux", builder="/bin/sh", env={"requiredSystemFeatures": "uid-range"}
    )
    assert unsupported(drv, settings, store_dir="/nix/store", relocated=False) == reason


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


@pytest.mark.parametrize(("setting", "value"), [("post-build-hook", "/bin/hook"), ("keep-failed", "true")])
def test_a_post_build_hook_and_keep_failed_stay_in_pynixd(setting: str, value: str) -> None:
    """pynixd runs the hook (`runPostBuildHook`, `derivation-building-goal.cc:1105`) and keeps a failed build's
    directory itself (`cleanupBuild`, `derivation-builder.cc:1697`)."""
    drv = BasicDerivation(outputs={"out": PLAIN}, platform="x86_64-linux", builder="/bin/sh")
    settings = BuildSettings.from_nix_config({}, {"system": "x86_64-linux", setting: value})
    assert unsupported(drv, settings, store_dir="/nix/store", relocated=True) is None
