"""A build Nix refuses fails in Nix's words on a store with no daemon.

Nix refuses each of these before the builder runs, or the builder fails at
once. pynixd refuses it the same way, so a store with no daemon needs none
for it. The daemon route, writes included, is the reference.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from pynixd.op_routes import OpRoute
from tests.conftest import NIX_BIN, run_subproc
from tests.functional.test_local_build_parity import _errors, _serve

if TYPE_CHECKING:
    from pathlib import Path


def _drv(name: str, attrs: str = "") -> str:
    return (
        f'derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; '
        f'args = [ "-c" "echo > $out" ]; {attrs} }}'
    )


CASES: dict[str, tuple[str, dict[str, Any], tuple[str, ...]]] = {
    # `makeDerivationBuilder` (`derivation-builder.cc:1760`).
    "no-chroot": (_drv("refused-no-chroot", "__noChroot = true;"), {"sandbox": "true"}, ()),
    # `makeDerivationBuilder` (`derivation-builder.cc:1797`).
    "uid-range-unsandboxed": (
        _drv("refused-uid-range", 'requiredSystemFeatures = [ "uid-range" ];'),
        {"sandbox": "false", "system_features": ["uid-range"]},
        (),
    ),
    # `setupBuildChroot` (`chroot.cc:52`): no build user with 65536 ids.
    "uid-range-no-users": (
        _drv("refused-uid-range-users", 'requiredSystemFeatures = [ "uid-range" ];'),
        {"sandbox": "true", "system_features": ["uid-range"]},
        (),
    ),
    # `startDaemon` (`derivation-builder.cc:787`).
    "recursive-nix": (
        _drv("refused-recursive", 'requiredSystemFeatures = [ "recursive-nix" ];'),
        {"system_features": ["recursive-nix"], "experimental_features": ["nix-command"]},
        (),
    ),
    # `runChild` finds no such entry in `builtinBuilders` (`derivation-builder.cc:1007`).
    "unknown-builtin": (
        'derivation { name = "refused-builtin"; system = builtins.currentSystem; builder = "builtin:no-such"; }',
        {},
        (),
    ),
    # The store reads the derivation without the feature (`derivations.cc:325`).
    "floating-without-ca": (
        _drv("refused-floating", 'outputHashMode = "recursive"; outputHashAlgo = "sha256"; __contentAddressed = true;'),
        {"experimental_features": ["nix-command"]},
        ("--extra-experimental-features", "ca-derivations"),
    ),
    # `derivationOptionsFromStructuredAttrs` (`derivation-options.cc:334`, `:339`) and `parseSingleDerivedPath`.
    "export-odd": (_drv("refused-export-odd", 'exportReferencesGraph = "a";'), {}, ()),
    "export-name": (_drv("refused-export-name", 'exportReferencesGraph = [ "1bad" "/nix/store/x" ];'), {}, ()),
    "export-path": (_drv("refused-export-path", 'exportReferencesGraph = [ "a" "/tmp" ];'), {}, ()),
    # `DerivationGoal::haveDerivation` (`derivation-goal.cc:113`): a check needs valid outputs.
    "check-unbuilt": (_drv("refused-check"), {}, ("--rebuild",)),
    "check-floating-unbuilt": (
        _drv(
            "refused-check-floating",
            'outputHashMode = "recursive"; outputHashAlgo = "sha256"; __contentAddressed = true;',
        ),
        {"experimental_features": ["nix-command", "ca-derivations"]},
        ("--rebuild", "--extra-experimental-features", "ca-derivations"),
    ),
    "impure-without-feature": (
        _drv("refused-impure", "__impure = true;"),
        {"experimental_features": ["nix-command"]},
        ("--extra-experimental-features", "impure-derivations ca-derivations"),
    ),
}


BUILDS: dict[str, tuple[str, dict[str, Any], tuple[str, ...]]] = {
    # A repair of a floating output with no realisation builds it (`derivation-goal.cc:71`).
    "repair-floating-unbuilt": (
        _drv(
            "built-repair-floating",
            'outputHashMode = "recursive"; outputHashAlgo = "sha256"; __contentAddressed = true;',
        ),
        {"experimental_features": ["nix-command", "ca-derivations"]},
        ("--repair", "--extra-experimental-features", "ca-derivations"),
    ),
}
"""Builds Nix runs, and a store with no daemon must run too."""

_OPS = ("BuildDerivation", "AddToStore", "AddToStoreNar", "AddMultipleToStore")


async def _refused(tmp_path: Path, route: OpRoute, case: str) -> tuple[int, list[str]]:
    expr, settings, flags = CASES[case] if case in CASES else BUILDS[case]
    spec_args: dict[str, Any] = {"daemon": False} if route is OpRoute.pynixd else {}
    serve = tmp_path / case
    serve.mkdir(exist_ok=True)
    # `NIX_CONFIG` holds `extra-experimental-features`; only a file can take a feature of the guest's away.
    # The writes take the route too: the daemon registers the `.drv` of the reference.
    async with _serve(serve, route, settings=settings, config_file=True, ops=_OPS, **spec_args) as (url, _root):
        rc, _out, err, _both = await run_subproc(
            [NIX_BIN, "build", "--store", url, "--impure", "--no-link", *flags, "--expr", expr],
            expected_retcode=None,
        )
    return rc, _errors(err) if "error:" in err else [err]


@pytest.mark.parametrize("case", sorted(CASES))
async def test_a_build_nix_refuses_fails_in_nix_words_with_no_daemon(tmp_path: Path, case: str) -> None:
    there = await _refused(tmp_path, OpRoute.daemon, case)
    here = await _refused(tmp_path, OpRoute.pynixd, case)

    assert there[0] != 0, there
    assert here == there
    if case in RAW_DAEMON:
        # Both arms ask pynixd's QueryMissing, so the reference is what a raw nix-daemon 2.35.3 printed.
        lines = here[1]
        assert (lines[0], lines[1].split("'")[0], lines[-1]) == (
            "error:",
            "       … while parsing derivation ",
            f"       error: {RAW_DAEMON[case]}",
        ), lines


@pytest.mark.parametrize("case", sorted(BUILDS))
async def test_a_build_nix_runs_builds_with_no_daemon(tmp_path: Path, case: str) -> None:
    there = await _refused(tmp_path, OpRoute.daemon, case)
    here = await _refused(tmp_path, OpRoute.pynixd, case)

    assert there[0] == 0, there
    assert here == there


RAW_DAEMON = {
    "export-odd": "odd number of tokens in 'exportReferencesGraph': 'a'",
    "export-name": "invalid file name '1bad' in 'exportReferencesGraph'",
    "export-path": "path '/tmp' is not in the Nix store",
}
