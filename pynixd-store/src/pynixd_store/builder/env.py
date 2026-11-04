"""The builder's environment: `initEnv` (`unix/build/derivation-builder.cc:692`) over `DesugaredEnv`."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..utils import nix32_encode

if TYPE_CHECKING:
    from collections.abc import Mapping

SANDBOX_BUILD_DIR = "/build"
"""`sandbox-build-dir`'s default: the build directory as the builder sees it."""

HOME_DIR = "/homeless-shelter"
"""`DerivationBuilderImpl::homeDir`; it must not exist."""


def hash_placeholder(output_name: str) -> str:
    """`hashPlaceholder` (`derivations.cc`): what `builtins.placeholder` returns."""
    return "/" + nix32_encode(hashlib.sha256(f"nix-output:{output_name}".encode()).digest())


def rewrite_strings(text: str, rewrites: Mapping[str, str]) -> str:
    """`rewriteStrings`: each pair in key order, every occurrence."""
    for old in sorted(rewrites):
        text = text.replace(old, rewrites[old])
    return text


@dataclass(frozen=True)
class BuildEnv:
    variables: dict[str, str]
    files: dict[str, str] = field(default_factory=dict)
    """Files written into the build directory before the builder starts, by name."""


def build_env(
    env: Mapping[str, str],
    *,
    store_dir: str,
    build_cores: int,
    rewrites: Mapping[str, str],
    fixed_output: bool,
    impure_source: Mapping[str, str],
) -> BuildEnv | None:
    """The environment the builder runs with, or `None` where pynixd leaves the build to the daemon.

    `None`: structured attrs (`prepareStructuredAttrs`) and `exportReferencesGraph`,
    which need the input closure's validity registration.
    """
    if "__json" in env or "exportReferencesGraph" in env:
        return None
    pass_as_file = set(env.get("passAsFile", "").split())

    variables = {
        "PATH": "/path-not-set",
        "HOME": HOME_DIR,
        "NIX_STORE": store_dir,
        "NIX_BUILD_CORES": str(build_cores),
    }
    files: dict[str, str] = {}
    for name, value in env.items():
        if name in pass_as_file:
            file_name = ".attr-" + nix32_encode(hashlib.sha256(name.encode()).digest())
            files[file_name] = rewrite_strings(value, rewrites)
            variables[name + "Path"] = f"{SANDBOX_BUILD_DIR}/{file_name}"
        else:
            variables[name] = value

    variables["NIX_BUILD_TOP"] = SANDBOX_BUILD_DIR
    for name in ("TMPDIR", "TEMPDIR", "TMP", "TEMP", "PWD"):
        variables[name] = SANDBOX_BUILD_DIR
    if fixed_output:
        variables["NIX_OUTPUT_CHECKED"] = "1"
        for name in env.get("impureEnvVars", "").split():
            variables[name] = impure_source.get(name, "")
    variables["NIX_LOG_FD"] = "2"
    variables["TERM"] = "xterm-256color"
    # `rewriteStrings` over `name=value`: a placeholder holds no `=`, so each half rewrites alone.
    return BuildEnv({rewrite_strings(k, rewrites): rewrite_strings(v, rewrites) for k, v in variables.items()}, files)
