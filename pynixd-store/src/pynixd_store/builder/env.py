"""The builder's environment: `initEnv` (`unix/build/derivation-builder.cc:692`) over `DesugaredEnv`."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from ..utils import nix32_encode

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

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


class _Unreproduced(ValueError):
    pass


_INT_RANGE = range(-(2**63), 2**64)
"""What nlohmann keeps as an integer; a larger one becomes a double."""

_NOT_YET = ("outputChecks", "unsafeDiscardReferences", "exportReferencesGraph")
# `derivationOptionsFromStructuredAttrs` warns about each (`derivation-options.cc:186`).
_WARNED = (
    "allowedReferences",
    "allowedRequisites",
    "disallowedRequisites",
    "disallowedReferences",
    "maxSize",
    "maxClosureSize",
)
_BOOLS = ("__noChroot", "preferLocalBuild", "allowSubstitutes", "__darwinAllowLocalNetworking")
_STRING_SETS = ("__impureHostDeps", "impureEnvVars", "requiredSystemFeatures")
_SH_VAR_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _refuse_number(text: str) -> Any:
    raise _Unreproduced(f"the number {text}")


def _integer(text: str) -> int:
    value = int(text)
    if value not in _INT_RANGE:
        raise _Unreproduced(f"the number {text}")
    return value


def _check_text(value: Any) -> None:
    """A lone surrogate parses in Python, and nlohmann refuses it."""
    if isinstance(value, str):
        value.encode("utf-8")
    elif isinstance(value, list):
        for item in value:
            _check_text(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            key.encode("utf-8")
            _check_text(item)


def parse_structured_attrs(raw: str) -> dict[str, Any]:
    """`StructuredAttrs::parse`, refusing what pynixd would not dump as nlohmann does.

    A float is refused: nlohmann prints 1e15 as `1e+15` and Python as `1000000000000000.0`.
    """
    try:
        attrs = json.loads(raw, parse_float=_refuse_number, parse_int=_integer, parse_constant=_refuse_number)
        _check_text(attrs)
    except UnicodeEncodeError as e:
        raise _Unreproduced("a lone surrogate") from e
    if not isinstance(attrs, dict):
        raise _Unreproduced("a value that is not an object")
    return attrs


def structured_attrs_reason(env: Mapping[str, str]) -> str | None:
    """Why pynixd leaves a `__structuredAttrs` build to the daemon, or `None`."""
    try:
        attrs = parse_structured_attrs(env["__json"])
    except ValueError as e:
        return f"__json with {e}"
    if "passAsFile" in env or any(name in attrs for name in _WARNED):
        return "an attribute structured attrs ignore, which Nix warns about"
    if found := [name for name in _NOT_YET if name in attrs]:
        return f"structured attrs with {found}"
    if any(name in attrs and not isinstance(attrs[name], bool) for name in _BOOLS):
        return "a structured attribute that must be a Boolean"
    if "__sandboxProfile" in attrs and not isinstance(attrs["__sandboxProfile"], str):
        return "a structured attribute that must be a string"
    for name in _STRING_SETS:
        if name in attrs and not (isinstance(attrs[name], list) and all(isinstance(word, str) for word in attrs[name])):
            return "a structured attribute that must be a list of strings"
    return None


def dump_json(value: Any) -> str:
    """nlohmann's `dump()`: keys sorted (`std::map`), no spaces, UTF-8 left unescaped."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _shell_quote(text: str) -> str:
    """`escapeShellArgAlways` (`util.cc:216`)."""
    return "'" + text.replace("'", "'\\''") + "'"


def _shell_simple(value: Any) -> str | None:
    if isinstance(value, str):
        return _shell_quote(value)
    if isinstance(value, bool):
        return "1" if value else ""
    if isinstance(value, int):
        # `get<int>()` of a 64-bit value: the low 32 bits, signed.
        return str((value + 2**31) % 2**32 - 2**31)
    if value is None:
        return "''"
    return None


def write_shell(attrs: Mapping[str, Any]) -> str:
    """`StructuredAttrs::writeShell` (`parsed-derivations.cc`): each value Bash can declare."""
    lines: list[str] = []
    for key in sorted(attrs):
        if not _SH_VAR_NAME.fullmatch(key):
            continue
        value = attrs[key]
        if (simple := _shell_simple(value)) is not None:
            lines.append(f"declare {key}={simple}\n")
        elif isinstance(value, list):
            items = [_shell_simple(item) for item in value]
            if all(item is not None for item in items):
                lines.append(f"declare -a {key}=({''.join(f'{item} ' for item in items)})\n")
        elif isinstance(value, dict):
            pairs = {k: _shell_simple(v) for k, v in sorted(value.items())}
            if all(item is not None for item in pairs.values()):
                lines.append(f"declare -A {key}=({''.join(f'[{_shell_quote(k)}]={v} ' for k, v in pairs.items())})\n")
    return "".join(lines)


def build_env(
    env: Mapping[str, str],
    *,
    outputs: Iterable[str],
    store_dir: str,
    build_cores: int,
    rewrites: Mapping[str, str],
    fixed_output: bool,
    impure_source: Mapping[str, str],
) -> BuildEnv | None:
    """The environment the builder runs with, or `None` where pynixd leaves the build to the daemon.

    `None`: `exportReferencesGraph`, which needs the input closure's validity
    registration. A structured build must pass `structured_attrs_reason` first.
    """
    variables = {
        "PATH": "/path-not-set",
        "HOME": HOME_DIR,
        "NIX_STORE": store_dir,
        "NIX_BUILD_CORES": str(build_cores),
    }
    files: dict[str, str] = {}
    impure: list[str]
    if "__json" in env:
        # `DesugaredEnv::create`: the JSON and its shell form, and nothing of `drv.env`.
        attrs = parse_structured_attrs(env["__json"])
        attrs["outputs"] = {name: hash_placeholder(name) for name in outputs}
        files[".attrs.sh"] = rewrite_strings(write_shell(attrs), rewrites)
        files[".attrs.json"] = rewrite_strings(dump_json(attrs), rewrites)
        variables["NIX_ATTRS_SH_FILE"] = f"{SANDBOX_BUILD_DIR}/.attrs.sh"
        variables["NIX_ATTRS_JSON_FILE"] = f"{SANDBOX_BUILD_DIR}/.attrs.json"
        impure = list(attrs.get("impureEnvVars", ()))
    elif "exportReferencesGraph" in env:
        return None
    else:
        pass_as_file = set(env.get("passAsFile", "").split())
        for name, value in env.items():
            if name in pass_as_file:
                file_name = ".attr-" + nix32_encode(hashlib.sha256(name.encode()).digest())
                files[file_name] = rewrite_strings(value, rewrites)
                variables[name + "Path"] = f"{SANDBOX_BUILD_DIR}/{file_name}"
            else:
                variables[name] = value
        impure = env.get("impureEnvVars", "").split()

    variables["NIX_BUILD_TOP"] = SANDBOX_BUILD_DIR
    for name in ("TMPDIR", "TEMPDIR", "TMP", "TEMP", "PWD"):
        variables[name] = SANDBOX_BUILD_DIR
    if fixed_output:
        variables["NIX_OUTPUT_CHECKED"] = "1"
        for name in impure:
            variables[name] = impure_source.get(name, "")
    variables["NIX_LOG_FD"] = "2"
    variables["TERM"] = "xterm-256color"
    # `rewriteStrings` over `name=value`: a placeholder holds no `=`, so each half rewrites alone.
    return BuildEnv({rewrite_strings(k, rewrites): rewrite_strings(v, rewrites) for k, v in variables.items()}, files)
