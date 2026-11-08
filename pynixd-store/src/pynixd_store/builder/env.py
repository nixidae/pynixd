"""The builder's environment: `initEnv` (`unix/build/derivation-builder.cc:692`) over `DesugaredEnv`."""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.store_path import StorePath

from ..signing import nar_hash_for_a_fingerprint
from ..utils import nix32_encode

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from nix_daemon_protocol.valid_path_info import ValidPathInfo

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

_C_INT_RANGE = range(-(2**31), 2**31)
_U64 = 2**64 - 1

# nlohmann's `kCachedPowers` (`detail/conversions/to_chars.hpp`): 10^k as f * 2^e, for k from -300 in steps of 8.
_CACHED_POWERS = (
    (0xAB70FE17C79AC6CA, -1060), (0xFF77B1FCBEBCDC4F, -1034), (0xBE5691EF416BD60C, -1007),
    (0x8DD01FAD907FFC3C, -980), (0xD3515C2831559A83, -954), (0x9D71AC8FADA6C9B5, -927),
    (0xEA9C227723EE8BCB, -901), (0xAECC49914078536D, -874), (0x823C12795DB6CE57, -847),
    (0xC21094364DFB5637, -821), (0x9096EA6F3848984F, -794), (0xD77485CB25823AC7, -768),
    (0xA086CFCD97BF97F4, -741), (0xEF340A98172AACE5, -715), (0xB23867FB2A35B28E, -688),
    (0x84C8D4DFD2C63F3B, -661), (0xC5DD44271AD3CDBA, -635), (0x936B9FCEBB25C996, -608),
    (0xDBAC6C247D62A584, -582), (0xA3AB66580D5FDAF6, -555), (0xF3E2F893DEC3F126, -529),
    (0xB5B5ADA8AAFF80B8, -502), (0x87625F056C7C4A8B, -475), (0xC9BCFF6034C13053, -449),
    (0x964E858C91BA2655, -422), (0xDFF9772470297EBD, -396), (0xA6DFBD9FB8E5B88F, -369),
    (0xF8A95FCF88747D94, -343), (0xB94470938FA89BCF, -316), (0x8A08F0F8BF0F156B, -289),
    (0xCDB02555653131B6, -263), (0x993FE2C6D07B7FAC, -236), (0xE45C10C42A2B3B06, -210),
    (0xAA242499697392D3, -183), (0xFD87B5F28300CA0E, -157), (0xBCE5086492111AEB, -130),
    (0x8CBCCC096F5088CC, -103), (0xD1B71758E219652C, -77), (0x9C40000000000000, -50),
    (0xE8D4A51000000000, -24), (0xAD78EBC5AC620000, 3), (0x813F3978F8940984, 30),
    (0xC097CE7BC90715B3, 56), (0x8F7E32CE7BEA5C70, 83), (0xD5D238A4ABE98068, 109),
    (0x9F4F2726179A2245, 136), (0xED63A231D4C4FB27, 162), (0xB0DE65388CC8ADA8, 189),
    (0x83C7088E1AAB65DB, 216), (0xC45D1DF942711D9A, 242), (0x924D692CA61BE758, 269),
    (0xDA01EE641A708DEA, 295), (0xA26DA3999AEF774A, 322), (0xF209787BB47D6B85, 348),
    (0xB454E4A179DD1877, 375), (0x865B86925B9BC5C2, 402), (0xC83553C5C8965D3D, 428),
    (0x952AB45CFA97A0B3, 455), (0xDE469FBD99A05FE3, 481), (0xA59BC234DB398C25, 508),
    (0xF6C69A72A3989F5C, 534), (0xB7DCBF5354E9BECE, 561), (0x88FCF317F22241E2, 588),
    (0xCC20CE9BD35C78A5, 614), (0x98165AF37B2153DF, 641), (0xE2A0B5DC971F303A, 667),
    (0xA8D9D1535CE3B396, 694), (0xFB9B7CD9A4A7443C, 720), (0xBB764C4CA7A44410, 747),
    (0x8BAB8EEFB6409C1A, 774), (0xD01FEF10A657842C, 800), (0x9B10A4E5E9913129, 827),
    (0xE7109BFBA19C0C9D, 853), (0xAC2820D9623BF429, 880), (0x80444B5E7AA7CF85, 907),
    (0xBF21E44003ACDD2D, 933), (0x8E679C2F5E44FF8F, 960), (0xD433179D9C8CB841, 986),
    (0x9E19DB92B4E31BA9, 1013),
)  # fmt: skip

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
_STORE_PATH_BASE = re.compile(r"[0-9a-df-np-sv-z]{32}-([A-Za-z0-9+\-._?=]{1,211})")
_EXPORT_FILE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*")
_TOKEN_SEPARATORS = re.compile(r"[ \t\n\r]+")
"""`tokenizeString`'s default separators; `str.split` splits on more."""


def is_store_path(word: str, store_dir: str) -> bool:
    """`StoreDirConfig::isStorePath`: in the store directory itself, with a valid base name."""
    parent, _, base = word.rpartition("/")
    match = _STORE_PATH_BASE.fullmatch(base)
    if parent != store_dir or match is None:
        return False
    name = match.group(1)
    return name not in (".", "..") and not name.startswith((".-", "..-"))


def _refuse_number(text: str) -> Any:
    raise _Unreproduced(f"the number {text}")


def _float(text: str) -> float:
    """A double, unless `writeShell` would cast it to `int` out of range, which C++ leaves undefined."""
    value = float(text)
    if not math.isfinite(value):
        raise _Unreproduced(f"the number {text}")
    try:
        single = struct.unpack("f", struct.pack("f", value))[0]
    except OverflowError:
        raise _Unreproduced(f"the number {text}") from None
    if math.ceil(single) == single and math.trunc(value) not in _C_INT_RANGE:
        raise _Unreproduced(f"the number {text}")
    return value


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
    """`StructuredAttrs::parse`, refusing what pynixd would not dump as nlohmann does."""
    try:
        attrs = json.loads(raw, parse_float=_float, parse_int=_integer, parse_constant=_refuse_number)
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
    if any(name in attrs and not isinstance(attrs[name], bool) for name in _BOOLS):
        return "a structured attribute that must be a Boolean"
    if "__sandboxProfile" in attrs and not isinstance(attrs["__sandboxProfile"], str):
        return "a structured attribute that must be a string"
    for name in _STRING_SETS:
        if name in attrs and not (isinstance(attrs[name], list) and all(isinstance(word, str) for word in attrs[name])):
            return "a structured attribute that must be a list of strings"
    return None


def _flatten(value: Any) -> Iterable[str]:
    """`flatten` (`derivation-options.cc:106`)."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from _flatten(item)
    else:
        raise _Unreproduced("a value that is not an array or a string")


def export_requests(env: Mapping[str, str], store_dir: str) -> dict[str, list[StorePath]]:
    """`DerivationOptions::exportReferencesGraph`: each name and its store paths, in Nix's `std::map` order.

    `ValueError` names what pynixd leaves to the daemon: what Nix refuses or
    warns about, and a derived path, which a `BasicDerivation` cannot resolve.
    """
    requests: dict[str, set[str]] = {}
    if "__json" in env:
        graph = parse_structured_attrs(env["__json"]).get("exportReferencesGraph")
        if graph is None:
            return {}
        if not isinstance(graph, dict):
            raise _Unreproduced("a value that is not an object")
        for key, value in graph.items():
            requests[key] = set(_flatten(value))
    else:
        words = [word for word in _TOKEN_SEPARATORS.split(env.get("exportReferencesGraph", "")) if word]
        if len(words) % 2:
            raise _Unreproduced("an odd number of words")
        for name, path in zip(words[::2], words[1::2], strict=True):
            if not _EXPORT_FILE_NAME.fullmatch(name):
                raise _Unreproduced(f"the file name {name!r}")
            requests[name] = {path}
    for paths in requests.values():
        if bad := sorted(path for path in paths if not is_store_path(path, store_dir)):
            raise _Unreproduced(f"the path {bad[0]!r}")
    return {name: sorted(StorePath(path) for path in paths) for name, paths in sorted(requests.items())}


def validity_registration(infos: Sequence[ValidPathInfo]) -> str:
    """`Store::makeValidityRegistration` without derivers or hashes (`store-api.cc:804`)."""
    lines: list[str] = []
    for info in infos:
        references = sorted(ref.path for ref in info.info.references)
        lines += [info.path.path, "", str(len(references)), *references]
    return "".join(f"{line}\n" for line in lines)


def path_info_json(infos: Sequence[ValidPathInfo]) -> list[dict[str, Any]]:
    """`pathInfoToJSON` (`parsed-derivations.cc`); *infos* is a closure, so each `closureSize` sums within it."""
    by_path = {info.path.path: info for info in infos}
    entries: list[dict[str, Any]] = []
    for info in infos:
        closure = {info.path.path}
        pending = [info.path.path]
        while pending:
            for ref in by_path[pending.pop()].info.references:
                if ref.path not in closure:
                    closure.add(ref.path)
                    pending.append(ref.path)
        entry: dict[str, Any] = {
            "narHash": nar_hash_for_a_fingerprint(info.info.nar_hash),
            "narSize": info.info.nar_size,
            "references": sorted(ref.path for ref in info.info.references),
            "path": info.path.path,
            "valid": True,
            "closureSize": sum(by_path[path].info.nar_size for path in closure),
        }
        if info.info.ca:
            entry["ca"] = str(info.info.ca)
        entries.append(entry)
    return entries


def _multiply(x: tuple[int, int], y: tuple[int, int]) -> tuple[int, int]:
    """`diyfp::mul`: the upper 64 bits of the product, rounded half up after the lowest 32 bits are dropped."""
    return (((x[0] * y[0]) >> 32) + (1 << 31)) >> 32, x[1] + y[1] + 64


def _normalize(f: int, e: int) -> tuple[int, int]:
    shift = 64 - f.bit_length()
    return f << shift, e - shift


def _grisu2_round(digits: list[int], dist: int, delta: int, rest: int, ten_k: int) -> None:
    """`grisu2_round`, in its unsigned 64-bit arithmetic."""
    while (
        rest < dist
        and (delta - rest) & _U64 >= ten_k
        and ((rest + ten_k) & _U64 < dist or (dist - rest) & _U64 > (rest + ten_k - dist) & _U64)
    ):
        digits[-1] -= 1
        rest = (rest + ten_k) & _U64


def _grisu2(value: float) -> tuple[list[int], int]:
    """nlohmann's `dtoa_impl::grisu2` for a positive finite double: its digits and decimal exponent.

    Not always the shortest form Python's `repr` gives: measured, 259 of 200,012 random doubles differ.
    """
    bits = struct.unpack("<Q", struct.pack("<d", value))[0]
    big_e, big_f = bits >> 52, bits & ((1 << 52) - 1)
    vf, ve = (big_f, -1074) if big_e == 0 else (big_f + (1 << 52), big_e - 1075)
    m_plus = _normalize(2 * vf + 1, ve - 1)
    minus_f, minus_e = (4 * vf - 1, ve - 2) if big_f == 0 and big_e > 1 else (2 * vf - 1, ve - 1)
    m_minus = (minus_f << (minus_e - m_plus[1]), m_plus[1])
    v = _normalize(vf, ve)

    f = -60 - m_plus[1] - 1
    k = (f * 78913) // (1 << 18) + (f > 0) if f >= 0 else -((-f * 78913) // (1 << 18))
    cached_f, cached_e = _CACHED_POWERS[(300 + k + 7) // 8]
    cached_k = -300 + 8 * ((300 + k + 7) // 8)
    w = _multiply(v, (cached_f, cached_e))
    w_minus = _multiply(m_minus, (cached_f, cached_e))
    w_plus = _multiply(m_plus, (cached_f, cached_e))
    big_minus, big_plus = w_minus[0] + 1, w_plus[0] - 1
    e = w_plus[1]
    decimal_exponent = -cached_k

    delta, dist = big_plus - big_minus, big_plus - w[0]
    one = 1 << -e
    p1, p2 = big_plus >> -e, big_plus & (one - 1)
    n = len(str(p1))
    pow10 = 10 ** (n - 1)
    digits: list[int] = []
    while n > 0:
        digits.append(p1 // pow10)
        p1 %= pow10
        n -= 1
        rest = (p1 << -e) + p2
        if rest <= delta:
            _grisu2_round(digits, dist, delta, rest, (pow10 << -e) & _U64)
            return digits, decimal_exponent + n
        pow10 //= 10
    m = 0
    while True:
        p2 = (p2 * 10) & _U64
        digits.append(p2 >> -e)
        p2 &= one - 1
        m += 1
        delta, dist = (delta * 10) & _U64, (dist * 10) & _U64
        if p2 <= delta:
            break
    _grisu2_round(digits, dist, delta, p2, one)
    return digits, decimal_exponent - m


def dump_float(value: float) -> str:
    """nlohmann's `to_chars`: Grisu2 digits, fixed notation for an exponent from -4 to 15, else `1e+15`."""
    if math.copysign(1.0, value) < 0:
        return "-" + dump_float(-value)
    if value == 0:
        return "0.0"
    raw, decimal_exponent = _grisu2(value)
    digits = "".join(map(str, raw))
    k, n = len(digits), len(digits) + decimal_exponent
    if k <= n <= 15:
        return digits + "0" * (n - k) + ".0"
    if 0 < n <= 15:
        return f"{digits[:n]}.{digits[n:]}"
    if -4 < n <= 0:
        return "0." + "0" * -n + digits
    mantissa = digits if k == 1 else f"{digits[0]}.{digits[1:]}"
    return f"{mantissa}e{'-' if n - 1 < 0 else '+'}{abs(n - 1):02d}"


def dump_json(value: Any) -> str:
    """nlohmann's `dump()`: keys sorted (`std::map`), no spaces, UTF-8 left unescaped."""
    if isinstance(value, float):
        return dump_float(value)
    if isinstance(value, dict):
        return "{" + ",".join(f"{dump_json(k)}:{dump_json(value[k])}" for k in sorted(value)) + "}"
    if isinstance(value, list):
        return "[" + ",".join(dump_json(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=False)


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
    if isinstance(value, float):
        # `get<float>()` is single precision; a whole one declares `get<int>()` of the double, truncated.
        single = struct.unpack("f", struct.pack("f", value))[0]
        return str(math.trunc(value)) if math.ceil(single) == single else None
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
    sandboxed: bool,
    impure_source: Mapping[str, str],
    exported: Mapping[str, Sequence[ValidPathInfo]],
    build_dir: str = SANDBOX_BUILD_DIR,
) -> BuildEnv:
    """The environment the builder runs with. A structured build must pass `structured_attrs_reason` first.

    *fixed_output* and *sandboxed* are `isFixed` and `isSandboxed` of the derivation's type: an impure
    derivation is neither. *exported* is `exportReferences` of each `export_requests` name. *build_dir* is
    `tmpDirInSandbox`: the host's build directory for a build without the chroot.
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
        for key, infos in exported.items():
            attrs[key] = path_info_json(infos)
        files[".attrs.sh"] = rewrite_strings(write_shell(attrs), rewrites)
        files[".attrs.json"] = rewrite_strings(dump_json(attrs), rewrites)
        variables["NIX_ATTRS_SH_FILE"] = f"{build_dir}/.attrs.sh"
        variables["NIX_ATTRS_JSON_FILE"] = f"{build_dir}/.attrs.json"
        impure = list(attrs.get("impureEnvVars", ()))
    else:
        pass_as_file = set(env.get("passAsFile", "").split())
        for name, value in env.items():
            if name in pass_as_file:
                file_name = ".attr-" + nix32_encode(hashlib.sha256(name.encode()).digest())
                files[file_name] = rewrite_strings(value, rewrites)
                variables[name + "Path"] = f"{build_dir}/{file_name}"
            else:
                variables[name] = value
        for file_name, infos in exported.items():
            files[file_name] = rewrite_strings(validity_registration(infos), rewrites)
        impure = env.get("impureEnvVars", "").split()

    variables["NIX_BUILD_TOP"] = build_dir
    for name in ("TMPDIR", "TEMPDIR", "TMP", "TEMP", "PWD"):
        variables[name] = build_dir
    if fixed_output:
        variables["NIX_OUTPUT_CHECKED"] = "1"
    if not sandboxed:
        for name in impure:
            variables[name] = impure_source.get(name, "")
    variables["NIX_LOG_FD"] = "2"
    variables["TERM"] = "xterm-256color"
    # `rewriteStrings` over `name=value`: a placeholder holds no `=`, so each half rewrites alone.
    return BuildEnv({rewrite_strings(k, rewrites): rewrite_strings(v, rewrites) for k, v in variables.items()}, files)
