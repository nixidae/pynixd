"""`registerOutputs` (`unix/build/derivation-builder.cc:1069`) for input-addressed and content-addressed outputs.

The builder wrote each output inside the chroot at its scratch path: the path
it will have when the derivation names one, else `fallback_path`. Only a
content-addressed output whose path differs from its scratch path has hashes
rewritten.
"""

from __future__ import annotations

import base64
import os
import re
import stat
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from nix_daemon_protocol.build_result import BuildResultStatus
from nix_daemon_protocol.content_address import ContentAddress as WireContentAddress
from nix_daemon_protocol.derivation_output import OutputKind
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from ..add import (
    AddError,
    InvalidOwnership,
    PathLock,
    TempDir,
    _nar_hash,
    canonicalise,
    content_hash,
    delete_path,
    git_hash,
)
from ..content_address import (
    ContentAddress,
    Method,
    content_addressed_path,
    fixed_output_address,
    make_store_path,
    output_method,
    parse_any_sha256,
)
from ..hintfmt import magenta
from ..nar_fs import dump_path, restore_path
from ..utils import nix32_encode
from .env import is_store_path, parse_structured_attrs
from .refscan import scan_for_references

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping
    from pathlib import Path

    from nix_daemon_protocol.derivation_output import DerivationOutput


@dataclass
class BuildFailure(Exception):
    """A `BuildError`: the status the `BuildResult` carries, and Nix's message."""

    status: BuildResultStatus
    message: str

    def __str__(self) -> str:
        return self.message


class PlainError(Exception):
    """A plain `Error` out of a build, which `Store::buildDerivation` answers as `MISC_FAILURE` with its
    rendered text (`entry-points.cc:89`)."""


@dataclass(frozen=True)
class Scanned:
    path: str
    references: frozenset[str]
    mode: int


def _base(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def _quoted(path: Path) -> str:
    """`PathFmt`: a path in double quotes."""
    return magenta(f'"{path}"')


def _hash_part(path: str) -> str:
    return _base(path)[:32]


def _output_path_name(drv_name: str, output: str) -> str:
    """`outputPathName`."""
    return drv_name if output == "out" else f"{drv_name}-{output}"


def fallback_path(drv_path: str, output: str, drv_name: str, store_dir: str) -> str:
    """`makeFallbackPath(outputName)` (`derivation-builder.cc:1708`): where an output with no known path builds."""
    kind = f"rewrite:{_base(drv_path)}:name:{output}"
    return make_store_path(kind, "sha256", bytes(32), _output_path_name(drv_name, output), store_dir)


def random_path(output: str, drv_name: str, store_dir: str) -> str:
    """`StorePath::random(outputPathName(...))`: the known path of an impure output (`derivation-building-goal.cc:383`)."""
    return f"{store_dir}/{nix32_encode(os.urandom(20))}-{_output_path_name(drv_name, output)}"


def _sri(algo: str, digest: bytes) -> str:
    return f"{algo}-{base64.b64encode(digest).decode()}"


def _scan(
    drv_path: str,
    name: str,
    path: str,
    actual: Path,
    referenceable: set[str],
    *,
    discard: bool,
    build_uid: int | None = None,
    seen: set[tuple[int, int]] | None = None,
) -> Scanned:
    try:
        st = os.lstat(actual)
    except FileNotFoundError:
        raise BuildFailure(
            BuildResultStatus.OUTPUT_REJECTED,
            f"builder for '{magenta(drv_path)}' failed to produce output path for output "
            f"'{magenta(name)}' at {_quoted(actual)}",
        ) from None
    # `registerOutputs` (`derivation-builder.cc:1153`): an output the build user does not own is suspicious too.
    if (not stat.S_ISLNK(st.st_mode) and st.st_mode & (stat.S_IWGRP | stat.S_IWOTH)) or (
        build_uid is not None and st.st_uid != build_uid
    ):
        raise BuildFailure(
            BuildResultStatus.OUTPUT_REJECTED,
            f"suspicious ownership or permission on {_quoted(actual)} for output "
            f"'{magenta(name)}'; rejecting this build output",
        )
    try:
        canonicalise(actual, uid_range=None if build_uid is None else (build_uid, build_uid), seen=seen)
    except InvalidOwnership as e:
        raise BuildFailure(
            BuildResultStatus.OUTPUT_REJECTED, f"invalid ownership on file {_quoted(Path(e.path))}"
        ) from None
    references = frozenset() if discard else frozenset(scan_for_references(actual, referenceable))
    return Scanned(path, references, st.st_mode)


def _sorted(drv_path: str, scanned: Mapping[str, Scanned]) -> list[str]:
    """`topoSort` over the references between outputs, dependencies first."""
    by_path = {s.path: name for name, s in scanned.items()}
    done: list[str] = []
    state: dict[str, bool] = {}

    def visit(name: str, parent: str) -> None:
        if state.get(name) is False:
            raise BuildFailure(
                BuildResultStatus.OUTPUT_REJECTED,
                f"cycle detected in build of '{magenta(drv_path)}' in the references of output "
                f"'{magenta(name)}' from output '{magenta(parent)}'",
            )
        if name in state:
            return
        state[name] = False
        for ref in sorted(scanned[name].references):
            other = by_path.get(ref)
            if other is not None and other != name:
                visit(other, name)
        state[name] = True
        done.append(name)

    for name in sorted(scanned):
        visit(name, name)
    return done


@dataclass
class Collected:
    """What `registerOutputs` holds before `checkOutputs`: each output in the store and described.

    *early* are the content-addressed outputs, which Nix registers before it
    checks them, so a fixed output with the wrong hash stays valid under its
    real one. *locks* guard paths that are not the derivation's own: hold them
    until *early* is registered, then `release`.
    """

    infos: dict[str, ValidPathInfo]
    early: list[ValidPathInfo]
    failure: BuildFailure | None
    locks: list[PathLock]

    def release(self) -> None:
        for lock in self.locks:
            lock.release(delete=True)


@dataclass(frozen=True)
class _Fixed:
    wanted: ContentAddress
    got: ContentAddress
    path: str
    references: frozenset[str]


@dataclass(frozen=True)
class _Addressed:
    got: ContentAddress
    path: str
    references: frozenset[str]


def collect_outputs(
    drv_path: str,
    outputs: Mapping[str, DerivationOutput],
    *,
    scratch: Mapping[str, str],
    drv_name: str,
    chroot_root: Path,
    real_store_dir: Path,
    store_dir: str,
    input_paths: set[str],
    valid: set[str],
    is_valid: Callable[[str], bool],
    discard: set[str],
    check: bool = False,
    build_uid: int | None = None,
) -> Collected:
    """Each output the build made at its *scratch* path, moved into the store and described; nothing is registered yet.

    An output whose scratch path is in *valid* is not looked at, as Nix does outside check mode.
    With *check*, every output stays in the chroot to be compared (`derivation-builder.cc:1530`).
    *is_valid* asks the store about a path no output names. An output in
    *discard* is not scanned (`unsafeDiscardReferences`). Blocking: call it
    from a worker thread. *build_uid* is the build user's, whose files alone an output may hold.
    """
    referenceable = input_paths | set(scratch.values())
    # One for every output, as Nix's: an output may hard-link a file of another (`derivation-builder.cc:1076`).
    seen: set[tuple[int, int]] = set()
    scanned = {
        name: _scan(
            drv_path,
            name,
            path,
            chroot_root / path.lstrip("/"),
            referenceable,
            discard=name in discard,
            build_uid=build_uid,
            seen=seen,
        )
        for name, path in scratch.items()
        if path not in valid
    }
    infos: dict[str, ValidPathInfo] = {}
    fixed: dict[str, _Fixed] = {}
    addressed: dict[str, ContentAddress] = {}
    # `outputRewrites`: the final path of each output done so far, by its scratch path.
    final: dict[str, str] = {}
    locks: list[PathLock] = []
    try:
        for name in _sorted(drv_path, scanned):
            output, s = outputs[name], scanned[name]
            actual = chroot_root / s.path.lstrip("/")
            path, references = s.path, s.references
            if output.method:
                a = _content_addressed(
                    name, drv_name, output, s, actual, store_dir, real_store_dir, final, discard=name in discard
                )
                path, references, addressed[name] = a.path, a.references, a.got
                if output.kind is OutputKind.CA_FIXED:
                    wanted = fixed_output_address(output.method, output.hash_digest)
                    fixed[name] = _Fixed(wanted, a.got, a.path, a.references)
            final[s.path] = path
            dest = real_store_dir / _base(path)
            nar_hash, nar_size = _nar_hash(actual)
            if not check:
                if path != s.path:
                    lock = PathLock(dest)
                    # `derivation-builder.cc:1510`: a floating output waits for a build of the same path.
                    # A mismatched fixed output does not: a build with the right hash may hold the lock.
                    if name not in fixed:
                        lock.acquire()
                    elif not lock.try_acquire():
                        delete_path(actual)
                        raise _check(drv_path, fixed[name]) or RuntimeError(f"'{drv_path}' made '{path}' for '{name}'")
                    locks.append(lock)
                if path != s.path and is_valid(path):
                    delete_path(actual)
                else:
                    delete_path(dest)
                    _move(actual, dest)
                    canonicalise(dest)
            infos[name] = ValidPathInfo(
                path=StorePath(path=path),
                info=UnkeyedValidPathInfo(
                    deriver=StorePath(path=drv_path),
                    nar_hash=NARHash(hash=nar_hash.hex()),
                    references={StorePath(path=ref) for ref in references},
                    registration_time=Time(ts=0),
                    nar_size=nar_size,
                    ultimate=True,
                    sigs=set(),
                    ca=WireContentAddress(addressed[name].render() if name in addressed else ""),
                ),
            )
    except BaseException:
        for lock in locks:
            lock.release(delete=True)
        raise
    failure = next((e for name in sorted(fixed) if (e := _check(drv_path, fixed[name])) is not None), None)
    return Collected(infos, [infos[name] for name in addressed], failure, locks)


def _content_addressed(
    name: str,
    drv_name: str,
    output: DerivationOutput,
    s: Scanned,
    actual: Path,
    store_dir: str,
    real_store_dir: Path,
    final: Mapping[str, str],
    *,
    discard: bool,
) -> _Addressed:
    """`newInfoFromCA` (`unix/build/derivation-builder.cc:1341`): the path the content has.

    The final hash of each output in *final* goes in first, as `rewriteOutput(outputRewrites)` puts it.
    Nix rewrites the whole tree whenever a hash moves; a hash the scan did not find is not in the
    tree, so pynixd rewrites only for a found one, or for an output it did not scan.
    """
    method, algo = output_method(output.method)
    if method in (Method.FLAT, Method.TEXT) and (not stat.S_ISREG(s.mode) or s.mode & stat.S_IXUSR):
        raise BuildFailure(
            BuildResultStatus.OUTPUT_REJECTED,
            f"output path {_quoted(actual)} should be a non-executable regular file since recursive "
            "hashing is not enabled (one of outputHashMode={flat,text} is true)",
        )
    moved = {
        _hash_part(old): _hash_part(new)
        for old, new in final.items()
        if old != new and (discard or old in s.references)
    }
    if moved:
        _rewrite(actual, moved, real_store_dir)
    old_hash_part = _hash_part(s.path)
    if method is Method.GIT:
        # `git::dumpHash` takes no modulo: the scratch hash part stays in what it hashes.
        try:
            digest = git_hash(actual, algo)[1]
        except AddError as e:
            raise PlainError(str(e)) from e
    else:
        digest = content_hash(ContentAddress(method, algo, b""), actual, old_hash_part, dump_path)
    got = ContentAddress(method, algo, digest)
    self_reference = s.path in s.references
    others = frozenset(final.get(ref, ref) for ref in s.references if ref != s.path)
    try:
        path = content_addressed_path(
            got, _output_path_name(drv_name, name), others, store_dir, self_reference=self_reference
        )
    except ValueError as e:
        # `ContentAddressWithReferences::fromParts` and `makeFixedOutputPath` throw a plain `Error`.
        raise PlainError(str(e)) from e
    if path != s.path and (self_reference or discard):
        _rewrite(actual, {old_hash_part: _hash_part(path)}, real_store_dir)
    return _Addressed(got, path, others | {path} if self_reference else others)


def compare_outputs(
    drv_path: str,
    infos: Mapping[str, ValidPathInfo],
    registered: Mapping[str, ValidPathInfo | None],
    real_store_dir: Path,
) -> tuple[BuildFailure | None, list[ValidPathInfo]]:
    """`registerOutputs` in check mode (`derivation-builder.cc:1546`): each rebuilt output against the valid one.

    Answers the first difference, and the valid infos to mark `ultimate`, which a check verified.
    """
    verified: list[ValidPathInfo] = []
    for info in infos.values():
        old = registered.get(info.path.path)
        if old is None:
            continue
        if parse_any_sha256(str(old.info.nar_hash)) != parse_any_sha256(str(info.info.nar_hash)):
            return BuildFailure(
                BuildResultStatus.NOT_DETERMINISTIC,
                f"derivation '{magenta(drv_path)}' may not be deterministic: output "
                f"{_quoted(real_store_dir / _base(info.path.path))} differs",
            ), verified
        if not old.info.ultimate:
            old.info.ultimate = True
            verified.append(old)
    return None, verified


def _check(drv_path: str, f: _Fixed) -> BuildFailure | None:
    """`checkCAFixedOutput` (`build/derivation-check.cc:10`)."""
    if f.got.digest != f.wanted.digest:
        return BuildFailure(
            BuildResultStatus.HASH_MISMATCH,
            f"hash mismatch in fixed-output derivation '{magenta(drv_path)}':\n"
            f"  specified: {magenta(_sri(f.wanted.algo, f.wanted.digest))}\n"
            f"     got:    {magenta(_sri(f.got.algo, f.got.digest))}",
        )
    if f.references:
        return BuildFailure(
            BuildResultStatus.HASH_MISMATCH,
            f"fixed-output derivations must not reference store paths: '{magenta(drv_path)}' references "
            f"{magenta(len(f.references))} distinct paths, e.g. '{magenta(min(f.references))}'",
        )
    return None


@dataclass(frozen=True)
class OutputChecks:
    """`DerivationOptions::OutputChecks`: each word is a store path or an output name."""

    ignore_self_refs: bool = False
    max_size: int | None = None
    max_closure_size: int | None = None
    allowed_references: frozenset[str] | None = None
    allowed_requisites: frozenset[str] | None = None
    disallowed_references: frozenset[str] = field(default_factory=frozenset)
    disallowed_requisites: frozenset[str] = field(default_factory=frozenset)


_REF_LISTS = ("allowedReferences", "allowedRequisites", "disallowedReferences", "disallowedRequisites")
_SIZES = ("maxSize", "maxClosureSize")


def _words(value: Any) -> frozenset[str]:
    """`getStringList`; anything else is an error Nix throws, which the daemon reports."""
    if not isinstance(value, list) or not all(isinstance(word, str) for word in value):
        raise ValueError("a list that is not a list of strings")
    return frozenset(value)


def _size(value: Any) -> int:
    """`get<uint64_t>`: a negative number would wrap, and a non-number throws."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("a size that is not a non-negative integer")
    return value


def output_checks(env: Mapping[str, str]) -> OutputChecks | dict[str, OutputChecks]:
    """`DerivationOptions::outputChecks` (`derivation-options.cc:214`).

    Without structured attrs, one set for every output that ignores a
    self-reference in the recursive checks. With them, one set per output
    name. `ValueError` names what pynixd leaves to the daemon.
    """
    if "__json" not in env:
        lists = {name: frozenset(env[name].split()) if name in env else None for name in _REF_LISTS}
        return OutputChecks(
            ignore_self_refs=True,
            allowed_references=lists["allowedReferences"],
            allowed_requisites=lists["allowedRequisites"],
            disallowed_references=lists["disallowedReferences"] or frozenset(),
            disallowed_requisites=lists["disallowedRequisites"] or frozenset(),
        )
    raw = parse_structured_attrs(env["__json"]).get("outputChecks")
    if raw is None:
        return {}
    if not isinstance(raw, dict) or not all(isinstance(value, dict) for value in raw.values()):
        raise ValueError("outputChecks that is not an object of objects")
    checks: dict[str, OutputChecks] = {}
    for output, value in raw.items():
        sizes = {name: _size(value[name]) if name in value else None for name in _SIZES}
        lists = {name: _words(value[name]) if name in value else None for name in _REF_LISTS}
        checks[output] = OutputChecks(
            max_size=sizes["maxSize"],
            max_closure_size=sizes["maxClosureSize"],
            allowed_references=lists["allowedReferences"],
            allowed_requisites=lists["allowedRequisites"],
            disallowed_references=lists["disallowedReferences"] or frozenset(),
            disallowed_requisites=lists["disallowedRequisites"] or frozenset(),
        )
    return checks


def discarded_references(env: Mapping[str, str]) -> set[str]:
    """The outputs `unsafeDiscardReferences` names true (`derivation-options.cc:277`); structured attrs only."""
    if "__json" not in env:
        return set()
    raw = parse_structured_attrs(env["__json"]).get("unsafeDiscardReferences")
    if raw is None:
        return set()
    if not isinstance(raw, dict) or not all(isinstance(value, bool) for value in raw.values()):
        raise ValueError("unsafeDiscardReferences that is not an object of Booleans")
    return {name for name, value in raw.items() if value}


def check_outputs(
    drv_path: str,
    checks: OutputChecks | Mapping[str, OutputChecks],
    infos: Mapping[str, ValidPathInfo],
    *,
    store_dir: str,
    path_info: Callable[[str], ValidPathInfo | None],
) -> BuildFailure | None:
    """`checkOutputs` (`build/derivation-check.cc:39`) after the fixed-output check.

    Op 36 carries no input derivations, so no placeholder resolves: an exact
    store path names a path, and any other word names an output.
    *path_info* blocks: call this from a worker thread.
    """
    by_path = {info.path.path: info for info in infos.values()}

    def closure(start: str) -> tuple[set[str], int]:
        done: set[str] = set()
        size = 0
        left = deque([start])
        while left:
            path = left.popleft()
            if path in done:
                continue
            done.add(path)
            info = by_path.get(path) or path_info(path)
            if info is None:
                raise RuntimeError(f"path '{path}' is not valid")
            size += info.info.nar_size
            left.extend(ref.path for ref in info.info.references)
        return done, size

    def spec(output: str, words: Iterable[str]) -> set[str] | BuildFailure:
        paths: set[str] = set()
        # `DrvRef` orders a store path before an output name.
        for word in sorted(words, key=lambda w: (not is_store_path(w, store_dir), w)):
            if is_store_path(word, store_dir):
                paths.add(word)
            elif word in infos:
                paths.add(infos[word].path.path)
            else:
                return BuildFailure(
                    BuildResultStatus.OUTPUT_REJECTED,
                    f"derivation '{magenta(drv_path)}' output check for '{magenta(output)}' contains output name "
                    f"'{magenta(word)}', but this is not a valid output of this derivation. "
                    f"(Valid outputs are [{magenta(', '.join(sorted(infos)))}].)",
                )
        return paths

    def apply(name: str, info: ValidPathInfo, c: OutputChecks) -> BuildFailure | None:
        path = info.path.path
        if c.max_size is not None and info.info.nar_size > c.max_size:
            return BuildFailure(
                BuildResultStatus.OUTPUT_REJECTED,
                f"path '{magenta(path)}' is too large at {magenta(info.info.nar_size)} bytes; "
                f"limit is {magenta(c.max_size)} bytes",
            )
        if c.max_closure_size is not None and (size := closure(path)[1]) > c.max_closure_size:
            return BuildFailure(
                BuildResultStatus.OUTPUT_REJECTED,
                f"closure of path '{magenta(path)}' is too large at {magenta(size)} bytes; "
                f"limit is {magenta(c.max_closure_size)} bytes",
            )
        # An absent allow-list allows everything; an empty disallow-list is skipped.
        lists = (
            (c.allowed_references, True, False),
            (c.allowed_requisites, True, True),
            (c.disallowed_references or None, False, False),
            (c.disallowed_requisites or None, False, True),
        )
        for words, allowed, recursive in lists:
            if words is None:
                continue
            refs = spec(name, words)
            if isinstance(refs, BuildFailure):
                return refs
            if recursive:
                used = closure(path)[0]
                if c.ignore_self_refs:
                    used.discard(path)
            else:
                used = {ref.path for ref in info.info.references}
            bad = {p for p in used if (p not in refs) == allowed}
            if bad:
                listing = "".join(f"\n  {p}" for p in sorted(bad, key=_base))
                return BuildFailure(
                    BuildResultStatus.OUTPUT_REJECTED,
                    f"output '{magenta(path)}' is not allowed to refer to the following paths:{magenta(listing)}",
                )
        return None

    for name in sorted(infos):
        c = checks if isinstance(checks, OutputChecks) else checks.get(name)
        if c is not None and (failure := apply(name, infos[name], c)) is not None:
            return failure
    return None


def _move(actual: Path, dest: Path) -> None:
    """Move a canonicalised output into the store.

    A directory's rename to another parent rewrites its `..`, which needs write
    permission on it: `canonicalise` left it 0555. The daemon moves it as root,
    which this check never stops; pynixd builds as its own user, so the caller
    canonicalises again after.
    """
    st = os.lstat(actual)
    if stat.S_ISDIR(st.st_mode):
        os.chmod(actual, stat.S_IMODE(st.st_mode) | stat.S_IWUSR)
    os.rename(actual, dest)


def _rewrite(actual: Path, rewrites: Mapping[str, str], real_store_dir: Path) -> None:
    """`rewriteOutput`: each key becomes its value throughout the tree, by a NAR round trip through the store."""
    tmp = TempDir.create(real_store_dir)
    try:
        staged = tmp.path / "x"
        encoded = {old.encode(): new.encode() for old, new in rewrites.items()}
        restore_path(staged, _Reader(_rewritten(dump_path(actual), encoded)))
        delete_path(actual)
        os.rename(staged, actual)
        canonicalise(actual)
    finally:
        tmp.remove()


def _rewritten(pieces: Iterable[bytes], rewrites: Mapping[bytes, bytes]) -> Iterator[bytes]:
    """`RewritingSink`: keeps the longest key's length less one byte back, so a match across two pieces is found."""
    pattern = re.compile(b"|".join(re.escape(old) for old in rewrites))
    keep = max(map(len, rewrites)) - 1
    prev = b""
    for piece in pieces:
        s = pattern.sub(lambda m: rewrites[m.group()], prev + piece)
        prev = s[len(s) - keep :] if len(s) > keep else s
        yield s[: len(s) - len(prev)]
    yield prev


class _Reader:
    """The sync `read` that `restore_path` takes, over an iterator of pieces."""

    def __init__(self, pieces: Iterable[bytes]) -> None:
        self._pieces = iter(pieces)
        self._buffer = memoryview(b"")

    def __call__(self, n: int) -> bytes:
        while not self._buffer:
            piece = next(self._pieces, None)
            if piece is None:
                return b""
            self._buffer = memoryview(piece)
        out = bytes(self._buffer[:n])
        self._buffer = self._buffer[n:]
        return out
