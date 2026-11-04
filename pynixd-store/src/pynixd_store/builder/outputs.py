"""`registerOutputs` (`unix/build/derivation-builder.cc:1069`) for input-addressed and fixed outputs.

The builder wrote each output inside the chroot, at the path it will have, so
no hash is rewritten: the sandbox needs none (`needsHashRewrite`).
"""

from __future__ import annotations

import base64
import os
import re
import stat
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

from nix_daemon_protocol.build_result import BuildResultStatus
from nix_daemon_protocol.content_address import ContentAddress as WireContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from ..add import PathLock, TempDir, _nar_hash, canonicalise, content_hash, delete_path
from ..content_address import ContentAddress, Method, content_addressed_path, fixed_output_address
from ..hintfmt import magenta
from ..nar_fs import dump_path, restore_path
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


def _sri(algo: str, digest: bytes) -> str:
    return f"{algo}-{base64.b64encode(digest).decode()}"


def _scan(drv_path: str, name: str, path: str, actual: Path, referenceable: set[str]) -> Scanned:
    try:
        st = os.lstat(actual)
    except FileNotFoundError:
        raise BuildFailure(
            BuildResultStatus.OUTPUT_REJECTED,
            f"builder for '{magenta(drv_path)}' failed to produce output path for output "
            f"'{magenta(name)}' at {_quoted(actual)}",
        ) from None
    if not stat.S_ISLNK(st.st_mode) and st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise BuildFailure(
            BuildResultStatus.OUTPUT_REJECTED,
            f"suspicious ownership or permission on {_quoted(actual)} for output "
            f"'{magenta(name)}'; rejecting this build output",
        )
    canonicalise(actual)
    return Scanned(path, frozenset(scan_for_references(actual, referenceable)), st.st_mode)


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


def collect_outputs(
    drv_path: str,
    outputs: Mapping[str, DerivationOutput],
    *,
    drv_name: str,
    chroot_root: Path,
    real_store_dir: Path,
    store_dir: str,
    input_paths: set[str],
    valid: set[str],
    is_valid: Callable[[str], bool],
) -> Collected:
    """Each output the build made, moved into the store and described; nothing is registered yet.

    An output in *valid* is not looked at, as Nix does outside check mode.
    *is_valid* asks the store about a path no output names. Blocking: call it
    from a worker thread.
    """
    referenceable = input_paths | {o.path for o in outputs.values()}
    scanned = {
        name: _scan(drv_path, name, o.path, chroot_root / o.path.lstrip("/"), referenceable)
        for name, o in outputs.items()
        if o.path not in valid
    }
    infos: dict[str, ValidPathInfo] = {}
    fixed: dict[str, _Fixed] = {}
    locks: list[PathLock] = []
    try:
        for name in _sorted(drv_path, scanned):
            output, s = outputs[name], scanned[name]
            actual = chroot_root / s.path.lstrip("/")
            path, references = s.path, s.references
            if output.method:
                f = fixed[name] = _fixed(name, drv_name, output, s, actual, store_dir, real_store_dir)
                path, references = f.path, f.references
            dest = real_store_dir / _base(path)
            nar_hash, nar_size = _nar_hash(actual)
            if path != s.path:
                lock = PathLock(dest)
                # Nix does not wait here: a build with the right hash may hold the lock.
                if not lock.try_acquire():
                    delete_path(actual)
                    raise _check(drv_path, fixed[name]) or RuntimeError(f"'{drv_path}' made '{path}' for '{name}'")
                locks.append(lock)
            if path != s.path and is_valid(path):
                delete_path(actual)
            else:
                delete_path(dest)
                os.rename(actual, dest)
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
                    ca=WireContentAddress(fixed[name].got.render() if name in fixed else ""),
                ),
            )
    except BaseException:
        for lock in locks:
            lock.release(delete=True)
        raise
    failure = next((e for name in sorted(fixed) if (e := _check(drv_path, fixed[name])) is not None), None)
    return Collected(infos, [infos[name] for name in fixed], failure, locks)


def _fixed(
    name: str,
    drv_name: str,
    output: DerivationOutput,
    s: Scanned,
    actual: Path,
    store_dir: str,
    real_store_dir: Path,
) -> _Fixed:
    """`newInfoFromCA` (`unix/build/derivation-builder.cc:1341`) for a fixed output: the path its content has."""
    wanted = fixed_output_address(output.method, output.hash_digest)
    if wanted.method in (Method.FLAT, Method.TEXT) and (not stat.S_ISREG(s.mode) or s.mode & stat.S_IXUSR):
        raise BuildFailure(
            BuildResultStatus.OUTPUT_REJECTED,
            f"output path {_quoted(actual)} should be a non-executable regular file since recursive "
            "hashing is not enabled (one of outputHashMode={flat,text} is true)",
        )
    old_hash_part = _base(s.path)[:32]
    got = ContentAddress(wanted.method, wanted.algo, content_hash(wanted, actual, old_hash_part, dump_path))
    self_reference = s.path in s.references
    others = s.references - {s.path}
    output_name = drv_name if name == "out" else f"{drv_name}-{name}"
    path = content_addressed_path(got, output_name, others, store_dir, self_reference=self_reference)
    if path != s.path and self_reference:
        _rewrite(actual, old_hash_part, _base(path)[:32], real_store_dir)
    return _Fixed(wanted, got, path, others | {path} if self_reference else others)


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


OUTPUT_CHECKS = (
    ("allowedReferences", True, False),
    ("allowedRequisites", True, True),
    ("disallowedReferences", False, False),
    ("disallowedRequisites", False, True),
)
"""Each attribute `checkOutputs` reads without structured attrs: allowed, recursive."""

_STORE_PATH_BASE = re.compile(r"[0-9a-df-np-sv-z]{32}-([A-Za-z0-9+\-._?=]{1,211})")


def check_outputs(
    drv_path: str,
    env: Mapping[str, str],
    infos: Mapping[str, ValidPathInfo],
    *,
    store_dir: str,
    path_info: Callable[[str], ValidPathInfo | None],
) -> BuildFailure | None:
    """`checkOutputs` (`build/derivation-check.cc:39`) for a derivation without structured attrs.

    Op 36 carries no input derivations, so no placeholder resolves: an exact
    store path names a path, and any other word names an output.
    *path_info* blocks: call this from a worker thread.
    """
    by_path = {info.path.path: info for info in infos.values()}

    def closure(start: str) -> set[str]:
        done: set[str] = set()
        left = deque([start])
        while left:
            path = left.popleft()
            if path in done:
                continue
            done.add(path)
            info = by_path.get(path) or path_info(path)
            if info is None:
                raise RuntimeError(f"path '{path}' is not valid")
            left.extend(ref.path for ref in info.info.references)
        return done

    def spec(output: str, words: Iterable[str]) -> set[str] | BuildFailure:
        paths: set[str] = set()
        # `DrvRef` orders a store path before an output name.
        for word in sorted(words, key=lambda w: (not _is_store_path(w, store_dir), w)):
            if _is_store_path(word, store_dir):
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

    for name in sorted(infos):
        info = infos[name]
        for attribute, allowed, recursive in OUTPUT_CHECKS:
            if attribute not in env:
                continue
            words = set(env[attribute].split())
            if not allowed and not words:
                continue
            refs = spec(name, words)
            if isinstance(refs, BuildFailure):
                return refs
            if recursive:
                used = closure(info.path.path) - {info.path.path}
            else:
                used = {ref.path for ref in info.info.references}
            bad = {p for p in used if (p not in refs) == allowed}
            if bad:
                listing = "".join(f"\n  {p}" for p in sorted(bad, key=_base))
                return BuildFailure(
                    BuildResultStatus.OUTPUT_REJECTED,
                    f"output '{magenta(info.path.path)}' is not allowed to refer to the following paths:"
                    f"{magenta(listing)}",
                )
    return None


def _is_store_path(word: str, store_dir: str) -> bool:
    """`StoreDirConfig::isStorePath`: in the store directory itself, with a valid base name."""
    parent, _, base = word.rpartition("/")
    match = _STORE_PATH_BASE.fullmatch(base)
    if parent != store_dir or match is None:
        return False
    name = match.group(1)
    return name not in (".", "..") and not name.startswith((".-", "..-"))


def _rewrite(actual: Path, old: str, new: str, real_store_dir: Path) -> None:
    """`rewriteOutput`: *old* becomes *new* throughout the tree, by a NAR round trip through the store."""
    tmp = TempDir.create(real_store_dir)
    try:
        staged = tmp.path / "x"
        restore_path(staged, _Reader(_rewritten(dump_path(actual), old.encode(), new.encode())))
        delete_path(actual)
        os.rename(staged, actual)
        canonicalise(actual)
    finally:
        tmp.remove()


def _rewritten(pieces: Iterable[bytes], old: bytes, new: bytes) -> Iterator[bytes]:
    """`RewritingSink`: keeps `len(old) - 1` bytes back, so a match across two pieces is found."""
    keep = len(old) - 1
    prev = b""
    for piece in pieces:
        s = (prev + piece).replace(old, new)
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
