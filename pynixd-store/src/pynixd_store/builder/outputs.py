"""`registerOutputs` (`unix/build/derivation-builder.cc:1069`) for input-addressed and fixed outputs.

The builder wrote each output inside the chroot, at the path it will have, so
no hash is rewritten: the sandbox needs none (`needsHashRewrite`).
"""

from __future__ import annotations

import base64
import os
import stat
from dataclasses import dataclass
from typing import TYPE_CHECKING

from nix_daemon_protocol.build_result import BuildResultStatus
from nix_daemon_protocol.content_address import ContentAddress as WireContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

from ..add import _nar_hash, canonicalise, content_hash, delete_path
from ..content_address import ContentAddress, Method, content_addressed_path, fixed_output_address
from ..hintfmt import magenta
from ..nar_fs import dump_path
from .refscan import scan_for_references

if TYPE_CHECKING:
    from collections.abc import Mapping
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
) -> dict[str, ValidPathInfo]:
    """Each output the build made, moved into the store and described; nothing is registered yet.

    An output in *valid* is not looked at, as Nix does outside check mode.
    Blocking: call it from a worker thread.
    """
    referenceable = input_paths | {o.path for o in outputs.values()}
    scanned = {
        name: _scan(drv_path, name, o.path, chroot_root / o.path.lstrip("/"), referenceable)
        for name, o in outputs.items()
        if o.path not in valid
    }
    infos: dict[str, ValidPathInfo] = {}
    for name in _sorted(drv_path, scanned):
        output, s = outputs[name], scanned[name]
        actual = chroot_root / s.path.lstrip("/")
        ca: ContentAddress | None = None
        path = s.path
        if output.method:
            ca, path = _fixed(drv_path, name, drv_name, output, s, actual, store_dir)
        dest = real_store_dir / _base(path)
        delete_path(dest)
        os.rename(actual, dest)
        canonicalise(dest)
        nar_hash, nar_size = _nar_hash(dest)
        infos[name] = ValidPathInfo(
            path=StorePath(path=path),
            info=UnkeyedValidPathInfo(
                deriver=StorePath(path=drv_path),
                nar_hash=NARHash(hash=nar_hash.hex()),
                references={StorePath(path=ref) for ref in s.references},
                registration_time=Time(ts=0),
                nar_size=nar_size,
                ultimate=True,
                sigs=set(),
                ca=WireContentAddress(ca.render() if ca else ""),
            ),
        )
    return infos


def _fixed(
    drv_path: str, name: str, drv_name: str, output: DerivationOutput, s: Scanned, actual: Path, store_dir: str
) -> tuple[ContentAddress, str]:
    """`newInfoFromCA` for a fixed output, then `checkCAFixedOutput` (`build/derivation-check.cc:10`)."""
    wanted = fixed_output_address(output.method, output.hash_digest)
    if wanted.method in (Method.FLAT, Method.TEXT) and (not stat.S_ISREG(s.mode) or s.mode & stat.S_IXUSR):
        raise BuildFailure(
            BuildResultStatus.OUTPUT_REJECTED,
            f"output path {_quoted(actual)} should be a non-executable regular file since recursive "
            "hashing is not enabled (one of outputHashMode={flat,text} is true)",
        )
    got = ContentAddress(wanted.method, wanted.algo, content_hash(wanted, actual, _base(s.path)[:32], dump_path))
    if got.digest != wanted.digest:
        raise BuildFailure(
            BuildResultStatus.HASH_MISMATCH,
            f"hash mismatch in fixed-output derivation '{magenta(drv_path)}':\n"
            f"  specified: {magenta(_sri(wanted.algo, wanted.digest))}\n"
            f"     got:    {magenta(_sri(got.algo, got.digest))}",
        )
    if s.references:
        raise BuildFailure(
            BuildResultStatus.HASH_MISMATCH,
            f"fixed-output derivations must not reference store paths: '{magenta(drv_path)}' references "
            f"{magenta(len(s.references))} distinct paths, e.g. '{magenta(min(s.references))}'",
        )
    output_name = drv_name if name == "out" else f"{drv_name}-{name}"
    return got, content_addressed_path(got, output_name, (), store_dir)
