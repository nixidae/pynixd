"""The roots a client adds: Nix's `IndirectRootStore` (`indirect-root-store.cc`) and `LocalStore::addIndirectRoot`.

Every function here blocks on the file system. The engine calls them on a
worker thread.
"""

from __future__ import annotations

import hashlib
import os
import random
from pathlib import Path

from .errors import PynixdError
from .hintfmt import magenta
from .utils import nix32_encode


class RootError(PynixdError):
    """Nix refuses this root, and the message is Nix's."""


def canon_path(path: str) -> str:
    """`canonPath` without symlink resolution (`file-system.cc:96`): no `.`, `..`, repeated or trailing `/`.

    Not `os.path.normpath`: it keeps a leading `//`, and Nix does not.
    """
    if not path:
        raise RootError("cannot canonicalise an empty path")
    if not path.startswith("/"):
        raise RootError(f"not an absolute path: {_path_fmt(path)}")
    parts: list[str] = []
    for part in path.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if parts:
                parts.pop()
            continue
        parts.append(part)
    return "/" + "/".join(parts)


def abs_path(path: str) -> str:
    """`absPath` (`file-system.cc:68`): *path* against the working directory, then `canonPath`."""
    return canon_path(path if path.startswith("/") else f"{os.getcwd()}/{path}")


def _path_fmt(path: str) -> str:
    """`PathFmt` of one argument (`fmt.hh:114`): double quotes, inside `HintFmt`'s colour."""
    return magenta(f'"{path}"')


def _in_store(path: str, store_dir: Path) -> bool:
    """`StoreDirConfig::isInStore`: under the store directory, and not the directory itself."""
    return path.startswith(f"{store_dir}/")


def make_symlink(link: str, target: str) -> None:
    """`IndirectRootStore::makeSymlink` (`indirect-root-store.cc:7`): parents, a temporary link, a rename."""
    os.makedirs(os.path.dirname(link), exist_ok=True)
    temporary = f"{link}.tmp-{os.getpid()}-{random.getrandbits(31)}"
    os.symlink(target, temporary)
    os.rename(temporary, link)


def add_indirect_root(state_dir: Path, path: str) -> None:
    """`LocalStore::addIndirectRoot` (`gc.cc:47`): `<state>/gcroots/auto/<sha1 of path>` links to *path*."""
    name = nix32_encode(hashlib.sha1(path.encode(), usedforsecurity=False).digest())
    make_symlink(canon_path(f"{state_dir}/gcroots/auto/{name}"), path)


def check_perm_root(gc_root: str, store_dir: Path) -> None:
    """The refusals of `IndirectRootStore::addPermRoot` (`indirect-root-store.cc:20`) before its temporary root."""
    if _in_store(gc_root, store_dir):
        raise RootError(
            f"creating a garbage collector root ({_path_fmt(gc_root)}) in the Nix store is forbidden "
            "(are you running nix-build inside the store?)"
        )


def add_perm_root(store_path: str, gc_root: str, store_dir: Path, state_dir: Path) -> None:
    """The rest of `addPermRoot`, after the temporary root: refuse to clobber, link, then the indirect root."""
    if os.path.lexists(gc_root) and (not os.path.islink(gc_root) or not _in_store(os.readlink(gc_root), store_dir)):
        raise RootError(f"cannot create symlink {_path_fmt(gc_root)}; already exists")
    make_symlink(gc_root, store_path)
    add_indirect_root(state_dir, gc_root)
