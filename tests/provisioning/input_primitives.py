"""Time each way to put an input closure in front of a build. nixidae/nixidae#1560, spike.

Runs with pynixd's interpreter, so `_bind` is the helper's own: on the node, or in a build pod's sandbox
container. The closures come from `--closures FILE` (JSON, `{label: [store path, ...]}`) or from `label=path`
arguments, queried here. stdout: JSON, `{label: {way: [seconds, ...]}}`.

Modes:

  namespace  in a new user and mount namespace, as the helper makes one, unprivileged:
             bind      a tmpfs root, then `_bind` of each path (`doBind`); an NRI bind farm costs the same
             overlay   one overlay mount with `/nix/store` as its lower layer, then a `stat` of each path
  copy       `nix copy` of the closure into a new local store: the floor of today's NAR stream
  hardlink   a tree of hard links to each file of the closure, on the store's own filesystem (`--store-root`)
  composefs  as root: an image that names each file's store path and no digest, as nixkube writes it
             (`mkcomposefs --from-file`), then its EROFS mount and the overlay over the store
"""

from __future__ import annotations

import argparse
import json
import os
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import TextIO

from pynixd_store.builder.child import MNT_DETACH, MS_PRIVATE, MS_REC, _bind, _check, _libc, _mount

STORE = Path("/nix/store")
MS_RDONLY = 1
CLONE_NEWUSER = 0x10000000
CLONE_NEWNS = 0x00020000

type Row = dict[str, list[float]]
type Closures = dict[str, list[str]]


def unmount(target: Path) -> None:
    _check(_libc.umount2(os.fsencode(target), MNT_DETACH), f"unmounting '{target}'")


def timed[*Ts](action: Callable[[*Ts], object], *args: *Ts) -> float:
    started = time.monotonic()
    action(*args)
    return time.monotonic() - started


def run(command: list[str]) -> None:
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)


def enter_namespace() -> None:
    """The helper's own user and mount namespace, with this process's ids mapped to root in it."""
    uid, gid = os.getuid(), os.getgid()
    os.unshare(CLONE_NEWUSER | CLONE_NEWNS)
    Path("/proc/self/setgroups").write_text("deny")
    Path("/proc/self/uid_map").write_text(f"0 {uid} 1")
    Path("/proc/self/gid_map").write_text(f"0 {gid} 1")
    _mount(None, "/", None, MS_PRIVATE | MS_REC)


def bind_all(paths: list[str], root: Path) -> None:
    for path in paths:
        _bind(path, root, path.lstrip("/"), False)


def bind(paths: list[str], top: Path) -> float:
    root = top / "root"
    root.mkdir(parents=True)
    _mount("none", str(root), "tmpfs", 0)
    seconds = timed(bind_all, paths, root)
    unmount(root)
    return seconds


def look_up(paths: list[str], at: Path) -> None:
    for path in paths:
        (at / Path(path).name).lstat()


def lookups(paths: list[str], at: Path) -> float:
    return timed(look_up, paths, at)


def overlay(paths: list[str], top: Path) -> tuple[float, float]:
    top.mkdir()
    _mount("none", str(top), "tmpfs", 0)
    for name in ("upper", "work", "merged"):
        (top / name).mkdir()
    merged = top / "merged"
    options = f"lowerdir={STORE},upperdir={top}/upper,workdir={top}/work"
    mounted = timed(_mount, "overlay", str(merged), "overlay", 0, options)
    looked_up = lookups(paths, merged)
    unmount(merged)
    unmount(top)
    return mounted, looked_up


def namespace(closures: Closures, repeats: int, scratch: Path) -> dict[str, Row]:
    enter_namespace()
    found: dict[str, Row] = {}
    for label, paths in closures.items():
        row: Row = {"bind": [], "overlay mount": [], "overlay lookups": []}
        for i in range(repeats):
            row["bind"].append(bind(paths, scratch / f"{label}-bind-{i}"))
            mounted, looked_up = overlay(paths, scratch / f"{label}-overlay-{i}")
            row["overlay mount"].append(mounted)
            row["overlay lookups"].append(looked_up)
        found[label] = row
    return found


def remove(tree: Path) -> None:
    subprocess.run(["chmod", "-R", "u+w", str(tree)], check=True)
    subprocess.run(["rm", "-rf", str(tree)], check=True)


def copy(closures: Closures, repeats: int, scratch: Path) -> dict[str, Row]:
    found: dict[str, Row] = {}
    for label, paths in closures.items():
        seconds = []
        for _ in range(repeats):
            store = Path(tempfile.mkdtemp(dir=scratch))
            command = ["nix", "copy", "--no-check-sigs", "--to", f"local?root={store}", *paths]
            seconds.append(timed(run, command))
            remove(store)
        found[label] = {"nix copy": seconds}
    return found


def link_tree(source: Path, target: Path) -> None:
    """`target` as `source`, each file a hard link: the hardlink presentation nixkube measured."""
    st = source.lstat()
    if stat.S_ISDIR(st.st_mode):
        target.mkdir()
        with os.scandir(source) as entries:
            for entry in entries:
                link_tree(Path(entry.path), target / entry.name)
    elif stat.S_ISLNK(st.st_mode):
        target.symlink_to(os.readlink(source))
    else:
        os.link(source, target)


def link_closure(paths: list[str], store_root: Path, tree: Path) -> None:
    for path in paths:
        link_tree(store_root / path.lstrip("/"), tree / Path(path).name)


def hardlink(closures: Closures, repeats: int, scratch: Path, store_root: Path) -> dict[str, Row]:
    found: dict[str, Row] = {}
    for label, paths in closures.items():
        seconds = []
        for i in range(repeats):
            tree = scratch / f"{label}-links-{i}"
            tree.mkdir()
            seconds.append(timed(link_closure, paths, store_root, tree))
            remove(tree)
        found[label] = {"hardlink tree": seconds}
    return found


def escape(text: str) -> str:
    """A composefs-dump(5) field, as nixkube's `composefs.escape` writes it."""
    return "".join(chr(b) if 0x21 <= b <= 0x7E and b not in (0x5C, 0x3D) else f"\\x{b:02x}" for b in text.encode())


def dump_line(out: TextIO, path: str, size: int, mode: int, nlink: int, payload: str) -> None:
    out.write(f"{escape(path)} {size} {mode:o} {nlink} 0 0 0 1.0 {payload} - -\n")


def dump_tree(out: TextIO, path: str, real: Path) -> None:
    st = real.lstat()
    if stat.S_ISLNK(st.st_mode):
        target = os.readlink(real)
        dump_line(out, path, len(target.encode()), st.st_mode, 1, escape(target))
    elif stat.S_ISDIR(st.st_mode):
        dump_line(out, path, 0, st.st_mode, 2, "-")
        with os.scandir(real) as entries:
            for entry in sorted(entries, key=lambda e: e.name):
                dump_tree(out, f"{path}/{entry.name}", Path(entry.path))
    else:
        dump_line(out, path, st.st_size, st.st_mode, 1, escape(str(real.relative_to(STORE))))


def write_dump(paths: list[str], dump: Path) -> None:
    with dump.open("w") as out:
        dump_line(out, "/", 0, stat.S_IFDIR | 0o555, 2, "-")
        for path in sorted(paths):
            dump_tree(out, f"/{Path(path).name}", Path(path))


def mount_composefs(image: Path, erofs: Path, merged: Path) -> None:
    _mount(str(image), str(erofs), "erofs", MS_RDONLY)
    options = f"metacopy=on,redirect_dir=on,lowerdir={erofs}::{STORE}"
    _mount("overlay", str(merged), "overlay", MS_RDONLY, options)


def composefs(closures: Closures, repeats: int, scratch: Path) -> dict[str, Row]:
    """nixkube's `compose` without the CSI around it: the image, its EROFS, the overlay with the store as a
    data-only lower layer (`::`), so only the image's paths show."""
    found: dict[str, Row] = {}
    for label, paths in closures.items():
        row: Row = {"dump": [], "mkcomposefs": [], "composefs mount": [], "composefs lookups": []}
        for i in range(repeats):
            top = scratch / f"{label}-cfs-{i}"
            erofs, merged = top / "erofs", top / "merged"
            for directory in (erofs, merged):
                directory.mkdir(parents=True)
            dump, image = top / "dump", top / "image.cfs"
            row["dump"].append(timed(write_dump, paths, dump))
            row["mkcomposefs"].append(timed(run, ["mkcomposefs", "--from-file", str(dump), str(image)]))
            row["composefs mount"].append(timed(mount_composefs, image, erofs, merged))
            row["composefs lookups"].append(lookups(paths, merged))
            unmount(merged)
            unmount(erofs)
            remove(top)
        found[label] = row
    return found


def closure(path: str) -> list[str]:
    query = subprocess.run(["nix-store", "--query", "--requisites", path], check=True, capture_output=True, text=True)
    return query.stdout.split()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("namespace", "copy", "hardlink", "composefs"))
    parser.add_argument("pairs", nargs="*", help="label=store path")
    parser.add_argument("--closures", type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--store-root", type=Path, default=Path("/"))
    args = parser.parse_args()
    closures: Closures = (
        json.loads(args.closures.read_text())
        if args.closures
        else {label: closure(path) for label, path in (pair.split("=", 1) for pair in args.pairs)}
    )
    scratch = Path(tempfile.mkdtemp(dir=args.scratch))
    match args.mode:
        case "namespace":
            found = namespace(closures, args.repeats, scratch)
        case "copy":
            found = copy(closures, args.repeats, scratch)
        case "hardlink":
            found = hardlink(closures, args.repeats, scratch, args.store_root)
        case _:
            found = composefs(closures, args.repeats, scratch)
    json.dump(found, sys.stdout)


if __name__ == "__main__":
    main()
