"""Time each way to put an input closure in front of a build. nixidae/nixidae#1560, spike.

Runs in the guest with pynixd's interpreter, so `_bind` is the helper's own. Arguments: a mode, the repeats,
then `label=store path` for each closure. stdout: JSON, `{label: {way: [seconds, ...]}}`.

Mode `namespace` runs under `unshare --user --map-root-user --mount`: the unprivileged namespaces a build
pod's helper works in.

  bind     a tmpfs root, then `_bind` of each path: what the helper does for each input (`doBind`), and what an
           NRI bind farm does on the host
  overlay  one overlay mount with the store as its lower layer, then a `stat` of each path through it

Mode `copy` runs as root: `nix copy` of the closure into a new local store, the floor of the NAR stream a build
pod gets today (no SSH, no network).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from pynixd_store.builder.child import MNT_DETACH, _bind, _check, _libc, _mount

SCRATCH = Path("/var/lib/pynixd-bench")


def unmount(target: Path) -> None:
    _check(_libc.umount2(os.fsencode(target), MNT_DETACH), f"unmounting '{target}'")


def bind(paths: list[str], top: Path) -> float:
    root = top / "root"
    root.mkdir(parents=True)
    _mount("none", str(root), "tmpfs", 0)
    started = time.monotonic()
    for path in paths:
        _bind(path, root, path.lstrip("/"), False)
    seconds = time.monotonic() - started
    unmount(root)
    return seconds


def overlay(paths: list[str], top: Path) -> tuple[float, float]:
    _mount("none", str(top), "tmpfs", 0)
    for name in ("upper", "work", "merged"):
        (top / name).mkdir()
    merged = top / "merged"
    started = time.monotonic()
    _mount("overlay", str(merged), "overlay", 0, f"lowerdir=/nix/store,upperdir={top}/upper,workdir={top}/work")
    mounted = time.monotonic() - started
    started = time.monotonic()
    for path in paths:
        (merged / Path(path).name).lstat()
    looked_up = time.monotonic() - started
    unmount(merged)
    unmount(top)
    return mounted, looked_up


def namespace(closures: dict[str, list[str]], repeats: int) -> dict[str, dict[str, list[float]]]:
    found: dict[str, dict[str, list[float]]] = {}
    with tempfile.TemporaryDirectory(dir=SCRATCH) as scratch:
        for label, paths in closures.items():
            row: dict[str, list[float]] = {"bind": [], "overlay mount": [], "overlay lookups": []}
            for i in range(repeats):
                row["bind"].append(bind(paths, Path(scratch, f"{label}-bind-{i}")))
                top = Path(scratch, f"{label}-overlay-{i}")
                top.mkdir()
                mounted, looked_up = overlay(paths, top)
                row["overlay mount"].append(mounted)
                row["overlay lookups"].append(looked_up)
            found[label] = row
    return found


def copy(closures: dict[str, list[str]], repeats: int) -> dict[str, dict[str, list[float]]]:
    found: dict[str, dict[str, list[float]]] = {}
    for label, paths in closures.items():
        seconds = []
        for _ in range(repeats):
            store = Path(tempfile.mkdtemp(dir=SCRATCH))
            started = time.monotonic()
            subprocess.run(
                ["nix", "copy", "--no-check-sigs", "--to", f"local?root={store}", *paths],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            seconds.append(time.monotonic() - started)
            subprocess.run(["chmod", "-R", "u+w", str(store)], check=True)
            shutil.rmtree(store)
        found[label] = {"nix copy": seconds}
    return found


def closure(path: str) -> list[str]:
    query = subprocess.run(["nix-store", "--query", "--requisites", path], check=True, capture_output=True, text=True)
    return query.stdout.split()


def main() -> None:
    mode, repeats = sys.argv[1], int(sys.argv[2])
    closures = {label: closure(path) for label, path in (pair.split("=", 1) for pair in sys.argv[3:])}
    run = namespace if mode == "namespace" else copy
    json.dump(run(closures, repeats), sys.stdout)


if __name__ == "__main__":
    main()
