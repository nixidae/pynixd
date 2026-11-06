"""A build in a libkrun microVM: `pynixd-krun` boots a guest that runs the sandbox helper (nixidae/nixidae#1186).

The guest mounts each share at its host path, so the helper's spec means the same inside. The stores are
read-only, each with an allowlist: the build store shows its input closure, and the host's store shows the
closure the helper runs from. A builder that escapes the helper's namespaces sees no other store path. A
`sandbox-paths` directory outside the stores, such as `/run/binfmt` or a ccache directory, is a writable share
of its own. The build's two directories are writable shares: the top directory (the spec, the build directory) and the chroot,
where the outputs land for `collect_outputs`.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

#: Where the helper's interpreter and its closure live: the store pynixd itself runs from.
HOST_STORE = Path("/nix/store")
STATUS_NAME = "vm-status"
EXIT_NAME = "vm-exit"


@dataclass(frozen=True)
class BuildVm:
    launcher: Path
    init: Path
    python: Path
    """The interpreter that runs the helper in the guest: a store path, unlike an editable `sys.executable`."""
    helper_closure: frozenset[str]
    """The host store paths the helper runs from: its interpreter and every module it imports."""
    memory_mib: int
    cpus: int
    dax_window_mib: int

    @classmethod
    def from_environment(cls, *, memory_mib: int, cpus: int, dax_window_mib: int) -> BuildVm:
        """What the package names: the launcher (`PYNIXD_KRUN`), the init (`PYNIXD_KRUN_INIT`), the helper's
        interpreter (`PYNIXD_KRUN_PYTHON`) and its closure (`PYNIXD_KRUN_CLOSURE`, a file of store paths)."""
        names = ("PYNIXD_KRUN", "PYNIXD_KRUN_INIT", "PYNIXD_KRUN_PYTHON", "PYNIXD_KRUN_CLOSURE")
        missing = [name for name in names if name not in os.environ]
        if missing:
            raise ValueError(f"build_vm needs {', '.join(missing)} in the environment; the pynixd package sets them")
        closure = Path(os.environ["PYNIXD_KRUN_CLOSURE"]).read_text().split()
        return cls(
            launcher=Path(os.environ["PYNIXD_KRUN"]),
            init=Path(os.environ["PYNIXD_KRUN_INIT"]),
            python=Path(os.environ["PYNIXD_KRUN_PYTHON"]),
            helper_closure=frozenset(closure),
            memory_mib=memory_mib,
            cpus=cpus,
            dax_window_mib=dax_window_mib,
        )

    def argv(
        self,
        *,
        top: Path,
        chroot_parent: Path,
        real_store_dir: Path,
        visible: Iterable[str],
        sources: Iterable[str],
        spec_file: Path,
    ) -> list[str]:
        """The launcher's command line, after it writes the allowlists into *top*.

        *visible* names the build store's paths the guest may see, as store paths. *sources* are the host paths
        the helper binds (`sandbox-paths`). One in a store shows its store path. One outside both stores is
        writable, as Nix binds it read-write and the host's permissions decide (`programs.ccache`'s cache): a
        directory is a share of its own, and a file is a share of its parent that shows the file alone, because
        virtio-fs mounts only a directory. The helper reads *spec_file* and gets `top/vm-status` as its file
        descriptor 3.
        """
        shown: dict[Path, set[str]] = {
            real_store_dir: {str(real_store_dir / path.rsplit("/", 1)[-1]) for path in visible},
        }
        shown.setdefault(HOST_STORE, set()).update(self.helper_closure)
        writable: dict[Path, set[str] | None] = {}
        for source in map(Path, sources):
            store = _store_of(source, real_store_dir)
            if store is not None:
                shown[store].add(str(store / source.relative_to(store).parts[0]))
            elif source.is_dir():
                writable[source] = None
            # A missing source fails in the helper, as it does on the host.
            elif source.exists():
                filtered = writable.setdefault(source.parent, set())
                if filtered is not None:
                    filtered.add(str(source))
        ro = _outermost(shown)
        rw = _outermost_writable(writable)
        # The build's own two directories mount inside a share or beside it, and one inside a filtered share
        # needs its mount point shown there.
        for inner in (chroot_parent, top):
            for root, paths in (*ro.items(), *rw.items()):
                if paths is not None and inner.is_relative_to(root):
                    paths.add(str(inner))
        argv = [
            str(self.launcher),
            f"--init={self.init}",
            f"--cpus={self.cpus}",
            f"--memory={self.memory_mib}",
            f"--dax-window={self.dax_window_mib * 1024 * 1024}",
        ]
        shares: list[tuple[Path, str, list[str] | None]] = [
            *((root, "ro", sorted(paths)) for root, paths in ro.items()),
            *((root, "rw-allow" if paths is not None else "rw", sorted(paths or ())) for root, paths in rw.items()),
            (chroot_parent, "rw", None),
            (top, "rw", None),
        ]
        # A share inside another mounts after it; at one depth, the read-only ones first.
        for i, (root, kind, allowed) in enumerate(sorted(shares, key=lambda s: (len(s[0].parts), s[1] != "ro"))):
            if kind == "rw":
                argv.append(f"--rw={root}")
                continue
            allowlist = top / f"vm-allowlist-{i}"
            allowlist.write_text("".join(f"{p}\n" for p in allowed or ()))
            argv.append(f"--{kind}={root}:{allowlist}")
        argv += [
            f"--cwd={top}",
            f"--status={top / STATUS_NAME}",
            f"--exit={top / EXIT_NAME}",
            "--",
            str(self.python),
            # Isolated: a writable share holds what earlier builders wrote, and the working directory and
            # `PYTHON*` must not reach `sys.path`.
            "-I",
            "-m",
            "pynixd_store.builder.child",
            str(spec_file),
            "3",
        ]
        return argv


def _store_of(path: Path, real_store_dir: Path) -> Path | None:
    for root in (real_store_dir, HOST_STORE):
        if path.is_relative_to(root) and path != root:
            return root
    return None


def unshareable(sources: Iterable[str], real_store_dir: Path) -> str | None:
    """The first source that is a file directly in `/`, or `None`.

    virtio-fs mounts only a directory: the kernel fixes the root's mode at `S_IFDIR` (`fs/fuse/virtio_fs.c`,
    `virtio_fs_ctx_set_defaults`) and takes no `rootmode=`. So a file is a share of its parent, and a share of
    `/` would cover the guest's own root.
    """
    return next(
        (
            s
            for s in sources
            if _store_of(Path(s), real_store_dir) is None
            and Path(s).parent == Path("/")
            and Path(s).exists()
            and not Path(s).is_dir()
        ),
        None,
    )


def _outermost_writable(shares: dict[Path, set[str] | None]) -> dict[Path, set[str] | None]:
    """*shares* with each share inside another folded into it. `None` shows a whole share.

    A share inside a whole one adds nothing. One inside a filtered share shows through it: a whole directory
    as its own path, a filtered one as the paths it lists.
    """
    merged: dict[Path, set[str] | None] = {}
    for root in sorted(shares, key=lambda r: len(r.parts)):
        outer = next((r for r in merged if root.is_relative_to(r)), None)
        if outer is None:
            merged[root] = None if shares[root] is None else set(shares[root] or ())
            continue
        outer_paths = merged[outer]
        if outer_paths is not None:
            inner = shares[root]
            outer_paths.update({str(root)} if inner is None else inner)
    return merged


def _outermost(shown: dict[Path, set[str]]) -> dict[Path, set[str]]:
    """*shown* with each share inside another folded into it: one mount, whose allowlist holds both."""
    merged: dict[Path, set[str]] = {}
    for root in sorted(shown, key=lambda r: len(r.parts)):
        outer = next((r for r in merged if root.is_relative_to(r)), None)
        merged.setdefault(outer or root, set()).update(shown[root])
    return merged


def returncode(top: Path) -> int | None:
    """How the helper ended, as `Popen.returncode` says it (a signal is negative), or `None` when the guest wrote
    nothing: it stopped before the helper did."""
    try:
        how, _, number = (top / EXIT_NAME).read_text().partition(" ")
    except FileNotFoundError:
        return None
    code = int(number)
    return -code if how == "signal" else code


def status(top: Path) -> str:
    """What the helper wrote to its file descriptor 3: an error that stopped the setup, or nothing."""
    try:
        return (top / STATUS_NAME).read_bytes().decode(errors="surrogateescape")
    except FileNotFoundError:
        return ""
