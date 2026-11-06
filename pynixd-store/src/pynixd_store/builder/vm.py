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
        the helper binds (`sandbox-paths`): one in a store shows its store path, and a directory outside both
        stores is a writable share of its own, as Nix binds it read-write and the host's permissions decide
        (`programs.ccache`'s cache). A file outside them cannot be a share (`unshareable`). The helper reads
        *spec_file* and gets `top/vm-status` as its file descriptor 3.
        """
        shown: dict[Path, set[str]] = {
            real_store_dir: {str(real_store_dir / path.rsplit("/", 1)[-1]) for path in visible},
        }
        shown.setdefault(HOST_STORE, set()).update(self.helper_closure)
        directories: set[Path] = set()
        for source in map(Path, sources):
            store = _store_of(source, real_store_dir)
            if store is not None:
                shown[store].add(str(store / source.relative_to(store).parts[0]))
            # A missing source fails in the helper, as it does on the host.
            elif source.is_dir():
                directories.add(source)
        ro = _outermost(shown)
        # A directory inside another is already in its share.
        extra = [d for d in directories if not any(d != o and d.is_relative_to(o) for o in directories)]
        rw = [chroot_parent, top, *sorted(extra)]
        # A share that mounts inside a read-only one needs its mount point shown there.
        for inner in rw:
            for root, paths in ro.items():
                if inner.is_relative_to(root):
                    paths.add(str(inner))
        argv = [
            str(self.launcher),
            f"--init={self.init}",
            f"--cpus={self.cpus}",
            f"--memory={self.memory_mib}",
            f"--dax-window={self.dax_window_mib * 1024 * 1024}",
        ]
        # A share inside another mounts after it; at one depth, the read-only ones first.
        shares = [(root, sorted(paths)) for root, paths in ro.items()] + [(root, None) for root in rw]
        for i, (root, allowed) in enumerate(sorted(shares, key=lambda share: (len(share[0].parts), share[1] is None))):
            if allowed is None:
                argv.append(f"--rw={root}")
                continue
            allowlist = top / f"vm-allowlist-{i}"
            allowlist.write_text("".join(f"{p}\n" for p in allowed))
            argv.append(f"--ro={root}:{allowlist}")
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
    """The first source that is a file outside both stores, or `None`.

    virtio-fs mounts only a directory: the kernel fixes the root's mode at `S_IFDIR` (`fs/fuse/virtio_fs.c`,
    `virtio_fs_ctx_set_defaults`) and takes no `rootmode=`. A writable share of the file's parent would show
    its siblings writable, and libkrun's allowlist filters only a read-only share (nixidae/nixidae#1186).
    """
    return next(
        (s for s in sources if _store_of(Path(s), real_store_dir) is None and Path(s).is_file()),
        None,
    )


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
