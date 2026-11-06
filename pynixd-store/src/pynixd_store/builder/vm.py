"""A build in a libkrun microVM: `pynixd-krun` boots a guest that runs the sandbox helper (nixidae/nixidae#1186).

The guest mounts each share at its host path, so the helper's spec means the same inside. The stores are
read-only, each with an allowlist: the build store shows its input closure, and the host's store shows the
closure the helper runs from. A builder that escapes the helper's namespaces sees no other store path. The
build's two directories are writable shares: the top directory (the spec, the build directory) and the chroot,
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
        the helper binds (`sandbox-paths`), each inside one of the two stores (`unshared`). The helper reads
        *spec_file* and gets `top/vm-status` as its file descriptor 3.
        """
        build_store = {str(real_store_dir / path.rsplit("/", 1)[-1]) for path in visible}
        # The chroot is a mount point inside the build store's share, so that share must show it.
        build_store.add(str(chroot_parent))
        stores: dict[Path, set[str]] = {real_store_dir: build_store}
        stores.setdefault(HOST_STORE, set()).update(self.helper_closure)
        for source in sources:
            root = _store_of(Path(source), real_store_dir)
            if root is None:
                raise ValueError(f"'{source}' is in no store the build VM shares")
            stores[root].add(str(root / Path(source).relative_to(root).parts[0]))
        argv = [
            str(self.launcher),
            f"--init={self.init}",
            f"--cpus={self.cpus}",
            f"--memory={self.memory_mib}",
            f"--dax-window={self.dax_window_mib * 1024 * 1024}",
        ]
        # A share inside another mounts after it.
        for i, (root, paths) in enumerate(sorted(stores.items(), key=lambda item: len(item[0].parts))):
            allowlist = top / f"vm-allowlist-{i}"
            allowlist.write_text("".join(f"{p}\n" for p in sorted(paths)))
            argv.append(f"--ro={root}:{allowlist}")
        argv += [
            f"--rw={chroot_parent}",
            f"--rw={top}",
            f"--status={top / STATUS_NAME}",
            f"--exit={top / EXIT_NAME}",
            "--",
            str(self.python),
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


def unshared(sources: Iterable[str], real_store_dir: Path) -> str | None:
    """The first host path of *sources* that no share of the guest holds, or `None`.

    The guest sees the two stores and nothing else of the host, so a `sandbox-paths` entry from outside them
    cannot reach the helper.
    """
    return next((s for s in sources if _store_of(Path(s), real_store_dir) is None), None)


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
