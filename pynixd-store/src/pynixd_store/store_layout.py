"""Where a local store keeps its paths and its state.

**Nix has two ways to put a store somewhere other than `/nix/store`, and they
are not the same way.** pynixd served one of them until issue nixidae/nixidae#215.

- **A chroot store.** `--store <root>` puts the files at `<root>/nix/store`
  and the state at `<root>/nix/var/nix`. It moves no store path:
  `builtins.storeDir` still answers `/nix/store`, and so does the text in
  front of the hash of every path on the wire. Measured against Nix 2.34.8.
- **A relocated store.** `NIX_STORE_DIR` moves the store path itself, so the
  logical directory and the directory on disk are one. `NIX_STATE_DIR` moves
  the state, and the two are independent in Nix.

`StoreLayout` holds the three directories that follow from the choice, and it
is the one place that answers them. Four readers had the layout written into
them before: `ensure_daemon` built the argument of the daemon, `_adopt_store_dir`
set the real directory, `resolve_db_path` built the path of `db.sqlite`, and
`temp_roots.state_dir` built the path of `temproots`. A relocated store breaks
each one, and each one broke in its own way.

`nix_daemon_protocol.store_dir` keeps the process-wide values that a codec
reads. This module decides what to put in them, and it also answers the state
directory, which no codec needs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DEFAULT_STORE_DIR = Path("/nix/store")
DEFAULT_STATE_DIR = Path("/nix/var/nix")
DEFAULT_LOG_DIR = Path("/nix/var/log/nix")
"""Nix's compiled `logDir` fallback (`store-api.cc:1271`), for a relocated store with no `NIX_LOG_DIR`."""


@dataclass(frozen=True)
class StoreLayout:
    """The three directories of one local store."""

    store_dir: Path
    """The directory in a store path. `builtins.storeDir` answers this."""

    real_store_dir: Path
    """The directory on the file system that holds the store paths."""

    state_dir: Path
    """The directory that holds `db/`, `temproots/` and `gc.lock`."""

    log_dir: Path
    """Nix's `logDir`: where `addBuildLog` writes `drvs/`. A sibling of the state, not under it.

    `local-fs-store.hh:54` sets it to `<root>/nix/var/log/nix` for a chroot
    store. A relocated store reads `NIX_LOG_DIR`, independent of the state, so
    the spec carries it (`store-api.cc:1271`).
    """

    relocated: bool
    """True when `NIX_STORE_DIR` moved the store, and not `--store <root>`.

    The two shapes need different arguments for a managed daemon, and this is
    the one thing that a caller cannot work out from the three directories: a
    chroot store of `/` and a relocated store of `/nix/store` name the same
    three.
    """

    @classmethod
    def chroot(cls, root: Path | None) -> StoreLayout:
        """The layout of `nix daemon --store <root>`.

        `local-fs-store.hh:54-70` of Nix builds `<root>/nix/store` and
        `<root>/nix/var/nix` from the root, and it reads no environment name
        for either. A root of `/` is the ordinary store of the machine.
        """
        if root is None or Path(root) == Path("/"):
            return cls(
                store_dir=DEFAULT_STORE_DIR,
                real_store_dir=DEFAULT_STORE_DIR,
                state_dir=DEFAULT_STATE_DIR,
                log_dir=DEFAULT_LOG_DIR,
                relocated=False,
            )
        root = Path(root)
        return cls(
            store_dir=DEFAULT_STORE_DIR,
            real_store_dir=root / "nix" / "store",
            state_dir=root / "nix" / "var" / "nix",
            log_dir=root / "nix" / "var" / "log" / "nix",
            relocated=False,
        )

    @classmethod
    def relocated_store(cls, store_dir: Path, state_dir: Path, log_dir: Path | None = None) -> StoreLayout:
        """The layout of `NIX_STORE_DIR=<dir> NIX_STATE_DIR=<state> NIX_LOG_DIR=<log>`.

        The three directories are independent in Nix, and none gives another.
        So this takes all of them, and `LocalSocketStoreSpec` refuses a
        relocated store that names the store alone. A default of `/nix/var/nix`
        for the state would put the temporary roots and the database of a test
        store in the store of the machine. *log_dir* left out is Nix's compiled
        `logDir` fallback, which the config never relies on: it carries the
        store's `NIX_LOG_DIR`.
        """
        store_dir = Path(store_dir)
        return cls(
            store_dir=store_dir,
            real_store_dir=store_dir,
            state_dir=Path(state_dir),
            log_dir=Path(log_dir) if log_dir is not None else DEFAULT_LOG_DIR,
            relocated=True,
        )

    @property
    def db_path(self) -> Path:
        """The `db.sqlite` of this store."""
        return self.state_dir / "db" / "db.sqlite"

    @property
    def links_dir(self) -> Path:
        """Where `optimiseStore` keeps one hard link per unique file content.

        `local-store.cc:128`: `realStoreDir / ".links"`, a sibling of the store
        paths on the same file system, so a hard link into it is possible. It
        follows `real_store_dir`, which a chroot store moves while `store_dir`
        stays `/nix/store`.
        """
        return self.real_store_dir / ".links"

    def socket_path(self, name: str) -> Path:
        """Where a managed daemon of this store puts its socket.

        `$NIX_STATE_DIR/daemon-socket/<name>`, which is where Nix puts its
        own. A chroot store of `<root>` answers
        `<root>/nix/var/nix/daemon-socket/<name>`, which is the path that
        pynixd used before it had a layout.
        """
        return self.state_dir / "daemon-socket" / name

    @property
    def build_dir(self) -> Path:
        """The `build-dir` that a managed daemon of this store uses."""
        return self.state_dir / "builds"

    def daemon_arguments(self) -> list[str]:
        """What `nix daemon` needs on its command line to serve this store.

        A relocated store gets nothing here, because `--store <dir>` would
        read the directory as a chroot root and make `<dir>/nix/store`.
        `daemon_environment` carries that shape instead.
        """
        if self.relocated:
            return []
        root = self.chroot_root()
        return ["--store", str(root)]

    def daemon_environment(self) -> dict[str, str]:
        """What `nix daemon` needs in its environment to serve this store.

        A chroot store gets nothing here. Issue nixidae/nixidae#210 measured that Nix reads
        no `NIX_STATE_DIR` when `--store <root>` gives it a root, so a name
        set here would say one thing and the daemon would do another.
        """
        if not self.relocated:
            return {}
        return {
            "NIX_STORE_DIR": str(self.store_dir),
            "NIX_STATE_DIR": str(self.state_dir),
            # `logDir` is independent of the state, so the daemon that writes a
            # build trace beside pynixd must hear it too, or the two disagree.
            "NIX_LOG_DIR": str(self.log_dir),
        }

    def chroot_root(self) -> Path:
        """The root that `--store` takes, for a chroot store."""
        if self.relocated:
            raise ValueError("a relocated store has no chroot root")
        if self.real_store_dir == DEFAULT_STORE_DIR:
            return Path("/")
        return self.real_store_dir.parent.parent
