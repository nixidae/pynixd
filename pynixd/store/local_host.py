"""LocalHost: what a local store knows of itself and its machine, with a daemon or without one.

`LocalStore` adds a Nix daemon to it, and `NoDaemonStore` (the `daemonless`
mode, `pynixd/store_mode.py`) adds none. Both read nix.conf in the store's
environment, sample the machine, and read a derivation from its file.
nixidae/nixidae#1325.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from nix_daemon_protocol.store_dir import on_disk
from pynixd_store.nix_config import nix_environment, read_nix_config
from pynixd_store.store_path import StorePath

from ..agent import local_monitor
from ..config import LocalSocketStoreSpec, PynixdSettings
from ..monitor import DummyResourceMonitor, local_disk
from ..trust import TrustPolicy
from .base import get_current_system
from .scheduled import ScheduledStore

if TYPE_CHECKING:
    from pynixd_store.drv_parser import Derivation
    from pynixd_store.store_layout import StoreLayout

    from ..monitor import ResourceMonitor

__all__ = ["LocalHost", "feature_matrix_from_nix_config"]

log = structlog.get_logger(__name__)


def feature_matrix_from_nix_config(config: dict[str, Any]) -> dict[str, set[str]]:
    """What a daemon builds, from the output of `nix config show --json`.

    The daemon decides with these settings whether it can build a
    derivation itself: `system` and `extra-platforms` for the platform,
    `system-features` for every one of them.
    """
    systems = {config["system"]["value"], *config["extra-platforms"]["value"]}
    features = set(config["system-features"]["value"])
    return {system: set(features) for system in systems}


def _sandbox_fallback_arguments(layout: StoreLayout) -> list[str]:
    """Refuse the silent sandbox fallback for a store Nix has diverted.

    **A diverted store that falls back to no sandbox cannot build anything.**
    nix 2.34.8 `derivation-builder.cc:2111` forces the sandbox on when
    `storeDir != realStoreDir`, because only the chroot puts the real
    directory at the store path the builder writes to. Twelve lines later, at
    `:2120`, a system without mount and PID namespaces turns it back off with
    `debug()` and `sandbox-fallback` left at its default of true. The builder
    then writes to `/nix/store/<hash>-<name>`, which is the store of the
    machine, and Nix looks for the output under the real directory and finds
    nothing.

    That is how a GitHub runner answered `failed to produce output path` for
    `echo x86_64-linux > $out`. The runner blocks unprivileged user
    namespaces, so `mountAndPidNamespacesSupported()` is false there and true
    on every developer machine. The fallback is right for a store at
    `/nix/store` and wrong for every store pynixd relocates, so it goes off
    here and stays on there. Issue #47.
    """
    if layout.store_dir == layout.real_store_dir:
        return []
    return ["--option", "sandbox-fallback", "false"]


class LocalHost(ScheduledStore):
    """A store on this machine: its layout, its nix.conf, its machine's samples, its derivation files."""

    def __init__(self, spec: LocalSocketStoreSpec) -> None:
        super().__init__(spec)
        if spec.mode.has_daemon is not self.runs_daemon:
            raise ValueError(f"{type(self).__name__} cannot serve a store whose mode is {spec.mode.answers}")
        # A relocated store is always ours to start: `NIX_STORE_DIR` names a
        # store that no daemon of the machine serves.
        #
        # `spec.managed` overrides that, because the inference reads a store at
        # `/` as the store of the machine and a container can hold its own.
        # See `LocalSocketStoreSpec.managed`.
        self.managed = (
            spec.managed if spec.managed is not None else (self.layout.relocated or self.store_path != Path("/"))
        )
        self.monitor_enabled = spec.monitor
        self.nix_config = spec.nix_config
        self.extra_env = spec.extra_env or {}
        self._trust_policy: TrustPolicy | None = None
        self.extra_args = spec.extra_args or []
        self.settings = spec.settings or PynixdSettings()
        self._probe_requested = spec.probe

        monitor: ResourceMonitor = (
            local_monitor(
                self.pressure_settings, self.on_pressure_sample, [get_current_system()], self._replace_monitor
            )
            if self.monitor_enabled
            else DummyResourceMonitor(self.pressure_settings, self.on_pressure_sample)
        )
        monitor.disk_fn = local_disk(self.layout.real_store_dir)
        self.monitor = monitor

    def _replace_monitor(self, monitor: ResourceMonitor) -> None:
        monitor.disk_fn = local_disk(self.layout.real_store_dir)
        self.monitor = monitor
        monitor.start(self._background)

    async def start(self, sync_paths: bool = True) -> None:
        await self._prepare()
        if self._features_from_config and self._feature_matrix is None and self._probe_requested is None:
            # The machine's own daemon, or none: nix.conf says what it builds.
            # Probing it with builds waits for free build slots before
            # READY=1, and missed systemd's start timeout on a busy machine
            # (#58).
            self._feature_matrix = feature_matrix_from_nix_config(await self.read_nix_config())
            self._probe = False
            log.info(
                "store_features_from_config",
                store_id=self.store_id,
                feature_matrix={k: sorted(v) for k, v in self._feature_matrix.items()},
            )
        await super().start(sync_paths=sync_paths)

    async def _prepare(self) -> None:
        """What `start` needs before the shared part."""

    @property
    def _features_from_config(self) -> bool:
        """Whether nix.conf, not a probe, says what the store builds."""
        return True

    def config_arguments(self) -> list[str]:
        """What the managed daemon's command line sets of its settings; none for a daemon pynixd did not start."""
        if not self.managed:
            return []
        return [
            *self.layout.daemon_arguments(),
            "--option",
            "build-dir",
            str(self.layout.build_dir),
            *_sandbox_fallback_arguments(self.layout),
            *self.extra_args,
            *self.layout.daemon_features(),
        ]

    async def trust_policy(self) -> TrustPolicy:
        """`trusted-users`, `allowed-users` and `build-users-group`, as this store's Nix reads them.

        Read once: nix-daemon reads its configuration once, at start, too.
        """
        if self._trust_policy is None:
            self._trust_policy = TrustPolicy.from_nix_config(await self.read_nix_config())
        return self._trust_policy

    async def read_nix_config(self) -> dict[str, Any]:
        """`nix config show --json` in the daemon's environment, so includes,
        `extra-` settings and `NIX_CONFIG` resolve as they do for nix-daemon."""
        return await read_nix_config(self.nix_bin, nix_environment(self.layout, self.extra_env))

    async def read_derivation(self, drv_store_path: StorePath | str) -> Derivation | None:
        """Fast-path: read .drv file directly from the filesystem."""
        from pynixd_store.drv_parser import parse_drv_bytes

        # `on_disk` and not `self.store_path / "nix" / "store" / ...`.
        # `real_store_dir()` is where the files of the store are, and a chroot
        # store puts them somewhere other than the directory a store path
        # names. See `read_drv_file` in `drv_parser.py`.
        drv_file = Path(on_disk(str(StorePath(str(drv_store_path)))))
        try:
            contents = drv_file.read_bytes()
        except (FileNotFoundError, OSError):
            return await self._read_derivation_without_file(drv_store_path)

        return await parse_drv_bytes(contents)

    async def _read_derivation_without_file(self, drv_store_path: StorePath | str) -> Derivation | None:
        """A derivation whose file is not there: nothing else holds it."""
        return None
