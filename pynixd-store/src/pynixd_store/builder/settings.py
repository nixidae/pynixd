"""What a local build reads of Nix's settings, from `nix config show --json` and the session's overrides."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import structlog

from ..signing import key_settings

_log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SandboxPath:
    """One entry of `sandbox-paths`: `target=source`, and optional with a trailing `?`."""

    target: str
    source: str
    optional: bool

    @classmethod
    def parse(cls, entry: str) -> SandboxPath:
        optional = entry.endswith("?")
        entry = entry.removesuffix("?")
        target, sep, source = entry.partition("=")
        return cls(target, source if sep else target, optional)

    @classmethod
    def parse_all(cls, raw: Any) -> tuple[SandboxPath, ...]:
        """`sandbox-paths` from an override (words) or from `nix config show --json`.

        The JSON is a map of target to `{"source", "optional"}` (`PathsInChroot`).
        Reading it as a list of words takes the targets alone, and binds the
        host's `/bin/sh` where Nix binds its busybox.
        """
        if isinstance(raw, Mapping):
            return tuple(cls(target, e["source"], bool(e.get("optional"))) for target, e in raw.items())
        words = raw.split() if isinstance(raw, str) else list(raw or ())
        return tuple(cls.parse(entry) for entry in words)


@dataclass(frozen=True, slots=True)
class NotBuiltHere:
    """Why pynixd's builder leaves a build to the store's daemon."""

    reason: str


@dataclass(frozen=True, slots=True)
class ExternalBuilder:
    """One entry of `external-builders` (`ExternalBuilder`, `derivation-builder.hh:199`)."""

    systems: frozenset[str]
    program: str
    args: tuple[str, ...]

    @classmethod
    def parse_all(cls, raw: Any) -> tuple[ExternalBuilder, ...]:
        """A list from `nix config show --json`, or its JSON text in an override.

        `from_json` (`derivation-builder.cc:8`) needs every key; a missing one raises `ValueError`.
        """
        entries = json.loads(raw) if isinstance(raw, str) else raw
        if not entries:
            return ()
        if not isinstance(entries, list):
            raise ValueError(f"expected a list of external builders, not {type(entries).__name__}")
        try:
            return tuple(
                cls(frozenset(e["systems"]), str(e["program"]), tuple(str(a) for a in e["args"])) for e in entries
            )
        except (KeyError, TypeError) as e:
            raise ValueError(f"an external builder needs 'systems', 'program' and 'args': {e}") from e


@dataclass(frozen=True)
class TransferSettings:
    """`FileTransferSettings` (`filetransfer.hh:30`) and `hashed-mirrors`, which `builtin:fetchurl` reads."""

    netrc_file: str = "/etc/nix/netrc"
    ssl_cert_file: str | None = None
    hashed_mirrors: tuple[str, ...] = ()
    http2: bool = True
    http3: bool = False
    user_agent_suffix: str = ""
    connect_timeout: int = 15
    stalled_download_timeout: int = 300
    download_speed: int = 0
    tries: int = 5
    retry_delay_ms: int = 100
    retry_delay_rate_limited_ms: int = 5000
    retry_max_delay_ms: int = 60000
    retry_jitter: bool = True


@dataclass(frozen=True)
class BuildSettings:
    system: str
    extra_platforms: frozenset[str]
    sandbox: str
    """`true`, `false` or `relaxed`."""
    sandbox_paths: tuple[SandboxPath, ...]
    build_cores: int
    max_silent_time: int
    timeout: int
    sandbox_shm_size: str
    allow_new_privileges: bool
    filter_syscalls: bool
    secret_key_files: tuple[str, ...]
    auto_optimise_store: bool
    build_dir: str | None
    ca_derivations: bool
    git_hashing: bool
    recursive_nix: bool
    dynamic_derivations: bool
    """`builder-rpc-v0` needs it (`startDaemon`, `unix-derivation-builder.cc:675`)."""
    impure_env: Mapping[str, str]
    """`impure-env`, which a fixed-output build reads before the process environment."""
    fsync_store_paths: bool = False
    keep_failed: bool = False
    post_build_hook: str = ""
    diff_hook: str = ""
    """`diff-hook`, which a check runs only with `run-diff-hook` (`local-settings.hh:551`); empty otherwise."""
    log_lines: int = 25
    keep_build_log: bool = True
    nix_command: bool = True
    transfer: TransferSettings = TransferSettings()
    nix_version: str = ""
    build_users_group: str = ""
    """The store's own: `useBuildUsers` (`user-lock.cc:237`) reads it once, so a client's never takes effect."""
    auto_allocate_uids: bool = False
    auto_allocate_feature: bool = False
    """`Xp::AutoAllocateUids`, which `AutoUserLock::acquire` requires (`user-lock.cc:176`)."""
    start_id: int = 0x34000000
    id_count: int = 128 << 16
    cgroups_feature: bool = False
    """`Xp::Cgroups`, which a build of 65536 ids requires (`linux-derivation-builder.cc:450`)."""
    session: tuple[tuple[str, str], ...] = ()
    """The session's settings in the order `ClientSettings::apply` sets them, which `NIX_CONFIG` shows."""
    max_jobs: int = 1
    system_features: frozenset[str] = frozenset()
    """`Store::Config::systemFeatures` (`store-api.cc:381`): the store's own, read when it opens."""
    builders: str = ""
    build_hook: tuple[str, ...] = ()
    external_builders: tuple[ExternalBuilder, ...] = ()
    external_builders_feature: bool = False
    """`Xp::ExternalBuilders`, which `ExternalDerivationBuilder` requires (`external-derivation-builder.cc:19`)."""

    @classmethod
    def from_nix_config(
        cls, config: Mapping[str, Any], overrides: Mapping[str, str], *, nix_version: str = ""
    ) -> BuildSettings:
        def value(name: str, default: Any = None, alias: str | None = None) -> Any:
            if name in overrides:
                return overrides[name]
            if alias is not None and alias in overrides:
                return overrides[alias]
            entry = config.get(name)
            return entry["value"] if entry is not None else default

        def words(raw: Any) -> list[str]:
            return raw.split() if isinstance(raw, str) else list(raw or ())

        def boolean(raw: Any) -> bool:
            return raw in (True, "true", "1")

        def sandbox_paths() -> tuple[SandboxPath, ...]:
            """`ClientSettings::apply` walks the overrides sorted, so `extra-sandbox-paths` comes before
            `sandbox-paths` and that one replaces it. An appended target that exists keeps its source
            (`appendOrSet`, `globals.cc:448`)."""
            paths = SandboxPath.parse_all(value("sandbox-paths", ()))
            if "sandbox-paths" in overrides:
                return paths
            targets = {p.target for p in paths}
            extra = SandboxPath.parse_all(overrides.get("extra-sandbox-paths", ()))
            return paths + tuple(p for p in extra if p.target not in targets)

        def secret_key_files() -> tuple[str, ...]:
            configured = config.get("secret-key-files")
            _trusted, files = key_settings((), words(configured["value"] if configured else ()), overrides)
            return tuple(files)

        def string_map(raw: Any) -> dict[str, str]:
            """`Setting<StringMap>::parse` (`config.cc`): words of `name=value`."""
            if isinstance(raw, Mapping):
                return {str(k): str(v) for k, v in raw.items()}
            return dict(word.split("=", 1) for word in words(raw) if "=" in word)

        def impure_env() -> dict[str, str]:
            """`extra-impure-env` comes first in the sorted overrides, and `impure-env` replaces it.

            `appendOrSet` (`configuration.cc:474`) inserts into the map, so a name already set keeps its value.
            """
            env = string_map(value("impure-env", {}))
            if "impure-env" not in overrides:
                env = string_map(overrides.get("extra-impure-env", ())) | env
            return env

        def transfer() -> TransferSettings:
            d = TransferSettings()
            return TransferSettings(
                netrc_file=str(value("netrc-file", d.netrc_file)),
                ssl_cert_file=value("ssl-cert-file") or None,
                hashed_mirrors=tuple(words(value("hashed-mirrors", ()))),
                http2=boolean(value("http2", d.http2)),
                http3=boolean(value("http3", d.http3)),
                user_agent_suffix=str(value("user-agent-suffix", d.user_agent_suffix)),
                connect_timeout=int(value("connect-timeout", d.connect_timeout)),
                stalled_download_timeout=int(value("stalled-download-timeout", d.stalled_download_timeout)),
                download_speed=int(value("download-speed", d.download_speed)),
                tries=int(value("filetransfer-retry-attempts", d.tries, alias="download-attempts")),
                retry_delay_ms=int(value("filetransfer-retry-delay", d.retry_delay_ms)),
                retry_delay_rate_limited_ms=int(
                    value("filetransfer-retry-delay-rate-limited", d.retry_delay_rate_limited_ms)
                ),
                retry_max_delay_ms=int(value("filetransfer-retry-max-delay", d.retry_max_delay_ms)),
                retry_jitter=boolean(value("filetransfer-retry-jitter", d.retry_jitter)),
            )

        def system_features() -> frozenset[str]:
            own = set(words(_own(config, "system-features", ())))
            xp = set(words(_own(config, "experimental-features", ())))
            return frozenset(own | {feature for feature in ("ca-derivations", "recursive-nix") if feature in xp})

        def max_jobs() -> int:
            raw = value("max-jobs", 1)
            return (os.cpu_count() or 1) if raw == "auto" else int(raw)

        def external_builders() -> tuple[ExternalBuilder, ...]:
            """A bad override is warned about and ignored, as `ClientSettings::apply` does (`daemon.cc:330`)."""
            if "external-builders" in overrides:
                try:
                    return ExternalBuilder.parse_all(overrides["external-builders"])
                except ValueError as e:
                    _log.warning("external_builders_ignored", error=str(e))
            entry = config.get("external-builders")
            return ExternalBuilder.parse_all(entry["value"]) if entry is not None else ()

        sandbox = value("sandbox", True)
        features = words(value("experimental-features", ()))
        cores = int(value("cores", 0) or 0)
        return cls(
            system=str(value("system")),
            extra_platforms=frozenset(words(value("extra-platforms", ()))),
            sandbox=sandbox if isinstance(sandbox, str) else ("true" if sandbox else "false"),
            sandbox_paths=sandbox_paths(),
            build_cores=cores or os.cpu_count() or 1,
            max_silent_time=int(value("max-silent-time", 0) or 0),
            timeout=int(value("timeout", 0) or 0),
            sandbox_shm_size=str(value("sandbox-dev-shm-size", "50%")),
            allow_new_privileges=boolean(value("allow-new-privileges", False)),
            filter_syscalls=boolean(value("filter-syscalls", True)),
            secret_key_files=secret_key_files(),
            auto_optimise_store=boolean(value("auto-optimise-store", False)),
            build_dir=value("build-dir") or None,
            ca_derivations="ca-derivations" in features,
            git_hashing="git-hashing" in features,
            recursive_nix="recursive-nix" in features,
            dynamic_derivations="dynamic-derivations" in features,
            # The setting carries `Xp::ConfigurableImpureEnv`: without the feature Nix ignores it.
            impure_env=impure_env() if "configurable-impure-env" in features else {},
            fsync_store_paths=boolean(value("fsync-store-paths", False)),
            keep_failed=boolean(value("keep-failed", False)),
            post_build_hook=str(value("post-build-hook", "") or ""),
            diff_hook=str(value("diff-hook", "") or "") if boolean(value("run-diff-hook", False)) else "",
            log_lines=int(value("log-lines", 25)),
            keep_build_log=boolean(value("keep-build-log", True, alias="build-keep-log")),
            nix_command="nix-command" in features,
            transfer=transfer(),
            nix_version=nix_version,
            build_users_group=str(_own(config, "build-users-group", "") or ""),
            auto_allocate_uids=boolean(_own(config, "auto-allocate-uids", False)),
            auto_allocate_feature="auto-allocate-uids" in features,
            start_id=int(_own(config, "start-id", 0x34000000)),
            id_count=int(_own(config, "id-count", 128 << 16)),
            cgroups_feature="cgroups" in features,
            session=tuple(overrides.items()),
            max_jobs=max_jobs(),
            system_features=system_features(),
            builders=str(value("builders", "") or ""),
            build_hook=tuple(words(value("build-hook", ()))),
            external_builders=external_builders(),
            external_builders_feature="external-builders" in features,
        )


def _own(config: Mapping[str, Any], name: str, default: Any) -> Any:
    entry = config.get(name)
    return entry["value"] if entry is not None else default
