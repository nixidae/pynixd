"""What a local build reads of Nix's settings, from `nix config show --json` and the session's overrides."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


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
    build_dir: str | None
    ca_derivations: bool

    @classmethod
    def from_nix_config(cls, config: Mapping[str, Any], overrides: Mapping[str, str]) -> BuildSettings:
        def value(name: str, default: Any = None) -> Any:
            if name in overrides:
                return overrides[name]
            entry = config.get(name)
            return entry["value"] if entry is not None else default

        def words(raw: Any) -> list[str]:
            return raw.split() if isinstance(raw, str) else list(raw or ())

        def boolean(raw: Any) -> bool:
            return raw in (True, "true", "1")

        sandbox = value("sandbox", True)
        cores = int(value("cores", 0) or 0)
        return cls(
            system=str(value("system")),
            extra_platforms=frozenset(words(value("extra-platforms", ()))),
            sandbox=sandbox if isinstance(sandbox, str) else ("true" if sandbox else "false"),
            sandbox_paths=SandboxPath.parse_all(value("sandbox-paths", ())),
            build_cores=cores or os.cpu_count() or 1,
            max_silent_time=int(value("max-silent-time", 0) or 0),
            timeout=int(value("timeout", 0) or 0),
            sandbox_shm_size=str(value("sandbox-dev-shm-size", "50%")),
            allow_new_privileges=boolean(value("allow-new-privileges", False)),
            build_dir=value("build-dir") or None,
            ca_derivations="ca-derivations" in words(value("experimental-features", ())),
        )
