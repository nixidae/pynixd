"""What pynixd keeps of a client's `SetOptions`. nixidae/nixidae#1198.

Three filters, in this order:

1. Every transport: a `pynixd-` name is pynixd's own. It goes to the session's `pynixd_options` and never
   upstream. huggorm's Nix sends such names (`nix-set-options-by-name.patch`).
2. A client that is not trusted keeps what Nix lets it keep (`restrict_overrides`).
3. A client on another machine that names `set-options-by-name`: only the names in `remote_settings`. Every
   setting it overrides travels by name, its user nix.conf included, and a delegating client's `max-jobs = 0` must not
   stop this daemon. A remote client without the feature is a pynixd controller of this builder: it sends the
   header alone, with `max-jobs` already set for a builder (`execution.builder_options`), and keeps it all.

The fixed header cannot say "not set". Under `set-options-by-name` a header setting that the client overrides
travels by name too. A local client's header applies as Nix applies it; a remote client's header is read by
name alone, over `DaemonHeader`, the values this daemon has with no `SetOptions` at all.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from .command_options import PREFIX, PynixdOption, Refusal, accept
from .serde import SetOptionsRequest, TimeSpan
from .serde.auth import Role
from .trust import restrict_overrides

if TYPE_CHECKING:
    from collections.abc import Mapping

    from .trust import TrustPolicy


@dataclass(frozen=True, slots=True)
class DaemonHeader:
    """The header settings of the daemon's own configuration. Nix's defaults here."""

    keep_failed: bool = False
    keep_going: bool = False
    try_fallback: bool = False
    max_build_jobs: int = 1
    max_silent_time: int = 0
    build_cores: int = 0
    use_substitutes: bool = True

    @classmethod
    def from_nix_config(cls, config: Mapping[str, Any]) -> DaemonHeader:
        """Read it from `nix config show --json`; a setting it lacks keeps Nix's default."""

        nix = cls()

        def value(name: str, default: Any) -> Any:
            entry = config.get(name)
            return default if entry is None else entry["value"]

        return cls(
            keep_failed=bool(value("keep-failed", nix.keep_failed)),
            keep_going=bool(value("keep-going", nix.keep_going)),
            try_fallback=bool(value("fallback", nix.try_fallback)),
            max_build_jobs=int(value("max-jobs", nix.max_build_jobs)),
            max_silent_time=int(value("max-silent-time", nix.max_silent_time)),
            build_cores=int(value("cores", nix.build_cores)),
            use_substitutes=bool(value("substitute", nix.use_substitutes)),
        )


def _flag(text: str) -> bool:
    if text not in ("true", "false"):
        raise ValueError(f"'{text}' is not a Boolean")
    return text == "true"


def _jobs(text: str) -> int:
    return (os.cpu_count() or 1) if text == "auto" else int(text)


HEADER_SETTINGS: Final[dict[str, tuple[str, Any]]] = {
    "keep-failed": ("keep_failed", _flag),
    "keep-going": ("keep_going", _flag),
    "fallback": ("try_fallback", _flag),
    "max-jobs": ("max_build_jobs", _jobs),
    "max-silent-time": ("max_silent_time", lambda text: TimeSpan(int(text))),
    "cores": ("build_cores", int),
    "substitute": ("use_substitutes", _flag),
}
"""The settings the fixed header carries (`remote-store.cc:120-125`): the field and the parser of each."""


@dataclass(frozen=True, slots=True)
class SessionOptions:
    options: SetOptionsRequest | None
    """What the session's operations read, and what goes upstream; `None` when a remote client keeps nothing."""
    pynixd_options: dict[PynixdOption, str] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()
    """For the client, at its verbosity."""
    dropped: tuple[str, ...] = ()
    """A remote client's names outside `remote_settings`: for the log, not the client, which sends its nix.conf."""


def session_options(
    request: SetOptionsRequest,
    *,
    role: Role,
    remote: bool,
    policy: TrustPolicy,
    remote_settings: frozenset[str],
    daemon: DaemonHeader,
) -> SessionOptions:
    """What a session keeps of *request*. Pure: the handler merges it into the session."""
    pynixd: dict[PynixdOption, str] = {}
    warnings: list[str] = []
    overrides: dict[str, str] = {}
    for name, value in request.overrides.items():
        if not name.startswith(PREFIX):
            overrides[name] = value
            continue
        option = accept(name, role)
        if isinstance(option, Refusal):
            warnings.append(option.warning)
        else:
            pynixd[option] = value
    # By name, besides the header, only under `set-options-by-name`. Nix applies the header to every client,
    # untrusted ones included, so these skip the trust filter.
    header = {name: overrides.pop(name) for name in HEADER_SETTINGS if name in overrides}
    if role < Role.ADMIN:
        overrides, refused = restrict_overrides(overrides, policy)
        warnings.extend(refused)
    if not remote:
        return SessionOptions(request.model_copy(update={"overrides": overrides}), pynixd, tuple(warnings))

    dropped = sorted(name for name in (*header, *overrides) if name not in remote_settings)
    kept = {name: value for name, value in overrides.items() if name in remote_settings}
    fields: dict[str, Any] = {}
    for name, value in header.items():
        if name not in remote_settings:
            continue
        attribute, parse = HEADER_SETTINGS[name]
        try:
            fields[attribute] = parse(value)
        except ValueError as exc:
            warnings.append(f"ignoring the setting '{name}': {exc}")
    if not kept and not fields:
        return SessionOptions(None, pynixd, tuple(warnings), tuple(dropped))
    options = SetOptionsRequest(
        keep_failed=daemon.keep_failed,
        keep_going=daemon.keep_going,
        try_fallback=daemon.try_fallback,
        verbosity=request.verbosity,
        max_build_jobs=daemon.max_build_jobs,
        max_silent_time=TimeSpan(daemon.max_silent_time),
        obsolete_use_build_hook=request.obsolete_use_build_hook,
        build_verbosity=request.build_verbosity,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=daemon.build_cores,
        use_substitutes=daemon.use_substitutes,
        overrides=kept,
    ).model_copy(update=fields)
    return SessionOptions(options, pynixd, tuple(warnings), tuple(dropped))
