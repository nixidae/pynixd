"""pynixd's own settings on the SSH command line of an `ssh-ng://` client.

Nix's `SSHStore::setOptions` sends no `SetOptions` at all (`ssh-store.cc:92-99`), so an `ssh-ng://` client cannot
set an option on its daemon. It does choose the command: `remote-program` is split on whitespace and `--stdio`
follows it (`ssh-store.cc:227-233`). A client writes
`ssh-ng://host?remote-program=nix-daemon%20--option%20pynixd-pool%20big`, and pynixd's SSH server reads the
`--option` pairs here.

Only `pynixd-` names count. A Nix setting here would never reach an upstream daemon, and pynixd would read it in
some places only. Each one dropped gets a warning on the SSH channel's stderr, which the client prints.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass, field
from enum import StrEnum

from .serde.auth import Role

PREFIX = "pynixd-"
"""huggorm's Nix sends an unregistered setting with this prefix in `SetOptions` (`nix-set-options-by-name.patch`)."""


class PynixdOption(StrEnum):
    POOL = "pynixd-pool"
    """Comma-separated provisioning pools the session's builds run on, as `pynixd-pool://NAME` in `builders`."""


TRUSTED_ONLY: frozenset[PynixdOption] = frozenset({PynixdOption.POOL})
"""Settings an untrusted client may not set: steering to a pool spends money, as `builders` does in Nix."""


@dataclass(frozen=True, slots=True)
class CommandOptions:
    options: dict[PynixdOption, str] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()


def parse_command(command: str, role: Role) -> CommandOptions:
    """The `--option NAME VALUE` pairs of *command* that a client of *role* may set, and a warning for each other."""
    try:
        words = shlex.split(command)
    except ValueError as exc:
        return CommandOptions(warnings=(f"ignoring the SSH command's settings: {exc}",))
    options: dict[PynixdOption, str] = {}
    warnings: list[str] = []
    i = 0
    while i < len(words):
        if words[i] != "--option":
            i += 1
            continue
        if i + 2 >= len(words):
            warnings.append("ignoring '--option' at the end of the SSH command: it needs a name and a value")
            break
        name, value = words[i + 1], words[i + 2]
        i += 3
        if not name.startswith(PREFIX):
            warnings.append(f"ignoring the setting '{name}' on the SSH command: pynixd reads only its own there")
            continue
        option = accept(name, role)
        if isinstance(option, Refusal):
            warnings.append(option.warning)
            continue
        options[option] = value
    return CommandOptions(options, tuple(warnings))


@dataclass(frozen=True, slots=True)
class Refusal:
    warning: str


def accept(name: str, role: Role) -> PynixdOption | Refusal:
    """The setting *name* names when a client of *role* may set it, or the warning that drops it."""
    try:
        option = PynixdOption(name)
    except ValueError:
        return Refusal(f"unknown setting '{name}'")
    if option in TRUSTED_ONLY and role < Role.ADMIN:
        # Nix's words for a restricted setting, `daemon.cc:292`.
        return Refusal(
            f"ignoring the client-specified setting '{name}', because it is a restricted setting and you are not"
            " a trusted user"
        )
    return option
