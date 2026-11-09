"""Nix's `builders` setting: the machines a client names for its builds.

A port of `Machine::parseConfig` and `Machine::completeStoreReference`
(`src/libstore/machines.cc`), with Nix's error texts.
"""

from __future__ import annotations

import base64
import binascii
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from .store_url import StoreReference

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

_SPECIAL_WORDS: Final = frozenset({"auto", "daemon", "local"})
_UNSIGNED: Final = re.compile(r"\+?[0-9]+")
_FLOAT: Final = re.compile(r"[-+]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][-+]?[0-9]+)?|[-+]?(?:inf|infinity|nan)", re.I)
_UINT_MAX: Final = 2**32 - 1


class MachinesError(ValueError):
    """A `builders` value Nix refuses: `FormatError` or `UsageError` in `machines.cc`."""


@dataclass(frozen=True, slots=True)
class Machine:
    """One line of `builders`: `struct Machine` in `machines.hh`."""

    store_uri: str
    """The store URL, with `ssh://` put in front of a bare host name."""
    systems: frozenset[str]
    ssh_key: str | None = None
    max_jobs: int = 1
    speed_factor: float = 1.0
    supported_features: frozenset[str] = frozenset()
    mandatory_features: frozenset[str] = frozenset()
    ssh_public_host_key: str = ""
    """Base64 of the host key, column 8, as Nix keeps it."""
    params: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))
    """The parameters of `completeStoreReference`: the URL's own, then what the line adds."""

    def system_supported(self, system: str) -> bool:
        """`Machine::systemSupported`."""
        return system == "builtin" or system in self.systems

    def all_supported(self, features: Iterable[str]) -> bool:
        """`Machine::allSupported`."""
        return all(f in self.supported_features or f in self.mandatory_features for f in features)

    def mandatory_met(self, features: Iterable[str]) -> bool:
        """`Machine::mandatoryMet`."""
        wanted = set(features)
        return self.mandatory_features <= wanted

    @property
    def reference(self) -> StoreReference:
        """The store URL in parts, with the parameters of `completeStoreReference`.

        A path is a `local` store at its absolute path, as `StoreReference::parse`
        reads one (`store-reference.cc`, `isNonUriPath`).
        """
        ref = StoreReference.parse(self.store_uri)
        if "://" not in self.store_uri and "/" in self.store_uri.split("?", 1)[0]:
            ref = StoreReference("local", "", os.path.abspath(ref.path), ref.params)
        return StoreReference(ref.scheme, ref.authority, ref.path, MappingProxyType(dict(self.params)))


def _with_scheme(uri: str) -> str:
    """`Machine::Machine`: a schemeless word that is no path and no special store gets `ssh://`."""
    if "://" in uri or "/" in uri or uri.startswith("?"):
        return uri
    if uri in _SPECIAL_WORDS or any(uri.startswith(f"{word}?") for word in _SPECIAL_WORDS):
        return uri
    return f"ssh://{uri}"


def _complete_params(uri: str, machine: Machine) -> dict[str, str]:
    """`Machine::completeStoreReference`."""
    ref = StoreReference.parse(uri)
    params = dict(ref.params)
    scheme = ref.scheme
    if scheme == "ssh":
        params["max-connections"] = "1"
        params["log-fd"] = "4"
    if scheme in ("ssh", "ssh-ng"):
        if machine.ssh_key is not None:
            params["ssh-key"] = machine.ssh_key
        if machine.ssh_public_host_key:
            params["base64-ssh-public-host-key"] = machine.ssh_public_host_key
    features = params.get("system-features", "")
    for feature in [*sorted(machine.supported_features), *sorted(machine.mandatory_features)]:
        features = f"{features} {feature}" if features else feature
    params["system-features"] = features
    return params


def expand_lines(builders: str) -> list[str]:
    """`expandBuilderLines`: one entry per machine, `@file` read in place.

    A file that does not exist names no machine, as Nix ignores it.
    """
    result: list[str] = []
    for raw in builders.split("\n"):
        if not raw:
            continue
        line = raw.split("#", 1)[0]
        for part in line.split(";"):
            entry = part.strip(" \n\r\t")
            if not entry:
                continue
            if entry.startswith("@"):
                path = Path(entry[1:].strip(" \n\r\t"))
                try:
                    text = path.read_text()
                except FileNotFoundError:
                    continue
                result.extend(expand_lines(text))
            else:
                result.append(entry)
    return result


def parse_line(default_systems: frozenset[str], line: str) -> Machine:
    """`parseBuilderLine`."""
    tokens = line.split()

    def is_set(index: int) -> bool:
        return len(tokens) > index and tokens[index] not in ("", "-")

    def unsigned(index: int) -> int:
        token = tokens[index]
        if _UNSIGNED.fullmatch(token) is None or int(token) > _UINT_MAX:
            raise MachinesError(
                f"bad machine specification: failed to convert column #{index} in a row: '{line}' to 'unsigned int'"
            )
        return int(token)

    def floating(index: int) -> float:
        token = tokens[index]
        if _FLOAT.fullmatch(token) is None:
            raise MachinesError(
                f"bad machine specification: failed to convert column #{index} in a row: '{line}' to 'float'"
            )
        return float(token)

    def base64_field(index: int) -> str:
        token = tokens[index]
        try:
            base64.b64decode(token + "=" * (-len(token) % 4), validate=True)
        except binascii.Error as e:
            raise MachinesError(
                f"invalid Base64 string: {e}\n"
                f"while parsing machine specification at a column #{index} in a row: '{line}'"
            ) from e
        return token

    if not is_set(0):
        raise MachinesError(
            f"bad machine specification: store URL was not found at the first column of a row: '{line}'"
        )

    uri = _with_scheme(tokens[0])
    speed = floating(4) if is_set(4) else 1.0
    if speed < 0:
        raise MachinesError("speed factor must be >= 0")
    machine = Machine(
        store_uri=uri,
        systems=frozenset(tokens[1].split(",")) - {""} if is_set(1) else default_systems,
        ssh_key=tokens[2] if is_set(2) else None,
        max_jobs=unsigned(3) if is_set(3) else 1,
        speed_factor=1.0 if speed == 0 else speed,
        supported_features=frozenset(tokens[5].split(",")) - {""} if is_set(5) else frozenset(),
        mandatory_features=frozenset(tokens[6].split(",")) - {""} if is_set(6) else frozenset(),
        ssh_public_host_key=base64_field(7) if is_set(7) else "",
    )
    return replace(machine, params=MappingProxyType(_complete_params(uri, machine)))


def parse_config(default_systems: Iterable[str], builders: str) -> list[Machine]:
    """`Machine::parseConfig`."""
    systems = frozenset(default_systems)
    return [parse_line(systems, line) for line in expand_lines(builders)]
