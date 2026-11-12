"""Stable goal identity keys."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pynixd_store.signing import key_overrides

if TYPE_CHECKING:
    from pynixd_store.signing import KeyOverrides

    from ..connection import ClientConn


def keys_for(client: ClientConn | None) -> KeyOverrides:
    """*client*'s overrides of the key settings: the keys its substitutes must meet (`daemon.cc:233`)."""
    if client is None or client.options is None:
        return ()
    return key_overrides(client.options.overrides)


@dataclass(frozen=True)
class EnsureDerivedPathKey:
    """Deduplication key for EnsureDerivedPathGoal instances."""

    derived_path: str
    substituter_fingerprint: str
    build_mode: int
    """A check of a derivation and the normal build of it as an input are two goals."""
    keys: KeyOverrides = ()
    """The client's key settings: they decide which substitute the goal may take."""


@dataclass(frozen=True)
class BuildDerivationKey:
    """Deduplication key for BuildDerivationGoal instances."""

    drv_path: str
    derivation_fingerprint: str
    build_mode: int


@dataclass(frozen=True)
class SubstitutePathKey:
    """Deduplication key for SubstitutePathGoal instances."""

    path: str
    substituter_fingerprint: str
    repair: bool = False
    keys: KeyOverrides = ()
    """The client's key settings: they decide which substitute the goal may take."""
