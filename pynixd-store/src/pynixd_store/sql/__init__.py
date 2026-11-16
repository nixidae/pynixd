"""Every statement pynixd sends to a store database, one set per engine.

Each module holds one group of tables: a frozen dataclass of statements and
one instance per engine. Each instance holds the whole text of each statement:
nothing translates one engine's SQL into another's, and the tests run each set
against its own engine (nixidae/nixidae#1209). Engine control (pragmas,
`BEGIN IMMEDIATE`, checkpoints) is not here: it belongs to the engine's code.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import codec, gc, liveness, nix, observations, path_access, placement
from .codec import Codec as Codec
from .gc import GcSql as GcSql
from .liveness import LivenessSql as LivenessSql
from .nix import NixSql as NixSql
from .observations import ObservationSql as ObservationSql
from .path_access import PathAccessSql as PathAccessSql
from .placement import PlacementSql as PlacementSql


@dataclass(frozen=True, slots=True)
class Statements:
    """One engine's statements. `nix` follows the store's `ValidPaths`, so a database replaces it at open."""

    nix: NixSql
    path_access: PathAccessSql
    observations: ObservationSql
    placement: PlacementSql
    gc: GcSql
    liveness: LivenessSql
    codec: Codec


SQLITE = Statements(
    nix=nix.SQLITE,
    path_access=path_access.SQLITE,
    observations=observations.SQLITE,
    placement=placement.SQLITE,
    gc=gc.SQLITE,
    liveness=liveness.SQLITE,
    codec=codec.SQLITE,
)

POSTGRES = Statements(
    nix=nix.POSTGRES,
    path_access=path_access.POSTGRES,
    observations=observations.POSTGRES,
    placement=placement.POSTGRES,
    gc=gc.POSTGRES,
    liveness=liveness.POSTGRES,
    codec=codec.POSTGRES,
)
