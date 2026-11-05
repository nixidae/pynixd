"""The non-deprecated Nix daemon operation contract for protocol 1.32+."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from .constants import MINIMUM_REMOTE_PROTOCOL, proto


@dataclass(frozen=True)
class StandardOperation:
    """A standard Nix daemon request supported by this package."""

    code: int
    name: str
    min_protocol: int = MINIMUM_REMOTE_PROTOCOL


# Source of truth: Nix's WorkerProto::Op enum, excluding entries Nix marks
# removed or obsolete. See src/libstore/include/nix/store/worker-protocol.hh.
STANDARD_OPERATIONS: Final[tuple[StandardOperation, ...]] = (
    StandardOperation(1, "IsValidPath"),
    StandardOperation(6, "QueryReferrers"),
    StandardOperation(7, "AddToStore"),
    StandardOperation(9, "BuildPaths"),
    StandardOperation(10, "EnsurePath"),
    StandardOperation(11, "AddTempRoot"),
    StandardOperation(12, "AddIndirectRoot"),
    StandardOperation(14, "FindRoots"),
    StandardOperation(19, "SetOptions"),
    StandardOperation(20, "CollectGarbage"),
    StandardOperation(21, "QuerySubstitutablePathInfo"),
    StandardOperation(23, "QueryAllValidPaths"),
    StandardOperation(26, "QueryPathInfo"),
    StandardOperation(29, "QueryPathFromHashPart"),
    StandardOperation(30, "QuerySubstitutablePathInfos"),
    StandardOperation(31, "QueryValidPaths"),
    StandardOperation(32, "QuerySubstitutablePaths"),
    StandardOperation(33, "QueryValidDerivers"),
    StandardOperation(34, "OptimiseStore"),
    StandardOperation(35, "VerifyStore"),
    StandardOperation(36, "BuildDerivation"),
    StandardOperation(37, "AddSignatures"),
    StandardOperation(38, "NarFromPath"),
    StandardOperation(39, "AddToStoreNar"),
    StandardOperation(40, "QueryMissing"),
    StandardOperation(41, "QueryDerivationOutputMap"),
    StandardOperation(42, "RegisterDrvOutput"),
    StandardOperation(43, "QueryRealisation"),
    StandardOperation(44, "AddMultipleToStore", proto(1, 32)),
    StandardOperation(45, "AddBuildLog", proto(1, 32)),
    StandardOperation(46, "BuildPathsWithResults", proto(1, 34)),
    StandardOperation(47, "AddPermRoot", proto(1, 36)),
    StandardOperation(49, "AddTempRoots", proto(1, 38)),
    # A builder's own daemon alone serves these two (`daemon.cc:349`).
    StandardOperation(1000, "SubmitOutput", proto(1, 38)),
    StandardOperation(1001, "AddToStoreScanning", proto(1, 38)),
)
