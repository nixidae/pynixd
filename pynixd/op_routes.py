"""Which side answers a protocol operation of the local store: the daemon, or pynixd.

nixidae/nixidae#1155 moves the store's work into pynixd one operation at a
time, and `docs/design/daemonless.md` lists the operations. An operation that
pynixd can answer has three routes:

- `daemon`: the daemon answers, and pynixd's answer is not computed.
- `shadow`: the daemon answers. pynixd computes its own answer too, compares
  the two and counts the outcome (`pynixd_op_shadow_total`). Only for an
  answer that writes nothing, because both sides run.
- `pynixd`: pynixd answers. A case it cannot answer still goes to the daemon,
  and `pynixd_op_fallthrough_total` counts each one. The operation is pynixd's
  alone when that counter stays at zero under real traffic.

An operation that pynixd cannot answer always goes to the daemon.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from nix_daemon_protocol.operations import STANDARD_OPERATIONS

__all__ = ["OPERATION_NAMES", "OpRoute", "Owned"]

#: The name of each standard operation, as `op_routes` in the config spells it.
OPERATION_NAMES: frozenset[str] = frozenset(op.name for op in STANDARD_OPERATIONS)


class OpRoute(StrEnum):
    daemon = "daemon"
    shadow = "shadow"
    pynixd = "pynixd"


@dataclass(frozen=True)
class Owned:
    """pynixd's answer to one operation: the store method, and its default route."""

    method: str
    #: The route when the config names none. A new answer starts at `daemon`,
    #: and moves to `pynixd` once shadow traffic shows it agrees.
    default: OpRoute = OpRoute.daemon
    #: An answer that writes cannot run beside the daemon's, so it has no shadow.
    writes: bool = False
    #: List fields of the response whose order Nix leaves unspecified. The
    #: shadow sorts them before it compares: `FindRoots` walks an unordered map.
    unordered: frozenset[str] = frozenset()
