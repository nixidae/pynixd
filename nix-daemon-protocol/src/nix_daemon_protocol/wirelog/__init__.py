"""Record what a client and a Nix daemon say to each other, and compare two runs.

pynixd is a proxy, so its contract is that a client cannot tell it apart from
`nix-daemon`. That is a statement about the wire. This package tests it by
running one workload twice and comparing the two recordings:

    run A:  nix client -- recorder --> nix-daemon
    run B:  nix client -- recorder --> pynixd --> nix-daemon

A test of Nix that fails for its own reasons fails the same way in both runs,
so the two recordings agree and the comparison reports nothing. Only a
difference between the two is a finding, and the finding names one operation.
Issue nixidae/nixidae#214.
"""

from __future__ import annotations

from .decode import (
    Handshake as Handshake,
)
from .decode import (
    Operation as Operation,
)
from .decode import (
    Session as Session,
)
from .decode import (
    decode as decode,
)
from .diff import (
    EXEMPTIONS as EXEMPTIONS,
)
from .diff import (
    Difference as Difference,
)
from .diff import (
    compare as compare,
)
from .diff import (
    exemptions as exemptions,
)
from .diff import (
    report as report,
)
from .framing import (
    Chunk as Chunk,
)
from .framing import (
    Direction as Direction,
)
from .framing import (
    encode_chunk as encode_chunk,
)
from .framing import (
    one_direction as one_direction,
)
from .framing import (
    read_chunks as read_chunks,
)
from .recorder import Recorder as Recorder
