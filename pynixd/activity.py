"""Activity ids for the log messages pynixd makes itself."""

from __future__ import annotations

import itertools
import os

_act_counter = itertools.count()


def next_activity_id() -> int:
    """An activity id that cannot collide with the ones a backend sends.

    pynixd forwards a backend daemon's log messages with their activity ids
    unchanged, so an id pynixd makes for itself has to miss that space.
    `src/libutil/logging.cc:208` is where Nix answers the same question:

        id(nextId++ + (((uint64_t) getPid()) << 32))

    Same scheme here. Two daemons on one machine have different pids, so the
    high half separates them. Two on different machines can share a pid, and
    that is a collision Nix has as well with a remote builder -- copying the
    scheme copies its limit, and inventing a different one would not remove
    it.
    """
    return next(_act_counter) + (os.getpid() << 32)
