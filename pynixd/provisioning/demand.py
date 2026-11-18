"""The waiting builds a provisioner may launch a builder for. nixidae/nixidae#1198.

A build counts when the last scheduling pass left it with no store
(`Placer.no_candidate`) or with every candidate at its build limit
(`Placer.held_by_pressure`). A build that waits for a local slot, for
`preferLocalBuild`, or for machines its client named, does not: a new
machine would not take it. A build whose client named a pool does: that
pool's new machine takes it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..serde import BuildMode
from ..system_features import effective_required_features
from .decide import Pool, Want

if TYPE_CHECKING:
    from collections.abc import Iterable, Set

    from nix_daemon_protocol.ids import BuildId

    from ..build_queue import QueuedBuild


def reaches_pools(build: QueuedBuild) -> bool:
    """Whether *build* may run on a pool's machine: its client named no builders, or named a pool."""
    return build.builders is None or bool(build.pools)


def counts(build: QueuedBuild) -> bool:
    """Whether *build* is a reason to launch a machine."""
    return (
        reaches_pools(build)
        and build.request.build_mode == BuildMode.NORMAL
        and not build.request.derivation.build_local
    )


def want_of(build: QueuedBuild, held_for: float | None = None) -> Want:
    return Want(
        build.request.derivation.platform,
        frozenset(effective_required_features(build.request.derivation)),
        held_for=held_for,
        pools=build.pools,
    )


def wants(builds: Iterable[QueuedBuild], held: Set[BuildId] = frozenset(), now: float = 0.0) -> list[Want]:
    """Each build as demand. One in *held* waits for a busy store; *now* is `time.monotonic()`."""
    return [
        want_of(build, now - build.enqueued_at if build.build_id in held else None)
        for build in builds
        if build.is_pending and counts(build)
    ]


def launchable(pools: Iterable[Pool], build: QueuedBuild) -> bool:
    """Whether a pool's machine may take *build*: placement waits for one instead of failing the build.

    Not `counts`: a `preferLocalBuild` or check build launches no machine, but takes one another build launched.
    """
    return reaches_pools(build) and any(pool.builds(want_of(build)) for pool in pools)
