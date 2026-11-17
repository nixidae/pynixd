"""The waiting builds a provisioner may launch a builder for. nixidae/nixidae#1198.

A build counts when the last scheduling pass left it with no store
(`Placer.no_candidate`) or with every candidate at its build limit
(`Placer.held_by_pressure`). A build that waits for a local slot, for
`preferLocalBuild`, or for builders its client named, does not: a new
machine would not take it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..serde import BuildMode
from ..system_features import effective_required_features
from .decide import Want

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..build_queue import QueuedBuild


def wants(builds: Iterable[QueuedBuild]) -> list[Want]:
    return [
        Want(build.request.derivation.platform, frozenset(effective_required_features(build.request.derivation)))
        for build in builds
        if build.is_pending
        and build.builders is None
        and build.request.build_mode == BuildMode.NORMAL
        and not build.request.derivation.build_local
    ]
