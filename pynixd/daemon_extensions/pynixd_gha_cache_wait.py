"""PynixdGhaCacheWait (op 114): wait until the GitHub Actions cache has every queued upload.

A job's last step sends it, because the runner goes away with the job and takes the queue with
it. nixidae/nixidae#1377; magic-nix-cache's `workflow-finish` is the same step (`api.rs:55`).
"""

from __future__ import annotations

from typing import ClassVar

from nix_daemon_protocol.wire_ops import WireRequest, WireResponse


class PynixdGhaCacheWaitResponse(WireResponse):
    """What each GitHub Actions cache store did, as one JSON object keyed by store id."""

    version: int = 1
    payload: str = ""


class PynixdGhaCacheWaitRequest(WireRequest):
    """Return once no GitHub Actions cache store has an upload queued or running."""

    op: ClassVar[int] = 114
    is_extension: ClassVar[bool] = True
    response_type = PynixdGhaCacheWaitResponse

    version: int = 1
