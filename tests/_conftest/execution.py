"""Fakes of what execution reads, held to the Protocols of `pynixd/execution.py`.

A test passes them with no cast, so the type checker fails a fake that lacks
a member execution reads. nixidae/nixidae#1246.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.config import StoreRankingSettings
from pynixd.execution import Executor
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode
from pynixd_store.store_path import StorePath
from tests._conftest.placement import FakeStore

if TYPE_CHECKING:
    from collections.abc import Mapping

    from nix_daemon_protocol.ids import StoreId
    from pynixd.build_queue import BuildQueue, QueuedBuild
    from pynixd.execution import Builder
    from pynixd.serde import SetOptionsRequest


def _conforms(store: FakeStore) -> Builder:
    """Never called. A fake that lacks a member fails `checks.types` here, and the error names the member."""
    return store


def request(name: str, platform: str = "x86_64-linux") -> BuildDerivationRequest:
    return BuildDerivationRequest(
        drv_path=StorePath(path=f"/nix/store/{name:0>32}-{name}.drv"),
        derivation=BasicDerivation(platform=platform, builder="/bin/sh"),
        build_mode=BuildMode.NORMAL,
    )


async def queued(
    queue: BuildQueue,
    name: str = "x",
    *,
    options: SetOptionsRequest | None = None,
    builders: frozenset[StoreId] | None = None,
) -> QueuedBuild:
    """A build in *queue*, as a client's `BuildDerivation` enqueues it."""
    build_id, _ = await queue.enqueue(request(name), options=options, builders=builders)
    return queue.by_id[build_id]


class Triggers:
    """Counts the scheduling passes execution asks for."""

    def __init__(self) -> None:
        self.count = 0

    def __call__(self) -> None:
        self.count += 1


def fake_executor(
    queue: BuildQueue,
    stores: Mapping[StoreId, FakeStore],
    *,
    settings: StoreRankingSettings | None = None,
    trigger: Triggers | None = None,
) -> Executor[FakeStore]:
    """An executor over fake stores, with the local store's id `local`."""
    return Executor(
        queue=queue,
        local_id=LOCAL_STORE_ID,
        stores=lambda: stores,
        settings=settings or StoreRankingSettings(),
        trigger=trigger or Triggers(),
    )
