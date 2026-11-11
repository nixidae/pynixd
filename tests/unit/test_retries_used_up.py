"""A build that failed too often on every store that can build it fails with those stores' errors.

`reject`'s reasons (`derivation-building-goal.cc:289`) say no store has the system or the features. After
the retries every capable store is out for the build (`allocator.py:480` drops it from the ranking), and
those reasons are not true: a build that needed `builder-rpc-v0` read "missing system features" with
`builder-rpc-v0` in the available list. nixidae/nixidae#1273.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.build_queue import BuildQueue
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildMode
from pynixd_store.store_path import StorePath
from tests._conftest.placement import FakeStore, FixedRanking, Spawned, fake_placer

DRV = StorePath("/nix/store/00000000000000000000000000000001-rpc-impure.drv")
REASON = "IncompleteReadError: 0 bytes read"


@pytest.mark.anyio
async def test_the_last_error_of_each_store_is_the_reason() -> None:
    queue = BuildQueue()
    build_id, answer = await queue.enqueue(
        BuildDerivationRequest(
            drv_path=DRV,
            derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"),
            build_mode=BuildMode.NORMAL,
        )
    )
    build = queue.by_id[build_id]
    build.reset_for_retry(LOCAL_STORE_ID, REASON, elsewhere=True)
    local = FakeStore(LOCAL_STORE_ID)
    spawned = Spawned()
    placer = fake_placer(queue, FixedRanking.of(), spawned, local=local, stores={LOCAL_STORE_ID: local})

    await placer.assign([build], {})

    message = str(answer.result().result.error_msg)
    assert f"{LOCAL_STORE_ID}: {REASON}" in message
    assert "missing system features" not in message
    assert spawned.stores == []
