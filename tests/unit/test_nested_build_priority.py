"""A build that recursive Nix in a build pod asks for goes ahead of shallower builds, and its outer build's slot
is free while it waits. nixidae/nixidae#1576.

Without both, N slots and N outer builds that each start a nested build leave
no slot free, and every build waits for ever.
"""

from __future__ import annotations

import anyio
import pytest

from nix_daemon_protocol.ids import BuildId
from pynixd.answer import Answer
from pynixd.build_queue import BuildQueue, QueuedBuild
from pynixd.build_result import failure
from pynixd.connection import ClientConn
from pynixd.scheduler import nest, waits_on_nested
from pynixd.serde import BasicDerivation, BuildDerivationRequest, BuildDerivationResponse, StorePath


def _build(build_id: int, name: str) -> QueuedBuild:
    request = BuildDerivationRequest(
        drv_path=StorePath(f"/nix/store/{build_id:032d}-{name}.drv"),
        derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"),
        build_mode=0,
    )
    return QueuedBuild(BuildId(build_id), request, Answer[BuildDerivationResponse]())


def _running(build: QueuedBuild) -> QueuedBuild:
    build.worker = anyio.CancelScope()
    return build


def _pod_session(outer: QueuedBuild) -> ClientConn:
    client = ClientConn(w=None)  # type: ignore[arg-type]  # nest reads the session's grant alone
    client.serves_build = str(outer.request.drv_path)
    return client


@pytest.mark.anyio
async def test_a_nested_build_goes_first_and_frees_its_outer_builds_slot() -> None:
    outer = _running(_build(1, "outer"))
    other = _build(2, "aardvark")
    nested = _build(3, "zebra")
    deeper = _build(4, "zebra-inner")

    nest(nested, [outer, other, nested], [_pod_session(outer)])
    assert nested.nested_in == {outer.build_id} and nested.depth == 1
    # A client's own build is nested in nothing.
    nest(other, [outer, other, nested], [])
    assert not other.nested_in and other.depth == 0
    # A build that only waits to run is no outer build.
    nest(deeper, [outer, other, nested, deeper], [_pod_session(nested)])
    assert not deeper.nested_in

    assert waits_on_nested([outer, other, nested]) == {outer.build_id}
    nested.answer.set(BuildDerivationResponse(result=failure("done")))
    assert waits_on_nested([outer, other, nested]) == set()

    queue = BuildQueue()
    deep = _build(5, "zebra")
    deep.depth = 1
    queue._queue.extend([_build(6, "aardvark"), deep])
    # Nix's order is by name; depth comes before it.
    assert [b.build_id for b in await queue.get_pending()] == [deep.build_id, BuildId(6)]
