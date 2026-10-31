"""`CollectGarbage` has two wire shapes, and a feature name picks between them.

Nix 2.35 sends its paths as a `GCPaths` under `delete-dead-specific-referrers`
(`daemon.cc:745`), and as a bare set without it. pynixd decodes a recording
of two Nix 2.35 peers, which negotiate the feature between them.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol import (
    FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS,
    PROTOCOL_VERSION,
    CollectGarbageRequest,
    GCAction,
    GCPaths,
    StorePath,
)
from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.io import BytesReader, BytesWriter

PATH = "/nix/store/00000000000000000000000000000000-x"
FEATURES = frozenset({FEATURE_DELETE_DEAD_SPECIFIC_REFERRERS})


def _u64(n: int) -> bytes:
    return n.to_bytes(8, "little")


def _request(
    *, paths_to_delete: set[StorePath] | None = None, gc_paths: GCPaths | None = None
) -> CollectGarbageRequest:
    return CollectGarbageRequest(
        action=GCAction.RETURN_DEAD,
        paths_to_delete=paths_to_delete or set(),
        gc_paths=gc_paths,
        ignore_liveness=0,
        max_freed=0,
        obsolete1=0,
        obsolete2=0,
        obsolete3=0,
    )


async def _bytes(request: CollectGarbageRequest, features: frozenset[str]) -> bytes:
    writer = BytesWriter("test")
    await request.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION, features=features))
    return writer.get_bytes()


@pytest.mark.anyio
async def test_the_whole_store_is_one_tag_under_the_feature() -> None:
    """Action, tag 1, then the five integers: nothing between them."""
    raw = await _bytes(_request(gc_paths=GCPaths(whole_store=1)), FEATURES)
    assert raw == _u64(20) + _u64(GCAction.RETURN_DEAD) + _u64(1) + _u64(0) * 5


@pytest.mark.anyio
async def test_named_paths_read_back_under_the_feature() -> None:
    request = _request(gc_paths=GCPaths(whole_store=0, paths={StorePath(PATH)}, delete_referrers=1))
    raw = await _bytes(request, FEATURES)

    read = await CollectGarbageRequest.from_reader(
        ReadContext(reader=BytesReader(raw[8:]), version=PROTOCOL_VERSION, features=FEATURES),
    )
    assert read.gc_paths == GCPaths(whole_store=0, paths={StorePath(PATH)}, delete_referrers=1)
    assert read.max_freed == 0


@pytest.mark.anyio
async def test_the_floor_shape_is_a_bare_set() -> None:
    """With no feature, the bytes are the ones Nix 2.34 reads."""
    raw = await _bytes(_request(paths_to_delete={StorePath(PATH)}), frozenset())

    read = await CollectGarbageRequest.from_reader(
        ReadContext(reader=BytesReader(raw[8:]), version=PROTOCOL_VERSION),
    )
    assert read.paths_to_delete == {StorePath(PATH)}
    assert read.gc_paths is None


def test_an_empty_set_is_the_whole_store_for_a_collection() -> None:
    """`daemon.cc:748` of Nix 2.35 reads the bare set this way."""
    both = _request(paths_to_delete=set()).with_both_shapes()
    assert both.gc_paths == GCPaths(whole_store=1)


def test_named_paths_fill_both_fields() -> None:
    specific = CollectGarbageRequest(
        action=GCAction.DELETE_SPECIFIC,
        gc_paths=GCPaths(paths={StorePath(PATH)}),
        ignore_liveness=0,
        max_freed=0,
        obsolete1=0,
        obsolete2=0,
        obsolete3=0,
    )
    both = specific.with_both_shapes()
    assert both.paths_to_delete == {StorePath(PATH)}
    assert not both.needs_the_feature()


def test_what_the_bare_set_cannot_say_needs_the_feature() -> None:
    """A 2.34 daemon reads `DELETE_DEAD` with paths as every dead path (`gc.cc:698`)."""
    referrers = _request(gc_paths=GCPaths(paths={StorePath(PATH)}, delete_referrers=1))
    dead = CollectGarbageRequest(
        action=GCAction.DELETE_DEAD,
        paths_to_delete={StorePath(PATH)},
        ignore_liveness=0,
        max_freed=0,
        obsolete1=0,
        obsolete2=0,
        obsolete3=0,
    )
    assert referrers.needs_the_feature()
    assert dead.needs_the_feature()
    assert not _request(gc_paths=GCPaths(whole_store=1)).needs_the_feature()


@pytest.mark.anyio
async def test_a_request_built_with_the_bare_set_writes_under_the_feature() -> None:
    """A caller that names only the old field still writes to a peer of the new shape."""
    raw = await _bytes(_request(paths_to_delete=set()), FEATURES)
    assert raw == _u64(20) + _u64(GCAction.RETURN_DEAD) + _u64(1) + _u64(0) * 5
