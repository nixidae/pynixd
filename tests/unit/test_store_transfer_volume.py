"""Per-store NAR volume from path infos, in and out separately. Issue #82.

A transfer already walks the closure infos to learn what to move, so the
per-store count sums their NAR sizes: one `.inc()` per path, never per
chunk, recorded unconditionally. Directions stay separate so a reader
sees which way the data flows: "in" is into the local store from the
peer, "out" is out of it to the peer.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd import metrics, state as state_collector
from pynixd.daemon_extensions.query_closure_with_info import QueryClosureWithInfoResponse
from pynixd.serde import (
    ContentAddress,
    NARHash,
    QueryValidPathsResponse,
    StorePath,
    Time,
    UnkeyedValidPathInfo,
)
from pynixd.sessions import ClientSessions
from pynixd.state_models import StoreTransfer
from pynixd.store import transfer as transfer_module
from pynixd.store.transfer import stream_paths_store_to_store
from pynixd.substitution_queue import SubstitutionCandidate, SubstitutionQueue

if TYPE_CHECKING:
    from pynixd.store import Store

_ZERO_HASH = "0" * 64


def _info(name: str, nar_size: int) -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path=f"/nix/store/{'0' * 31}{name}"),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash=_ZERO_HASH),
            references=set(),
            registration_time=Time(ts=1),
            nar_size=nar_size,
            ultimate=True,
            sigs=set(),
            ca=ContentAddress(value=""),
        ),
    )


def _delta(store_id: str) -> dict[str, int]:
    """The per-store map as deltas, so shared registry state cannot leak in."""
    return metrics.store_transfers([store_id])[store_id]


def test_record_sums_bytes_and_paths_per_store_and_direction() -> None:
    """In and out accumulate independently under their own store."""
    store_id = "vol-summing"
    before = _delta(store_id)
    metrics.record_store_transfer(store_id, "in", 100, 2)
    metrics.record_store_transfer(store_id, "in", 50, 1)
    metrics.record_store_transfer(store_id, "out", 7, 1)
    after = _delta(store_id)
    assert after["bytes_in"] - before["bytes_in"] == 150
    assert after["paths_in"] - before["paths_in"] == 3
    assert after["bytes_out"] - before["bytes_out"] == 7
    assert after["paths_out"] - before["paths_out"] == 1


def test_reader_reports_zeros_for_a_quiet_store() -> None:
    """A store with no traffic is zeros, like every other reader here."""
    assert _delta("vol-quiet-never-touched") == {
        "bytes_in": 0,
        "bytes_out": 0,
        "paths_in": 0,
        "paths_out": 0,
    }


class _Src:
    """Answers the closure query, and nothing else."""

    store_id = "vol-src"

    def __init__(self, infos: list[ValidPathInfo]) -> None:
        self._infos = infos

    async def execute(self, _request: object, client: object = None) -> QueryClosureWithInfoResponse:  # noqa: ARG002
        return QueryClosureWithInfoResponse(infos=self._infos)

    def transfer_conn(self) -> Any:
        return _Conn("src")


class _Dst:
    """Holds the paths named at construction, so the rest are moved."""

    store_id = "vol-dst"

    def __init__(self, held: set[StorePath]) -> None:
        self._held = held

    async def execute(self, _request: object) -> QueryValidPathsResponse:
        return QueryValidPathsResponse(paths=self._held)

    def transfer_conn(self) -> Any:
        return _Conn("dst")

    def add_path_infos(self, _infos: object) -> None:
        return None


class _Conn:
    """A connection the monkeypatched stream never touches."""

    def __init__(self, conn_id: str) -> None:
        self.id = conn_id

    async def __aenter__(self) -> _Conn:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False


async def test_stream_counts_only_moved_bytes_under_peer_and_direction(monkeypatch: pytest.MonkeyPatch) -> None:
    """The already-valid filter runs first, so held paths count nothing."""
    infos = [_info("a", 100), _info("b", 50), _info("c", 25)]
    streamed: list[ValidPathInfo] = []

    async def _no_stream(src_conn: object, dst_conn: object, to_transfer: list[ValidPathInfo], cancel: object) -> None:
        streamed.extend(to_transfer)

    monkeypatch.setattr(transfer_module, "_stream_paths_over_conns", _no_stream)
    before = _delta("vol-peer")
    await stream_paths_store_to_store(
        cast("Any", _Src(infos)),
        cast("Any", _Dst({infos[2].path})),
        [info.path for info in infos],
        peer_store_id="vol-peer",
        direction="out",
    )
    assert [info.path for info in streamed] == [infos[0].path, infos[1].path]
    after = _delta("vol-peer")
    assert after["bytes_out"] - before["bytes_out"] == 150
    assert after["paths_out"] - before["paths_out"] == 2
    assert after["bytes_in"] - before["bytes_in"] == 0


async def test_stream_without_a_peer_records_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Callers that name no peer get the old behaviour: bytes move, count none."""
    infos = [_info("a", 96)]

    async def _no_stream(src_conn: object, dst_conn: object, to_transfer: list[ValidPathInfo], cancel: object) -> None:
        return None

    monkeypatch.setattr(transfer_module, "_stream_paths_over_conns", _no_stream)
    await stream_paths_store_to_store(
        cast("Any", _Src(infos)),
        cast("Any", _Dst(set())),
        [info.path for info in infos],
    )
    assert (
        REGISTRY.get_sample_value(
            "pynixd_store_transfer_nar_bytes_total",
            {"store_id": "vol-unmetered", "direction": "out"},
        )
        is None
    )


async def test_peer_and_direction_come_together() -> None:
    """The function cannot tell which side is local, so half a label is refused."""
    infos = [_info("a", 96)]
    with pytest.raises(RuntimeError, match="come together"):
        await stream_paths_store_to_store(
            cast("Any", _Src(infos)),
            cast("Any", _Dst(set())),
            [info.path for info in infos],
            peer_store_id="vol-half",
        )
    with pytest.raises(RuntimeError, match="come together"):
        await stream_paths_store_to_store(
            cast("Any", _Src(infos)),
            cast("Any", _Dst(set())),
            [info.path for info in infos],
            direction="in",
        )


async def test_substitution_import_counts_in_for_its_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """The third road: a substituted path arrives from its substituter."""
    info = _info("s", 300)
    candidate = SubstitutionCandidate(store=cast("Store", SimpleNamespace(store_id="vol-cache")), path_info=info)
    queue = SubstitutionQueue(
        cast(
            "Any",
            SimpleNamespace(
                settings=SimpleNamespace(
                    substitution_cache_maxsize=8,
                    substitution_positive_ttl=60,
                    substitution_negative_ttl=60,
                    http_upstream_negative_ttl=60,
                )
            ),
        )
    )

    async def _candidate(path: object, stores: object = None) -> SubstitutionCandidate:
        return candidate

    async def _import(path: object, cand: object) -> None:
        assert cand is candidate
        return None

    monkeypatch.setattr(queue, "get_substituter", _candidate)
    monkeypatch.setattr(queue, "_import_nar", _import)
    before = _delta("vol-cache")
    result = await queue._substitute_uncached(info.path, ())
    assert result.substituted
    after = _delta("vol-cache")
    assert after["bytes_in"] - before["bytes_in"] == 300
    assert after["paths_in"] - before["paths_in"] == 1
    assert after["bytes_out"] - before["bytes_out"] == 0


def test_transfers_section_carries_per_store_volume() -> None:
    """The state collector nests the per-store map under transfers."""
    metrics.record_store_transfer("vol-section", "out", 40, 1)
    ctx = SimpleNamespace(
        scheduler=None,
        stores={"vol-section": object(), "vol-section-quiet": object()},
        sessions=ClientSessions(),
    )
    transfers = state_collector.collect(cast("Any", ctx), ["transfers"]).transfers
    assert transfers is not None
    stores = transfers.stores
    assert stores["vol-section"].bytes_out >= 40
    assert stores["vol-section"].paths_out >= 1
    assert stores["vol-section-quiet"] == StoreTransfer()
    assert transfers.bytes_received >= 0
