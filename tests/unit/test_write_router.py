"""The write router (`pynixd/write_router.py`): the op's route first, then the reason table in order."""

from __future__ import annotations

from typing import Any

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time
from pynixd.constants import proto
from pynixd.op_routes import OpRoute
from pynixd.store.add import ImportSettings
from pynixd.write_router import FRAMED_NAR, Write, WriteRouter, pynixd_import

STORE = "router-test"
NEW = proto(1, 38)
NAR_OPS = {39: OpRoute.pynixd, 44: OpRoute.pynixd}


def _info(ca: str = "") -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path="/nix/store/" + "a" * 32 + "-p"),
        info=UnkeyedValidPathInfo(
            deriver=None,
            nar_hash=NARHash(hash="0" * 64),
            references=set(),
            registration_time=Time(ts=1),
            nar_size=96,
            ultimate=False,
            sigs=set(),
            ca=ContentAddress(value=ca),
        ),
    )


class _Engine:
    def __init__(self, *, writable: bool = True, **settings: Any) -> None:
        self.writable = writable
        self._settings = ImportSettings(**settings)

    async def settings(self) -> ImportSettings:
        return self._settings


def _router(routes: dict[int, OpRoute] | None = None, **engine: Any) -> WriteRouter:
    return WriteRouter(NAR_OPS if routes is None else routes, _Engine(**engine), STORE)  # type: ignore[arg-type]  # a stand-in engine


def _fallthroughs(op: str) -> float:
    return REGISTRY.get_sample_value("pynixd_op_fallthrough_total", {"store_id": STORE, "op": op}) or 0.0


async def test_the_engine_takes_a_plain_import() -> None:
    assert await _router().engine_takes(Write(op="AddToStoreNar", version=NEW, info=_info()))


async def test_the_daemon_route_wins_and_counts_nothing() -> None:
    before = _fallthroughs("AddToStoreNar")
    router = _router({39: OpRoute.daemon})
    write = Write(op="AddToStoreNar", version=NEW, info=_info())
    assert not await router.engine_takes(write)
    assert await router.why_the_daemon(write) == "AddToStoreNar takes the daemon route"
    assert _fallthroughs("AddToStoreNar") == before


@pytest.mark.parametrize(
    ("engine", "write", "reason"),
    [
        ({"writable": False}, Write(op="AddToStoreNar", version=NEW, repair=True), "not writable"),
        ({}, Write(op="AddToStoreNar", version=proto(1, 22)), "NAR unframed"),
        ({"auto_optimise_store": True}, Write(op="AddToStoreNar", version=NEW, repair=True), "repair"),
        ({"auto_optimise_store": True, "fsync_store_paths": True}, Write(op="AddToStoreNar", version=NEW), "optimise"),
        ({"fsync_store_paths": True}, Write(op="AddToStoreNar", version=NEW), "fsync"),
        ({}, Write(op="AddMultipleToStore", version=NEW, info=_info("fixed:git:sha1:" + "0" * 40)), "Git"),
    ],
)
async def test_the_first_reason_sends_it_to_the_daemon(engine: dict[str, Any], write: Write, reason: str) -> None:
    before = _fallthroughs(write.op)
    router = _router(**engine)
    why = await router.why_the_daemon(write)
    assert why is not None
    assert reason in why
    assert not await router.engine_takes(write)
    assert _fallthroughs(write.op) == before + 1


async def test_pynixd_imports_follow_add_to_store_nar() -> None:
    write = pynixd_import(_info())
    assert write.op == "AddToStoreNar"
    assert write.version == FRAMED_NAR
    assert await _router().engine_takes(write)
    assert not await _router({39: OpRoute.daemon, 44: OpRoute.pynixd}).engine_takes(write)
