"""The write router (`pynixd/write_router.py`): the op's route first, then the reason table in order."""

from __future__ import annotations

from collections.abc import AsyncGenerator
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
from pynixd.store.source import Source
from pynixd.write_router import FRAMED_NAR, Write, WriteClient, WriteRouter, pynixd_import

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
        self.added: list[str] = []
        self.forgotten: list[str] = []

    async def settings(self) -> ImportSettings:
        return self._settings

    async def add_to_store(self, info: ValidPathInfo, source: Source, **_: Any) -> None:
        await source.skip(info.info.nar_size)
        self.added.append(str(info.path))

    def invalidate_path_info_cache_for(self, path: object) -> None:
        self.forgotten.append(str(path))


class _Daemon:
    def __init__(self) -> None:
        self.added: list[tuple[str, object]] = []

    async def add_to_store(self, info: ValidPathInfo, source: Source, *, logs: object = None, **_: Any) -> None:
        await source.skip(info.info.nar_size)
        self.added.append((str(info.path), logs))


def _router(routes: dict[int, OpRoute] | None = None, *, daemon: _Daemon | None = None, **engine: Any) -> WriteRouter:
    return WriteRouter(
        NAR_OPS if routes is None else routes,
        _Engine(**engine),  # type: ignore[arg-type]  # a stand-in engine
        daemon or _Daemon(),  # type: ignore[arg-type]  # a stand-in daemon
        STORE,
        lambda info: None,
    )


class _Bytes:
    """A `Source` of *size* zero bytes."""

    def __init__(self, size: int) -> None:
        self.left = size

    async def read(self, n: int) -> bytes:
        taken = min(n, self.left)
        self.left -= taken
        return bytes(taken)

    async def skip(self, n: int) -> None:
        self.left -= n


async def _paths(*infos: ValidPathInfo) -> AsyncGenerator[tuple[ValidPathInfo, Source]]:
    for info in infos:
        yield info, _Bytes(info.info.nar_size)


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


async def test_a_store_with_no_engine_sends_every_write_to_the_daemon() -> None:
    daemon = _Daemon()
    router = WriteRouter(NAR_OPS, None, daemon, STORE, lambda info: None)  # type: ignore[arg-type]  # a stand-in daemon
    assert await router.why_the_daemon(pynixd_import(_info())) == "AddToStoreNar takes the daemon route"
    await router.import_nar(_info(), _Bytes(96), dont_check_sigs=True)
    assert daemon.added == [(str(_info().path), None)]


async def test_add_multiple_sends_a_path_the_engine_refuses_to_the_daemon_alone() -> None:
    """The loop of `daemon.cc:505`: each path asks the router, and a Git path goes to the daemon with the client's logs."""
    plain = _info()
    git = _info("fixed:git:sha1:" + "0" * 40).model_copy(
        update={"path": StorePath(path="/nix/store/" + "b" * 32 + "-g")}
    )
    daemon = _Daemon()
    router = _router(daemon=daemon)
    rooted: list[str] = []

    async def root(path: str) -> None:
        rooted.append(path)

    conn = object()
    client = WriteClient(conn=conn, root=root)  # type: ignore[arg-type]  # a stand-in connection
    await router.add_multiple_to_store(
        Write(op="AddMultipleToStore", version=NEW), 2, _paths(git, plain), dont_check_sigs=True, client=client
    )
    engine = router.engine
    assert isinstance(engine, _Engine)
    assert engine.added == [str(plain.path)]
    assert daemon.added == [(str(git.path), conn)]
    assert rooted == [str(git.path), str(plain.path)]
    assert sorted(engine.forgotten) == sorted([str(git.path), str(plain.path)])
