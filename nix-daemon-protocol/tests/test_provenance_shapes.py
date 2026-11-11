"""Determinate Nix's `provenance` feature: one JSON string, empty for none, in each path info and in `AddToStore`.

The string follows the content address of `UnkeyedValidPathInfo` (`worker-protocol.cc:390`, `:407` of Determinate
v3.24.0), so `AddToStoreNar` carries it before `repair` (`daemon.cc:1014`). `AddToStore` carries it after `repair`
(`daemon.cc:452`). A peer without the feature sees neither.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol import (
    FEATURE_PROVENANCE,
    PROTOCOL_VERSION,
    AddToStoreNarRequest,
    AddToStoreRequest,
    StorePath,
)
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.io import BytesReader, BytesWriter
from nix_daemon_protocol.nar_hash import NARHash
from nix_daemon_protocol.path_info import UnkeyedValidPathInfo
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from nix_daemon_protocol.wire_time import Time

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_message import WireModel

PATH = "/nix/store/00000000000000000000000000000000-x"
PROVENANCE = '{"type":"build","drv":"00000000000000000000000000000000-x.drv","output":"out"}'
FEATURES = frozenset({FEATURE_PROVENANCE})


def _string(text: str) -> bytes:
    raw = text.encode()
    return len(raw).to_bytes(8, "little") + raw + b"\0" * (-len(raw) % 8)


def _info(provenance: str = PROVENANCE) -> ValidPathInfo:
    return ValidPathInfo(
        path=StorePath(path=PATH),
        info=UnkeyedValidPathInfo(
            nar_hash=NARHash(hash="0" * 64),
            references=set(),
            registration_time=Time(ts=0),
            nar_size=8,
            ultimate=False,
            sigs=set(),
            ca=ContentAddress(value=""),
            provenance=provenance,
        ),
    )


async def _bytes(value: WireModel, features: frozenset[str]) -> bytes:
    writer = BytesWriter("test")
    await value.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION, features=features))
    return writer.get_bytes()


@pytest.mark.anyio
async def test_a_path_info_carries_provenance_last_under_the_feature() -> None:
    without = await _bytes(_info(), frozenset())
    assert await _bytes(_info(), FEATURES) == without + _string(PROVENANCE)
    assert without == await _bytes(_info(provenance=""), frozenset())


@pytest.mark.anyio
async def test_a_path_info_reads_its_provenance_back() -> None:
    reader = BytesReader(await _bytes(_info(), FEATURES))
    read = await ValidPathInfo.from_reader(ReadContext(reader=reader, version=PROTOCOL_VERSION, features=FEATURES))
    assert read.info.provenance == PROVENANCE


@pytest.mark.anyio
async def test_add_to_store_carries_provenance_after_repair() -> None:
    request = AddToStoreRequest(
        path_name="x", cam=ContentAddress(value="fixed:r:sha256"), references=set(), repair=0, provenance=PROVENANCE
    )
    without = await _bytes(request, frozenset())
    assert await _bytes(request, FEATURES) == without + _string(PROVENANCE)


@pytest.mark.anyio
async def test_add_to_store_nar_carries_provenance_before_repair() -> None:
    request = AddToStoreNarRequest(info=_info(), repair=1, dont_check_sigs=0)
    flags = (1).to_bytes(8, "little") + (0).to_bytes(8, "little")
    without = await _bytes(request, frozenset())
    assert without.endswith(flags)
    assert await _bytes(request, FEATURES) == without[: -len(flags)] + _string(PROVENANCE) + flags


def test_a_narinfo_keeps_its_provenance_line() -> None:
    text = _info().to_narinfo()
    assert f"Provenance: {PROVENANCE}\n" in text
    assert ValidPathInfo.from_narinfo(text).info.provenance == PROVENANCE
    assert "Provenance" not in _info(provenance="").to_narinfo()
