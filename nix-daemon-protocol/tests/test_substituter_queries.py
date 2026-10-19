"""The bytes of the two substituter queries, against `daemon.cc` of Nix.

`QuerySubstitutablePathInfo` is operation 21, at `daemon.cc:794`, and it asks
about one path. `QuerySubstitutablePathInfos` is operation 30, at
`daemon.cc:812`, and it asks about a named set. `Store::queryMissing` asks the
second one, because `Store::querySubstitutablePaths` at `store-api.cc:517`
skips each substituter whose `want-mass-query` is off.

`tests/test_model_roundtrips.py` writes each model and reads it back, so it
finds a codec that disagrees with itself. It does not find a codec that
disagrees with Nix. This module pins the field order and the field count of
both operations to the two blocks of `daemon.cc`.
"""

from __future__ import annotations

import struct

from nix_daemon_protocol import (
    PROTOCOL_VERSION,
    STDERR_LAST,
    ContentAddress,
    QuerySubstitutablePathInfoRequest,
    QuerySubstitutablePathInfoResponse,
    QuerySubstitutablePathInfosRequest,
    QuerySubstitutablePathInfosResponse,
    StorePath,
    SubstitutablePathInfo,
    UnkeyedSubstitutablePathInfo,
)
from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.io import BytesReader, BytesWriter

PATH = "/nix/store/00000000000000000000000000000000-example"
DERIVER = "/nix/store/11111111111111111111111111111111-example.drv"
REFERENCE = "/nix/store/22222222222222222222222222222222-reference"


def uint64(value: int) -> bytes:
    """One little-endian 64-bit integer, which is every number on this wire."""
    return struct.pack("<Q", value)


def wire_string(value: str) -> bytes:
    """A length, the bytes, and zero padding up to a multiple of eight."""
    raw = value.encode()
    padding = (-len(raw)) % 8
    return uint64(len(raw)) + raw + b"\0" * padding


def op_code(code: int) -> bytes:
    """`WireRequest.to_writer` writes the operation code before the body."""
    return uint64(code)


def empty_log() -> bytes:
    """`WireResponse` writes the stderr stream before the body.

    An answer with no log line is one `STDERR_LAST`, which is the word that
    `daemon.cc` writes through `logger->stopWork()`.
    """
    return uint64(STDERR_LAST)


async def to_bytes(message: object) -> bytes:
    writer = BytesWriter()
    await message.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))  # type: ignore[attr-defined] -- every wire model has it
    return writer.get_bytes()


async def from_bytes(model: type, raw: bytes) -> object:
    reader = BytesReader(raw)
    return await model.from_reader(ReadContext(reader=reader, version=PROTOCOL_VERSION))


# ── Operation 21, the singular form ─────────────────────────────────


async def test_the_request_of_operation_21_is_one_path() -> None:
    """`daemon.cc:795` reads one `StorePath` and nothing else."""
    request = QuerySubstitutablePathInfoRequest(path=StorePath(path=PATH))

    assert await to_bytes(request) == op_code(21) + wire_string(PATH)


async def test_a_path_that_no_substituter_holds_is_one_zero() -> None:
    """`daemon.cc:802` writes `0`, and the four fields do not follow it."""
    response = QuerySubstitutablePathInfoResponse(found=False)

    assert await to_bytes(response) == empty_log() + uint64(0)


async def test_the_answer_of_operation_21_carries_no_path() -> None:
    """The request named the path, so `daemon.cc:804` writes four fields.

    The order is the deriver, the references, the download size and the NAR
    size. There is no store path in the answer.
    """
    response = QuerySubstitutablePathInfoResponse(
        found=True,
        info=UnkeyedSubstitutablePathInfo(
            deriver=StorePath(path=DERIVER),
            references={StorePath(path=REFERENCE)},
            download_size=11,
            nar_size=22,
        ),
    )

    assert await to_bytes(response) == (
        empty_log() + uint64(1) + wire_string(DERIVER) + uint64(1) + wire_string(REFERENCE) + uint64(11) + uint64(22)
    )


async def test_a_missing_deriver_of_operation_21_is_the_empty_string() -> None:
    """`std::optional<StorePath>` writes an empty string, `common-protocol.cc:71`."""
    response = QuerySubstitutablePathInfoResponse(
        found=True,
        info=UnkeyedSubstitutablePathInfo(deriver=None, references=set(), download_size=0, nar_size=0),
    )

    assert await to_bytes(response) == (empty_log() + uint64(1) + wire_string("") + uint64(0) + uint64(0) + uint64(0))


async def test_operation_21_reads_back_what_it_wrote() -> None:
    written = (
        empty_log() + uint64(1) + wire_string(DERIVER) + uint64(1) + wire_string(REFERENCE) + uint64(11) + uint64(22)
    )

    response = await from_bytes(QuerySubstitutablePathInfoResponse, written)

    assert isinstance(response, QuerySubstitutablePathInfoResponse)
    assert response.found
    assert response.info is not None
    assert str(response.info.deriver) == DERIVER
    assert {str(one) for one in response.info.references} == {REFERENCE}
    assert (response.info.download_size, response.info.nar_size) == (11, 22)


# ── Operation 30, the set form ──────────────────────────────────────


async def test_the_request_of_operation_30_is_a_path_to_content_address_map() -> None:
    """`StorePathCAMap` above protocol 1.22, at `daemon.cc:819`.

    A path with no content address carries the empty string, which is what
    `CommonProto::Serialise<std::optional<ContentAddress>>::write` writes.
    """
    request = QuerySubstitutablePathInfosRequest(paths={StorePath(path=PATH): ContentAddress("")})

    assert await to_bytes(request) == op_code(30) + uint64(1) + wire_string(PATH) + wire_string("")


async def test_the_answer_of_operation_30_carries_the_path_of_each_entry() -> None:
    """`daemon.cc:824` writes the count, and then five fields for each entry."""
    response = QuerySubstitutablePathInfosResponse(
        infos=[
            SubstitutablePathInfo(
                path=StorePath(path=PATH),
                deriver=StorePath(path=DERIVER),
                references={StorePath(path=REFERENCE)},
                download_size=11,
                nar_size=22,
            )
        ]
    )

    assert await to_bytes(response) == (
        empty_log()
        + uint64(1)
        + wire_string(PATH)
        + wire_string(DERIVER)
        + uint64(1)
        + wire_string(REFERENCE)
        + uint64(11)
        + uint64(22)
    )


async def test_an_empty_answer_of_operation_30_is_one_zero() -> None:
    """No substituter holds any path of the request."""
    assert await to_bytes(QuerySubstitutablePathInfosResponse(infos=[])) == empty_log() + uint64(0)


async def test_operation_30_reads_back_two_entries_in_order() -> None:
    """The count leads, so a reader that miscounts a field loses the rest."""
    other = "/nix/store/33333333333333333333333333333333-other"
    written = (
        empty_log()
        + uint64(2)
        + wire_string(PATH)
        + wire_string("")
        + uint64(0)
        + uint64(1)
        + uint64(2)
        + wire_string(other)
        + wire_string(DERIVER)
        + uint64(0)
        + uint64(3)
        + uint64(4)
    )

    response = await from_bytes(QuerySubstitutablePathInfosResponse, written)

    assert isinstance(response, QuerySubstitutablePathInfosResponse)
    assert [str(one.path) for one in response.infos] == [PATH, other]
    # **An absent deriver reads as `None`, and the bytes do not move.**
    # `common-protocol.cc:71` writes the empty string for an absent optional
    # store path, and this reads that empty string back as `None`. The rule
    # belongs to the package rather than to this codec: every optional
    # `WireScalar` field reads the same way, and
    # `UnkeyedValidPathInfo.deriver` is the other declaration of this one.
    # Issue nixidae/nixidae#233.
    assert response.infos[0].deriver is None
    assert [(one.download_size, one.nar_size) for one in response.infos] == [(1, 2), (3, 4)]


def test_both_operations_carry_the_codes_that_nix_gives_them() -> None:
    assert QuerySubstitutablePathInfoRequest.op == 21
    assert QuerySubstitutablePathInfosRequest.op == 30
