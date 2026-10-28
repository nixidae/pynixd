"""The build trace has two wire shapes, and a feature name picks between them.

**Nix 2.34 carries a whole `Realisation` as one JSON string.** `realisation.py`
holds that shape, and every peer at the floor of this repository speaks it.

**The master branch carries a struct, and refuses the JSON.**
`WorkerProto::Serialise<UnkeyedRealisation>` at `worker-protocol.cc:485`
raises when the negotiated set does not hold `realisation-with-path-not-hash`,
and writes an output path and a set of signatures when it does.
`WorkerProto::Serialise<DrvOutput>` at `worker-protocol.cc:544` does the same
with a derivation path and an output name.

These tests pin the second shape, byte for byte. Nothing writes it yet:
`SUPPORTED_STANDARD_FEATURES` is empty, so pynixd never claims the feature and
never meets a peer that sends it. The codec lands first and the claim lands
after, because a claim with no codec drops the connection. Issue #14.
"""

from __future__ import annotations

from typing import Any

import pytest

from nix_daemon_protocol import (
    FEATURE_REALISATION_WITH_PATH,
    PROTOCOL_VERSION,
    BuildResult,
    KeyedDrvOutput,
    OptMicroseconds,
    QueryRealisationResponse,
    Realisation,
    RegisterDrvOutputRequest,
    Signature,
    StorePath,
    UnkeyedRealisation,
)
from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.io import BytesReader, BytesWriter
from nix_daemon_protocol.wire_message import WireModel

OUT_PATH = "/nix/store/00000000000000000000000000000000-x"
DRV_PATH = "/nix/store/11111111111111111111111111111111-x.drv"


def _a_result(**maps: Any) -> BuildResult:
    """A `BuildResult` with every version-gated field filled in.

    The 1.29 and 1.37 fields default to `None`, and `PROTOCOL_VERSION` puts
    each of them on the wire, so a writer that meets `None` where an integer
    belongs raises. The maps are the subject here; the rest is scaffolding.
    """
    return BuildResult(
        status=0,
        error_msg="",
        times_built=1,
        is_non_deterministic=0,
        start_time=1_700_000_000,
        stop_time=1_700_000_100,
        cpu_user=OptMicroseconds(tag=0, value=0),
        cpu_system=OptMicroseconds(tag=0, value=0),
        **maps,
    )


async def _round_trip[T: WireModel](value: WireModel, model: type[T]) -> tuple[bytes, T]:
    """Write *value* and read it back as *model*, and answer both."""
    writer = BytesWriter("test")
    await value.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    raw = writer.get_bytes()
    read = await model.from_reader(ReadContext(reader=BytesReader(raw), version=PROTOCOL_VERSION))
    return raw, read


@pytest.mark.anyio
async def test_an_unkeyed_realisation_is_a_path_and_the_signatures() -> None:
    value = UnkeyedRealisation(out_path=StorePath(OUT_PATH), signatures={Signature("cache.example.org-1:abc")})

    _, read = await _round_trip(value, UnkeyedRealisation)

    assert read.out_path == StorePath(OUT_PATH)
    assert read.signatures == {"cache.example.org-1:abc"}


@pytest.mark.anyio
async def test_an_unkeyed_realisation_carries_no_dependent_realisations() -> None:
    """The field of the JSON shape is gone, and it is not empty but absent.

    `UnkeyedRealisation` of the master branch has no such member. A codec that
    wrote an empty map would put eight bytes on the wire that the peer reads
    as the next field.
    """
    assert "dependent_realisations" not in UnkeyedRealisation.model_fields
    assert "dependentRealisations" not in Realisation.model_fields  # it is there under its own name

    writer = BytesWriter("test")
    await UnkeyedRealisation(out_path=StorePath(OUT_PATH)).to_writer(
        WriteContext(writer=writer, version=PROTOCOL_VERSION),
    )

    # One string for the path, and one count of zero for the signature set,
    # and nothing after them. A Nix string is a length of eight bytes and the
    # bytes padded up to a multiple of eight.
    padded = (len(OUT_PATH) + 7) // 8 * 8
    assert len(writer.get_bytes()) == 8 + padded + 8


@pytest.mark.anyio
async def test_a_keyed_drv_output_names_a_derivation_and_not_a_hash() -> None:
    """The report on #14 saw `sha256:0000…0000!out`, which this shape cannot say."""
    value = KeyedDrvOutput(drv_path=StorePath(DRV_PATH), output_name="out")

    _, read = await _round_trip(value, KeyedDrvOutput)

    assert read.drv_path == StorePath(DRV_PATH)
    assert read.output_name == "out"


@pytest.mark.anyio
async def test_the_two_shapes_of_a_realisation_are_not_the_same_bytes() -> None:
    """So a peer that reads one from the other decodes nothing useful.

    This is the reason the feature has to be negotiated and not assumed. The
    wire holds no marker that says which shape it carries.
    """
    old = BytesWriter("test")
    await Realisation(out_path=StorePath(OUT_PATH)).to_writer(
        WriteContext(writer=old, version=PROTOCOL_VERSION),
    )
    new = BytesWriter("test")
    await UnkeyedRealisation(out_path=StorePath(OUT_PATH)).to_writer(
        WriteContext(writer=new, version=PROTOCOL_VERSION),
    )

    assert old.get_bytes() != new.get_bytes()
    # The old shape is one JSON string, so it holds the field names.
    assert b"outPath" in old.get_bytes()
    assert b"outPath" not in new.get_bytes()


@pytest.mark.anyio
async def test_a_build_result_carries_one_built_outputs_map_and_not_two() -> None:
    """`worker-protocol.cc:268` writes it as an if/else, and so does this.

    Two fields hold the two shapes, and exactly one of them reaches the wire.
    A model that wrote both would put a second map on the wire that the peer
    reads as whatever comes after `BuildResult`.
    """
    result = _a_result(
        built_outputs={"h!out": Realisation(out_path=StorePath(OUT_PATH))},
        built_outputs_by_name={"out": UnkeyedRealisation(out_path=StorePath(OUT_PATH))},
    )

    without = BytesWriter("test")
    await result.to_writer(WriteContext(writer=without, version=PROTOCOL_VERSION))
    with_it = BytesWriter("test")
    await result.to_writer(
        WriteContext(writer=with_it, version=PROTOCOL_VERSION, features=frozenset({FEATURE_REALISATION_WITH_PATH})),
    )

    # The JSON of the old shape names its fields; the new shape names none.
    assert b"outPath" in without.get_bytes()
    assert b"outPath" not in with_it.get_bytes()
    assert without.get_bytes() != with_it.get_bytes()


@pytest.mark.anyio
async def test_a_build_result_reads_back_the_shape_it_wrote() -> None:
    """Under the feature, the map is keyed by the output name alone."""
    result = _a_result(built_outputs_by_name={"out": UnkeyedRealisation(out_path=StorePath(OUT_PATH))})
    features = frozenset({FEATURE_REALISATION_WITH_PATH})

    writer = BytesWriter("test")
    await result.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION, features=features))
    read = await BuildResult.from_reader(
        ReadContext(reader=BytesReader(writer.get_bytes()), version=PROTOCOL_VERSION, features=features),
    )

    assert read.built_outputs is None
    assert read.built_outputs_by_name is not None
    assert read.built_outputs_by_name["out"].out_path == StorePath(OUT_PATH)


@pytest.mark.anyio
async def test_a_query_realisation_answer_is_an_option_under_the_feature() -> None:
    """`daemon.cc:1024` writes a tag, and the body only when the tag is 1.

    The fields are read in the order they are declared, so `present` holds
    its value before the gate on `realisation` runs.
    """
    features = frozenset({FEATURE_REALISATION_WITH_PATH})
    answer = QueryRealisationResponse(
        present=1,
        realisation=UnkeyedRealisation(out_path=StorePath(OUT_PATH)),
    )

    writer = BytesWriter("test")
    await answer.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION, features=features))
    read = await QueryRealisationResponse.from_reader(
        ReadContext(reader=BytesReader(writer.get_bytes()), version=PROTOCOL_VERSION, features=features),
    )

    assert read.present == 1
    assert read.realisation is not None
    assert read.realisation.out_path == StorePath(OUT_PATH)
    assert read.realisations == []


@pytest.mark.anyio
async def test_a_query_realisation_answer_of_none_carries_no_body() -> None:
    """A tag of 0 and nothing after it. A body here would desync the wire."""
    features = frozenset({FEATURE_REALISATION_WITH_PATH})

    writer = BytesWriter("test")
    await QueryRealisationResponse(present=0).to_writer(
        WriteContext(writer=writer, version=PROTOCOL_VERSION, features=features),
    )

    # Eight bytes for the end of the stderr stream that every `WireResponse`
    # writes first, and eight for the tag. Nothing follows the tag.
    assert len(writer.get_bytes()) == 16
    read = await QueryRealisationResponse.from_reader(
        ReadContext(reader=BytesReader(writer.get_bytes()), version=PROTOCOL_VERSION, features=features),
    )
    assert read.realisation is None


@pytest.mark.anyio
async def test_a_registered_realisation_is_the_id_and_then_the_body() -> None:
    """`Serialise<Realisation>::write` at `worker-protocol.cc:582` writes that order."""
    features = frozenset({FEATURE_REALISATION_WITH_PATH})
    request = RegisterDrvOutputRequest(
        keyed_drv_output=KeyedDrvOutput(drv_path=StorePath(DRV_PATH), output_name="out"),
        unkeyed_realisation=UnkeyedRealisation(out_path=StorePath(OUT_PATH)),
    )

    writer = BytesWriter("test")
    await request.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION, features=features))
    raw = writer.get_bytes()

    # The derivation path comes before the output path.
    assert raw.index(DRV_PATH.encode()) < raw.index(OUT_PATH.encode())

    # `WireRequest.to_writer` writes the operation number first, and
    # `from_reader` reads the body alone because the dispatcher consumed it.
    read = await RegisterDrvOutputRequest.from_reader(
        ReadContext(reader=BytesReader(raw[8:]), version=PROTOCOL_VERSION, features=features),
    )
    assert read.realisation is None
    assert read.keyed_drv_output is not None
    assert read.keyed_drv_output.output_name == "out"
    assert read.unkeyed_realisation is not None
    assert read.unkeyed_realisation.out_path == StorePath(OUT_PATH)


@pytest.mark.anyio
async def test_the_floor_shape_of_both_operations_is_unchanged() -> None:
    """With no feature, the bytes are the ones Nix 2.34 reads.

    This is the guard on the whole change: nothing claims the feature yet, so
    every real connection takes this road.
    """
    request = RegisterDrvOutputRequest(realisation=Realisation(out_path=StorePath(OUT_PATH)))
    writer = BytesWriter("test")
    await request.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))

    # One JSON string, and nothing else.
    assert b"outPath" in writer.get_bytes()
    read = await RegisterDrvOutputRequest.from_reader(
        ReadContext(reader=BytesReader(writer.get_bytes()[8:]), version=PROTOCOL_VERSION),
    )
    assert read.realisation is not None
    assert read.keyed_drv_output is None
    assert read.unkeyed_realisation is None


def test_the_json_shape_keeps_signatures_as_plain_strings() -> None:
    """`Realisation.signatures` holds `Signature`, and JSON never shows it.

    The two shapes disagree on the container on purpose: this one is JSON and
    keeps a list, `UnkeyedRealisation` is a struct and keeps a set. A set would
    serialize in no fixed order, and two dumps of one value would differ.

    The element type is the same scalar in both, so the `set(...)` that
    `BuildResult` and `goals/ensure.py` build from this list is a
    `set[Signature]` and needs no conversion at the call site.
    """
    raw = (
        b'{"id":"sha256-abc!out","outPath":"' + OUT_PATH.encode() + b'",'
        b'"signatures":["k1:sig1","k2:sig2"],"dependentRealisations":{}}'
    )
    realisation = Realisation.from_json(raw)

    assert [type(one) for one in realisation.signatures] == [Signature, Signature]
    assert realisation.signatures[0].name == "k1"
    assert realisation.signatures[0].signature == "sig1"

    # Order kept, and the same bytes every time.
    assert realisation.to_json() == realisation.to_json()
    assert '"signatures":["k1:sig1","k2:sig2"]' in realisation.to_json()
