"""Equivalence tests for the opt-in generated-code codec experiment."""

from __future__ import annotations

from typing import cast

import pytest

from nix_daemon_protocol import (
    SUPPORTED_PROTOCOL_VERSIONS,
    AddBuildLogRequest,
    BasicDerivation,
    BuildResult,
    DerivationOutput,
    DrvOutput,
    OptMicroseconds,
    QueryPathInfoResponse,
    Realisation,
    Signature,
    StorePath,
    UnkeyedRealisation,
)
from nix_daemon_protocol.constants import FEATURE_REALISATION_WITH_PATH
from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.experimental_compiled import compile_codec
from nix_daemon_protocol.io import BytesReader, BytesWriter
from nix_daemon_protocol.wire_message import WireModel
from nix_daemon_protocol.wire_ops import WireRequest


def _values() -> tuple[WireModel, ...]:
    output = StorePath(path="/nix/store/0123456789abcdefghijklmnopqrstuv-output")
    return (
        AddBuildLogRequest(path=output.name),
        BasicDerivation(
            outputs={"out": DerivationOutput(path=str(output))},
            input_srcs=set(),
            platform="x86_64-linux",
            builder="/nix/store/0123456789abcdefghijklmnopqrstuv-builder",
            args=["--arg", "value"],
            env={"PATH": "/bin"},
        ),
        BuildResult(
            status=0,
            error_msg="",
            times_built=1,
            is_non_deterministic=0,
            start_time=1_700_000_000,
            stop_time=1_700_000_100,
            cpu_user=OptMicroseconds(tag=1, value=1_500),
            cpu_system=OptMicroseconds(tag=1, value=250),
            built_outputs={
                "out": Realisation(
                    id=DrvOutput(drv_hash="sha256:0123456789abcdefghijklmnopqrstuv", output_name="out"),
                    out_path=output,
                    signatures=[Signature("cache:signature")],
                    dependent_realisations={},
                ),
            },
        ),
        QueryPathInfoResponse(valid=False),
    )


@pytest.mark.parametrize("version", SUPPORTED_PROTOCOL_VERSIONS)
@pytest.mark.parametrize("value", _values(), ids=lambda value: type(value).__name__)
async def test_compiled_codec_matches_generic_codec(value: WireModel, version: int) -> None:
    """Generated code has identical bytes and decoded model state."""
    generic_writer = BytesWriter()
    await value.to_writer(WriteContext(writer=generic_writer, version=version))

    codec = compile_codec(type(value), version)
    compiled_writer = BytesWriter()
    await codec.write(value, WriteContext(writer=compiled_writer, version=version))
    assert compiled_writer.bytes() == generic_writer.bytes()

    generic_reader = BytesReader(generic_writer.bytes())
    compiled_reader = BytesReader(generic_writer.bytes())
    if isinstance(value, WireRequest):
        await generic_reader.read_uint64()
        await compiled_reader.read_uint64()
    generic_decoded = await type(value).from_reader(ReadContext(reader=generic_reader, version=version))
    compiled_decoded = await codec.read(
        ReadContext(reader=compiled_reader, version=version),
    )
    assert compiled_decoded.model_dump() == generic_decoded.model_dump()


def test_compiled_codec_exposes_inspectable_source() -> None:
    """The experiment remains reviewable rather than opaque generated magic."""
    codec = compile_codec(BuildResult, SUPPORTED_PROTOCOL_VERSIONS[-1])
    assert codec.schema.model is BuildResult
    assert codec.schema.version == SUPPORTED_PROTOCOL_VERSIONS[-1]
    assert "value.status" in codec.write_source
    assert "await ctx.reader.read_uint64()" in codec.read_source


class _MaybePath(WireModel):
    """One optional scalar: absence travels as the empty string, both ways."""

    path: StorePath | None = None


async def test_compiled_optional_scalar_roundtrips_absence() -> None:
    """`None` writes `""` and `""` reads back as `None`, on both codecs.

    The generic codec spells this rule in `_find_reader`/`_find_writer`
    (issue nixidae/nixidae#233); the compiler must spell the same one.
    A hot path hits it constantly: an empty `deriver` is the common case.
    """
    version = SUPPORTED_PROTOCOL_VERSIONS[-1]

    generic_writer = BytesWriter()
    await _MaybePath(path=None).to_writer(WriteContext(writer=generic_writer, version=version))
    compiled_writer = BytesWriter()
    await compile_codec(_MaybePath, version).write(
        _MaybePath(path=None), WriteContext(writer=compiled_writer, version=version)
    )
    assert compiled_writer.bytes() == generic_writer.bytes()

    generic_decoded = await _MaybePath.from_reader(
        ReadContext(reader=BytesReader(generic_writer.bytes()), version=version)
    )
    compiled_decoded = await compile_codec(_MaybePath, version).read(
        ReadContext(reader=BytesReader(generic_writer.bytes()), version=version),
    )
    assert generic_decoded.path is None
    assert cast("_MaybePath", compiled_decoded).path is None
    assert compiled_decoded.model_dump() == generic_decoded.model_dump()


async def test_compiled_codec_follows_features() -> None:
    """`BuildResult` carries two shapes, and the feature picks one (issue #14).

    Each shape is filled where it is gated in: a gated-in field left at
    `None` is unwritable on either codec, because no presence flag carries
    it. The shapes must differ across features and agree within one.
    """
    version = SUPPORTED_PROTOCOL_VERSIONS[-1]
    output = StorePath(path="/nix/store/0123456789abcdefghijklmnopqrstuv-output")
    plain_value = BuildResult(
        status=0,
        error_msg="",
        times_built=1,
        is_non_deterministic=0,
        start_time=1_700_000_000,
        stop_time=1_700_000_100,
        built_outputs={
            "sha256:0123456789abcdefghijklmnopqrstuv!out": Realisation(
                id=DrvOutput(drv_hash="sha256:0123456789abcdefghijklmnopqrstuv", output_name="out"),
                out_path=output,
                signatures=[],
                dependent_realisations={},
            )
        },
    )
    shaped_value = BuildResult(
        status=0,
        error_msg="",
        times_built=1,
        is_non_deterministic=0,
        start_time=1_700_000_000,
        stop_time=1_700_000_100,
        built_outputs_by_name={
            "out": UnkeyedRealisation(out_path=output, signatures=set()),
        },
    )

    plain: frozenset[str] = frozenset()
    shaped: frozenset[str] = frozenset({FEATURE_REALISATION_WITH_PATH})
    encoded: dict[frozenset[str], bytes] = {}
    for features, value in ((plain, plain_value), (shaped, shaped_value)):
        generic_writer = BytesWriter()
        await value.to_writer(WriteContext(writer=generic_writer, version=version, features=features))
        compiled_writer = BytesWriter()
        await compile_codec(BuildResult, version, features).write(
            value, WriteContext(writer=compiled_writer, version=version, features=features)
        )
        assert compiled_writer.bytes() == generic_writer.bytes()
        encoded[features] = generic_writer.bytes()

        compiled_decoded = await compile_codec(BuildResult, version, features).read(
            ReadContext(reader=BytesReader(generic_writer.bytes()), version=version, features=features),
        )
        generic_decoded = await BuildResult.from_reader(
            ReadContext(reader=BytesReader(generic_writer.bytes()), version=version, features=features),
        )
        assert compiled_decoded.model_dump() == generic_decoded.model_dump()

    assert encoded[plain] != encoded[shaped]
