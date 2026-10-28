"""Roundtrip every concrete daemon-protocol wire model.

The test discovers models rather than maintaining a fixture per operation. A
new model is therefore covered as soon as it is added to the package; a type
the exemplar builder does not understand fails loudly and asks for support.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import types
from enum import Enum
from typing import Any, get_args, get_origin

import pytest

import nix_daemon_protocol
from nix_daemon_protocol import PROTOCOL_VERSION
from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.io import BytesReader, BytesWriter
from nix_daemon_protocol.wire_integer import WireUInt64
from nix_daemon_protocol.wire_message import WireModel
from nix_daemon_protocol.wire_ops import WireRequest, WireResponse
from nix_daemon_protocol.wire_scalar import WireScalar, is_wire_scalar
from nix_daemon_protocol.wire_string import WireString


def _concrete_wire_models() -> list[type[WireModel]]:
    """Return every concrete model defined by this distribution."""
    excluded = {WireModel, WireString, WireRequest, WireResponse}
    models: set[type[WireModel]] = set()
    prefix = f"{nix_daemon_protocol.__name__}."
    for module_info in pkgutil.walk_packages(nix_daemon_protocol.__path__, prefix):
        module = importlib.import_module(module_info.name)
        for _, candidate in inspect.getmembers(module, inspect.isclass):
            if (
                candidate.__module__ == module.__name__
                and issubclass(candidate, WireModel)
                and candidate not in excluded
            ):
                models.add(candidate)
    return sorted(models, key=lambda model: (model.__module__, model.__name__))


def _example_value(annotation: Any, field_name: str) -> Any:
    """Build one non-empty value for a wire annotation."""
    origin = get_origin(annotation)
    arguments = get_args(annotation)

    if origin is types.UnionType:
        return _example_value(next(member for member in arguments if member is not type(None)), field_name)
    if origin is list:
        return [_example_value(arguments[0], field_name)]
    if origin is set:
        return {_example_value(arguments[0], field_name)}
    if origin is dict:
        return {_example_value(arguments[0], f"{field_name}_key"): _example_value(arguments[1], field_name)}

    if annotation is bool:
        return True
    if annotation is int:
        return 1
    if annotation is str:
        return f"{field_name}-value"
    if annotation is bytes:
        return b"wire-value"
    if inspect.isclass(annotation) and issubclass(annotation, WireUInt64):
        return annotation(1)
    # `is_wire_scalar` and not `issubclass(..., WireScalar)`: a scalar is
    # known by `from_wire`/`to_wire`, and `StorePath` no longer inherits the
    # base class.
    if is_wire_scalar(annotation):
        return annotation.from_wire(f"{field_name}-value")
    if inspect.isclass(annotation) and issubclass(annotation, Enum):
        return next(iter(annotation))
    if inspect.isclass(annotation) and issubclass(annotation, WireModel):
        return _example_model(annotation)

    raise TypeError(f"No roundtrip exemplar for {annotation!r} ({field_name})")


def _example_model(model_type: type[WireModel]) -> WireModel:
    """Instantiate a model from all of its declared wire fields."""
    values = {}
    for name, field in model_type.model_fields.items():
        # Tagged stderr messages must retain their protocol-defined tag.
        if name == "code" and field.default is not None:
            continue
        values[name] = _example_value(field.annotation, name)
    return model_type(**values)


def _model_id(model_type: type[WireModel]) -> str:
    return f"{model_type.__module__.removeprefix('nix_daemon_protocol.')}.{model_type.__name__}"


@pytest.mark.parametrize("model_type", _concrete_wire_models(), ids=_model_id)
async def test_every_wire_model_roundtrips(model_type: type[WireModel]) -> None:
    """Every model preserves its wire representation through a decode."""
    value = _example_model(model_type)
    encoded = BytesWriter()
    await value.to_writer(WriteContext(writer=encoded, version=PROTOCOL_VERSION))

    reader = BytesReader(encoded.bytes())
    read_context = ReadContext(reader=reader, version=PROTOCOL_VERSION)
    if issubclass(model_type, WireRequest):
        request_type = model_type
        assert await reader.read_uint64() == request_type.op
    elif "code" in model_type.model_fields and model_type.__name__ != "WireLogs":
        await reader.read_uint64()
    decoded = await model_type.from_reader(read_context)

    reencoded = BytesWriter()
    await decoded.to_writer(WriteContext(writer=reencoded, version=PROTOCOL_VERSION))
    assert reencoded.bytes() == encoded.bytes()


def _optional_scalar_fields(model_type: type[WireModel]) -> list[str]:
    """The names of the fields that are an optional `WireScalar`."""
    names = []
    for name, field in model_type.model_fields.items():
        annotation = field.annotation
        if get_origin(annotation) is not types.UnionType:
            continue
        members = get_args(annotation)
        if type(None) not in members:
            continue
        rest = tuple(member for member in members if member is not type(None))
        if len(rest) == 1 and inspect.isclass(rest[0]) and issubclass(rest[0], WireScalar):
            names.append(name)
    return names


def _models_with_an_optional_scalar() -> list[type[WireModel]]:
    return [model for model in _concrete_wire_models() if _optional_scalar_fields(model)]


@pytest.mark.parametrize("model_type", _models_with_an_optional_scalar(), ids=_model_id)
async def test_an_absent_optional_scalar_reads_back_as_none(model_type: type[WireModel]) -> None:
    """The value survives the decode, and not the bytes alone.

    `common-protocol.cc:71` writes the empty string for an absent optional
    store path, so the bytes of `None` and of the empty scalar are the same
    and the test above cannot tell them apart. It compares the re-encoding
    with the encoding, which an empty scalar satisfies just as well.

    This compares the **model**. A field that reads back as the empty scalar
    is not equal to the one that was written, so a caller that tests
    `is None` would take a branch that never runs. Issue nixidae/nixidae#233.
    """
    value = _example_model(model_type)
    # Every optional field, and not the scalar ones alone. A field that a
    # feature gates off is never written, so it always reads back as its
    # default; leaving a value in one would compare a field that the wire
    # never carried. `QueryRealisationRequest` declares one of each.
    for name, field in model_type.model_fields.items():
        if get_origin(field.annotation) is types.UnionType and type(None) in get_args(field.annotation):
            object.__setattr__(value, name, None)

    encoded = BytesWriter()
    await value.to_writer(WriteContext(writer=encoded, version=PROTOCOL_VERSION))

    reader = BytesReader(encoded.bytes())
    read_context = ReadContext(reader=reader, version=PROTOCOL_VERSION)
    if issubclass(model_type, WireRequest):
        request_type = model_type
        assert await reader.read_uint64() == request_type.op
    elif "code" in model_type.model_fields and model_type.__name__ != "WireLogs":
        await reader.read_uint64()
    decoded = await model_type.from_reader(read_context)

    for name in _optional_scalar_fields(model_type):
        assert getattr(decoded, name) is None, f"{name} came back as {getattr(decoded, name)!r}"
    assert decoded == value
