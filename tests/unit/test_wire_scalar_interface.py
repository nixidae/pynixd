"""The codec finds a scalar by its methods, not by its base class.

`StorePath` has to stop being a `str`: Nix's own holds the base name and the
store directory belongs to the store, not to the value (nixidae/nixidae#393). It still
has to travel through the same codec afterwards.

So `wire_message` tests for `from_wire` and `to_wire` rather than for
`issubclass(ann, WireScalar)`. `WireScalar` satisfies that, so nothing moved
when the test changed; this file is what proves the other half, with a scalar
that inherits nothing.
"""

from __future__ import annotations

from typing import Any, Self

import pytest
from pydantic_core import core_schema

from nix_daemon_protocol.context import ReadContext, WriteContext
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.wire_message import WireField, WireModel
from nix_daemon_protocol.wire_scalar import WireScalar, is_wire_scalar
from pynixd import wire


class Tag:
    """A scalar that is not a `str` and not a `WireScalar`.

    The shape `StorePath` is heading for: a value with helper methods, one
    string on the wire, and no `str` in its bases.
    """

    __slots__ = ("value",)

    def __init__(self, value: str = "") -> None:
        self.value = value

    @classmethod
    def from_wire(cls, value: str) -> Self:
        return cls(value)

    def to_wire(self) -> str:
        return self.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Tag) and other.value == self.value

    def __hash__(self) -> int:
        return hash(self.value)

    @classmethod
    def __get_pydantic_core_schema__(cls, _source: Any, _handler: Any) -> core_schema.CoreSchema:
        """Accept the class, or the wire string it came from.

        **`WireScalar`'s schema wraps `str_schema()` alone, and that works
        only because a `WireScalar` is a `str`.** Hand a non-`str` scalar to
        it and pydantic refuses the value it just produced: "Input should be
        a valid string ... input_type=Tag". So a scalar that keeps its own
        type needs the instance arm as well. nixidae/nixidae#394.
        """
        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(cls),
                core_schema.no_info_after_validator_function(
                    cls.from_wire,
                    core_schema.str_schema(),
                ),
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(cls.to_wire),
        )


class Tagged(WireModel):
    tag: Tag = WireField(default_factory=Tag)


async def _round_trip(value: Tagged) -> Tagged:
    writer = wire.BytesWriter("test")
    await value.to_writer(WriteContext(writer=writer, version=0, features=frozenset()))
    return await Tagged.from_reader(
        ReadContext(reader=wire.BytesReader(writer.get_bytes()), version=0, features=frozenset()),
    )


def test_a_scalar_needs_no_base_class() -> None:
    assert is_wire_scalar(Tag)
    assert not issubclass(Tag, str)
    assert not issubclass(Tag, WireScalar)


@pytest.mark.anyio
async def test_a_scalar_that_is_not_a_str_round_trips() -> None:
    read = await _round_trip(Tagged(tag=Tag("hello")))

    assert read.tag == Tag("hello")
    assert not isinstance(read.tag, str)


def test_the_existing_scalars_still_answer() -> None:
    """`WireScalar` satisfies the same test, so nothing moved today."""
    assert is_wire_scalar(WireScalar)
    assert is_wire_scalar(StorePath)


def test_a_plain_type_is_not_a_scalar() -> None:
    assert not is_wire_scalar(str)
    assert not is_wire_scalar(int)
    assert not is_wire_scalar(WireModel)


def test_with_store_prefix_keeps_the_whole_base_name() -> None:
    """It used `hash_part()`, so the name after the hash was dropped.

    `StorePath("abc123-foo").with_store_prefix()` gave `/nix/store/abc123`,
    which names no file. Only `pynixd.store_path.StorePath` had callers, so
    nothing shipped took it; merging the two classes (#4) would have.
    """
    assert str(StorePath("abc123-foo").with_store_prefix()) == "/nix/store/abc123-foo"


def test_with_store_prefix_leaves_a_prefixed_path_alone() -> None:
    already = StorePath("/nix/store/abc123-foo")
    assert already.with_store_prefix() is already
