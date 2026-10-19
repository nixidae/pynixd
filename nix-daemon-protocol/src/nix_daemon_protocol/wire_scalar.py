"""Domain-specific scalar values whose wire representation is a string."""

from __future__ import annotations

from typing import Any, Protocol, Self, TypeIs, runtime_checkable

from pydantic_core import core_schema


@runtime_checkable
class WireScalarLike(Protocol):
    """One Python value, one string on the wire.

    **The codec tests for this pair of methods, not for a base class.**
    `StorePath` has to stop being a `str` (nixidae/nixidae#393: Nix's own holds the base
    name and the store directory belongs to the store), and it still has to
    travel through the same codec. Only `WireScalar` satisfies this today, so
    nothing moves yet.
    """

    @classmethod
    def from_wire(cls, value: str) -> Self: ...

    def to_wire(self) -> str: ...


def is_wire_scalar(ann: object) -> TypeIs[type[WireScalarLike]]:
    """Does this annotation encode as one wire string?"""
    return isinstance(ann, type) and issubclass(ann, WireScalarLike)


class WireScalar(str):
    """A typed daemon-protocol string with optional domain helper methods.

    Subclasses are native strings at runtime, avoiding a Pydantic model per
    scalar while still validating and serializing as strings in Pydantic models.
    """

    def __new__(cls, value: str = "") -> Self:
        return super().__new__(cls, value)

    @classmethod
    def from_wire(cls, value: str) -> Self:
        """Construct the domain value from its canonical wire string."""
        return cls(value)

    def to_wire(self) -> str:
        """Return the canonical daemon-protocol string."""
        return str(self)

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: Any,
    ) -> core_schema.CoreSchema:
        return wire_scalar_schema(cls)


def wire_scalar_schema(cls: type[WireScalarLike]) -> core_schema.CoreSchema:
    """The pydantic schema for one Python value that travels as one string.

    **The instance arm is what lets a scalar keep its own type.** A schema of
    `str_schema()` alone validates the *input*, so it passes only because a
    `WireScalar` is a `str`. Give it a scalar that is not one and pydantic
    refuses the value it would itself have produced: "Input should be a valid
    string ... input_type=Tag". Measured in
    `tests/unit/test_wire_scalar_interface.py`.

    Serialization names `to_wire` rather than going through `str()`. The two
    agree for a `WireScalar`, and they must not be assumed to: `StorePath` is
    to hold the base name and print the store directory in `__str__` (#4), so
    `str()` would write the wrong bytes.
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
