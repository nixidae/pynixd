"""Abstract base: a single length-prefixed string on the wire."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

from pydantic import ConfigDict, model_serializer, model_validator

from .logging import deserialization_scope
from .wire_message import WireModel, _find_reader

if TYPE_CHECKING:
    from .context import ReadContext, WriteContext


class WireString(WireModel):
    """Abstract base: a single length-prefixed string on the wire.

    Subclasses define their own fields.  JSON is native Pydantic
    (the destructed object).  Wire format is ``str(self)``.

    ``to_writer`` writes ``str(self)`` as a single wire string.
    ``__hash__`` / ``__eq__`` delegate to ``str(self)``.

    Override ``from_str`` / ``to_str`` in subclasses that need custom
    wire format (e.g. ``WireSignature`` splits on ``:``).
    """

    model_config = ConfigDict(frozen=True)

    async def to_writer(self, ctx: WriteContext) -> None:
        ctx.writer.write_string(str(self))

    def __str__(self) -> str:
        return self.to_str()

    def __hash__(self) -> int:
        field_names = tuple(type(self).model_fields)
        if type(self).to_str is WireString.to_str and len(field_names) == 1:
            return hash(getattr(self, field_names[0]))
        return hash(str(self))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, WireString):
            return str(self) == str(other)
        if isinstance(other, str):
            return str(self) == other
        return NotImplemented

    @classmethod
    async def from_reader(cls, ctx: ReadContext) -> Self:
        with deserialization_scope(ctx, cls):
            reader = _find_reader(cls, version=ctx.version)
            value: Self = await reader(ctx)
            return value

    @model_serializer
    def to_str(self) -> str:
        """Override in subclasses to customize wire output. Default: single-field value."""
        if len(type(self).model_fields) == 1:
            return str(getattr(self, next(iter(type(self).model_fields))))
        raise NotImplementedError(f"{type(self).__name__}: override to_str")

    @classmethod
    def from_str(cls, data: str) -> object:
        """Default: map string to the single field."""
        if len(cls.model_fields) == 1:
            return {next(iter(cls.model_fields)): data}
        raise NotImplementedError(f"{cls.__name__} has multiple fields, override from_str")

    @model_validator(mode="before")
    @classmethod
    def transform(cls, data: object) -> object:  # type: ignore[operator]  # zuban-bug: zuban calls the PydanticDescriptorProxy from @model_validator over @classmethod; pydantic documents this order, and mypy accepts it
        if isinstance(data, str):
            return cls.from_str(data)
        return data
