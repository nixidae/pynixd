"""Domain-specific unsigned integer values used on the daemon wire."""

from __future__ import annotations

from typing import Any, Self

from pydantic_core import core_schema


class WireUInt64(int):
    """A typed Nix daemon ``uint64`` with Pydantic boundary support."""

    def __new__(cls, value: int = 0) -> Self:
        return super().__new__(cls, value)

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: Any,
    ) -> core_schema.CoreSchema:
        """Validate and serialize as a bounded JSON integer."""
        return core_schema.no_info_after_validator_function(
            cls,  # type: ignore[arg-type]  # zuban-bug: zuban reads type[Self] as not Callable[[Any], Any]; mypy accepts the class as a callable
            core_schema.int_schema(ge=0, le=(1 << 64) - 1),
            serialization=core_schema.plain_serializer_function_ser_schema(
                int,
                return_schema=core_schema.int_schema(),
            ),
        )
