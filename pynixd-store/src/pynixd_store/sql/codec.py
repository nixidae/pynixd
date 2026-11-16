"""How each engine binds and reads the values its schema stores in a type of its own.

Callers hand typed values to the codec of the session's engine, never a packed
string. SQLite keeps Nix's shapes: a JSON array for `json_each`, signatures
joined by spaces, `ultimate` as 1 or NULL. PostgreSQL binds native types.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

__all__ = ["POSTGRES", "SQLITE", "Codec"]


class Codec(Protocol):
    def path_set(self, paths: Iterable[str]) -> object:
        """A set of store paths, bound as one parameter."""
        ...

    def path_signatures(self, sigs: Sequence[str]) -> object:
        """`ValidPaths.sigs`, in the order the caller gives."""
        ...

    def trace_signatures(self, sigs: Sequence[str]) -> object:
        """`BuildTraceV3.signatures` and `Realisations.signatures`."""
        ...

    def ultimate(self, flag: bool) -> object:
        """`ValidPaths.ultimate`."""
        ...

    def strings(self, value: object) -> list[str]:
        """A column of signatures, or of paths an aggregate collected, as a list."""
        ...

    def string_list(self, values: Sequence[str]) -> object:
        """A list of strings a column keeps in order: a derivation shape's keys and features."""
        ...


@dataclass(frozen=True, slots=True)
class SqliteCodec:
    def path_set(self, paths: Iterable[str]) -> object:
        return json.dumps(list(paths))

    def path_signatures(self, sigs: Sequence[str]) -> object:
        return " ".join(sigs) or None

    def trace_signatures(self, sigs: Sequence[str]) -> object:
        # `registerDrvOutput` binds the empty set as "", with no NULL flag.
        return " ".join(sigs)

    def ultimate(self, flag: bool) -> object:
        # `addValidPath` binds `ultimate` with its own NULL flag: NULL when false.
        return 1 if flag else None

    def strings(self, value: object) -> list[str]:
        return str(value).split() if value else []

    def string_list(self, values: Sequence[str]) -> object:
        return json.dumps(list(values))


@dataclass(frozen=True, slots=True)
class PostgresCodec:
    def path_set(self, paths: Iterable[str]) -> object:
        return list(paths)

    def path_signatures(self, sigs: Sequence[str]) -> object:
        return list(sigs)

    def trace_signatures(self, sigs: Sequence[str]) -> object:
        return list(sigs)

    def ultimate(self, flag: bool) -> object:
        return flag

    def strings(self, value: object) -> list[str]:
        return list(value) if isinstance(value, list) else []

    def string_list(self, values: Sequence[str]) -> object:
        return list(values)


SQLITE: Codec = SqliteCodec()
POSTGRES: Codec = PostgresCodec()
