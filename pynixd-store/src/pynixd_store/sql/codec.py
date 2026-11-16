"""How each engine binds the values its schema stores in a type of its own.

Callers hand typed values to the codec of the session's engine, never a packed
string. SQLite keeps Nix's shapes (a JSON array for `json_each`); PostgreSQL
binds native types (`text[]`).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Iterable

__all__ = ["POSTGRES", "SQLITE", "Codec"]


class Codec(Protocol):
    def path_set(self, paths: Iterable[str]) -> object:
        """A set of store paths, bound as one parameter."""
        ...


@dataclass(frozen=True, slots=True)
class SqliteCodec:
    def path_set(self, paths: Iterable[str]) -> object:
        return json.dumps(list(paths))


@dataclass(frozen=True, slots=True)
class PostgresCodec:
    def path_set(self, paths: Iterable[str]) -> object:
        return list(paths)


SQLITE: Codec = SqliteCodec()
POSTGRES: Codec = PostgresCodec()
