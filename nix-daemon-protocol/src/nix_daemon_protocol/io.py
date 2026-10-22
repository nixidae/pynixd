"""Transport-neutral byte primitives for the Nix daemon protocol."""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable

_UINT64 = struct.Struct("<Q")
_PACK_UINT64 = _UINT64.pack
_UNPACK_UINT64_FROM = _UINT64.unpack_from


class NixReader(Protocol):
    """The read surface needed by daemon message codecs."""

    async def readexactly(self, n: int) -> bytes: ...

    async def read_uint64(self) -> int: ...

    async def read_bool(self) -> bool: ...

    async def read_bytes(self) -> bytes: ...

    async def read_string[T = str](self, tp: Callable[[str], T] = str) -> T: ...


class NixWriter(Protocol):
    """The write surface needed by daemon message codecs."""

    def write_uint64(self, value: int, /) -> None: ...

    def write_bool(self, value: bool, /) -> None: ...

    def write_bytes(self, value: bytes, /) -> None: ...

    def write_string(self, value: object, /) -> None: ...


class BytesWriter:
    """An in-memory writer suitable for serialization and tests."""

    def __init__(self, identifier: str = "memory") -> None:
        self.identifier = identifier
        self._buffer = bytearray()

    def write_uint64(self, value: int) -> None:
        self._buffer.extend(_PACK_UINT64(value))

    def write_bool(self, value: bool) -> None:
        self.write_uint64(int(value))

    def write_bytes(self, value: bytes) -> None:
        self.write_uint64(len(value))
        self._buffer.extend(value)
        self._buffer.extend(b"\0" * ((-len(value)) % 8))

    def write_string(self, value: object) -> None:
        self.write_bytes(str(value).encode("utf-8", "surrogateescape"))

    def bytes(self) -> bytes:
        """Return the written protocol bytes."""
        return bytes(self._buffer)

    def get_bytes(self) -> bytes:
        """Return the written protocol bytes (pynixd compatibility spelling)."""
        return self.bytes()

    def tell(self) -> int:
        """Return the current write offset."""
        return len(self._buffer)


class BytesReader:
    """An in-memory reader suitable for deserialization tests."""

    def __init__(self, data: bytes, identifier: str = "memory") -> None:
        self.identifier = identifier
        self._data = data
        self._position = 0

    def remaining(self) -> bytes:
        """Every byte that this reader has not read yet.

        `wirelog.decode` reads the log messages of a response and keeps what
        is left, which is the payload that the client consumes.
        """
        return self._data[self._position :]

    async def readexactly(self, n: int) -> bytes:
        """Read exactly *n* bytes or raise ``EOFError``."""
        if self._position + n > len(self._data):
            available = len(self._data) - self._position
            raise EOFError(
                f"BytesReader: need {n} bytes, have {available} remaining "
                f"(total {len(self._data)}, at offset {self._position})",
            )
        value = self._data[self._position : self._position + n]
        self._position += n
        return value

    async def read_uint64(self) -> int:
        """Read a little-endian unsigned 64-bit integer."""
        position = self._position
        end = position + 8
        if end > len(self._data):
            available = len(self._data) - position
            raise EOFError(
                f"BytesReader: need 8 bytes, have {available} remaining "
                f"(total {len(self._data)}, at offset {position})",
            )
        self._position = end
        return _UNPACK_UINT64_FROM(self._data, position)[0]

    async def read_bool(self) -> bool:
        """Read a Nix boolean."""
        return bool(await self.read_uint64())

    async def read_bytes(self) -> bytes:
        """Read an aligned Nix byte string."""
        length = await self.read_uint64()
        position = self._position
        end = position + length
        padding = (-length) % 8
        padded_end = end + padding
        if padded_end > len(self._data):
            available = len(self._data) - position
            raise EOFError(
                f"BytesReader: need {length + padding} bytes, have {available} remaining "
                f"(total {len(self._data)}, at offset {position})",
            )
        self._position = padded_end
        return self._data[position:end]

    async def read_string[T = str](self, tp: Callable[[str], T] = str) -> T:
        """Read a Nix string, bytes to Nix, losslessly, and convert it with *tp*. nixidae/nixidae#1010."""
        return tp((await self.read_bytes()).decode("utf-8", "surrogateescape"))

    def tell(self) -> int:
        """Return the current read offset."""
        return self._position
