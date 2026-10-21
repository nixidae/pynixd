"""Shared utility functions."""

from __future__ import annotations

import hashlib
import os

NIX32_CHARS = "0123456789abcdfghijklmnpqrsvwxyz"


def nix32_encode(data: bytes) -> str:
    """Encode bytes to Nix's base-32 format.

    Nix uses a custom base-32 encoding (5 bits per character, LSB first)
    with the alphabet ``0123456789abcdfghijklmnpqrsvwxyz``.
    """
    if not data:
        return ""
    size = len(data)
    result_len = (size * 8 - 1) // 5 + 1
    result: list[str] = []
    for n in range(result_len - 1, -1, -1):
        b = n * 5
        i = b // 8
        j = b % 8
        c = (data[i] >> j) & 0x1F
        if i + 1 < size:
            c |= (data[i + 1] << (8 - j)) & 0x1F
        result.append(NIX32_CHARS[c])
    return "".join(result)


def nix32_decode(text: str) -> bytes:
    """The bytes `nix32_encode` wrote: `Hash::parse` of the Nix32 form."""
    size = len(text) * 5 // 8
    out = bytearray(size)
    for n, char in enumerate(reversed(text)):
        digit = NIX32_CHARS.index(char)
        i, j = divmod(n * 5, 8)
        out[i] |= (digit << j) & 0xFF
        carry = digit >> (8 - j)
        if i + 1 < size:
            out[i + 1] |= carry
        elif carry:
            raise ValueError(f"invalid nix32 hash {text!r}")
    return bytes(out)


def compress_hash(data: bytes, new_size: int) -> bytes:
    """XOR-fold a hash digest into *new_size* bytes.

    Matches Nix's ``hash::compressHash`` (used by ``Store::makeStorePath``).
    """
    if len(data) == new_size:
        return data
    assert len(data) > new_size
    result = bytearray(data[:new_size])
    for i in range(new_size, len(data)):
        result[i % new_size] ^= data[i]
    return bytes(result)


def random_nix32_hash() -> str:
    """Generate a random 32-char nix base32 hash for probe derivations."""
    return nix32_encode(hashlib.sha256(os.urandom(32)).digest())[:32]
