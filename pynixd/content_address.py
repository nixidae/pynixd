"""Content addresses and the store paths they give, as Nix computes them.

`content-address.cc` of Nix parses and renders the address, and
`store-dir-config.cc` turns it into a path (`makeFixedOutputPathFromCA`).
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

from .utils import compress_hash, nix32_decode, nix32_encode

if TYPE_CHECKING:
    from collections.abc import Iterable

HASH_SIZES = {"md5": 16, "sha1": 20, "sha256": 32, "sha512": 64}


class Method(StrEnum):
    """`ContentAddressMethod::Raw`, by the prefix `makeFileIngestionPrefix` gives it."""

    TEXT = "text:"
    FLAT = ""
    NAR = "r:"
    GIT = "git:"


def parse_digest(text: str, algo: str) -> bytes:
    """`Hash::parseNonSRIUnprefixed`: Base16, Nix32 or Base64, told apart by length."""
    size = HASH_SIZES.get(algo)
    if size is None:
        raise ValueError(f"unknown hash algorithm '{algo}'")
    if len(text) == 2 * size:
        return bytes.fromhex(text)
    if len(text) == (size * 8 - 1) // 5 + 1:
        return nix32_decode(text)
    if len(text) == 4 * ((size + 2) // 3):
        return base64.b64decode(text)
    raise ValueError(f"hash '{text}' has the wrong length for its algorithm")


@dataclass(frozen=True)
class ContentAddress:
    method: Method
    algo: str
    digest: bytes

    @classmethod
    def parse(cls, raw: str) -> ContentAddress:
        """`ContentAddress::parse` at `content-address.cc:191`."""
        prefix, sep, rest = raw.partition(":")
        if not sep:
            raise ValueError(f"not a content address because it is not in the form '<prefix>:<rest>': {raw}")
        if prefix == "text":
            method = Method.TEXT
        elif prefix == "fixed":
            method = Method.FLAT
            for candidate in (Method.NAR, Method.GIT):
                if rest.startswith(candidate):
                    method, rest = candidate, rest.removeprefix(candidate)
                    break
        else:
            raise ValueError(
                f"content address prefix '{prefix}' is unrecognized. Recogonized prefixes are 'text' or 'fixed'"
            )
        algo, sep, text = rest.partition(":")
        if not sep:
            raise ValueError(f"content address hash must be in form '<algo>:<hash>', but found: {raw}")
        return cls(method, algo, parse_digest(text, algo))

    def render(self) -> str:
        """`ContentAddress::render`: the form `ValidPaths.ca` holds, with the hash in Nix32."""
        head = "text:" if self.method is Method.TEXT else f"fixed:{self.method}"
        return f"{head}{self.algo}:{nix32_encode(self.digest)}"


def fixed_output_address(method: str, hash_text: str) -> ContentAddress:
    """The address a fixed output of a derivation names: `r:sha256` and its hash, say."""
    found = next((m for m in (Method.TEXT, Method.NAR, Method.GIT) if method.startswith(m)), Method.FLAT)
    algo = method.removeprefix(found)
    return ContentAddress(found, algo, parse_digest(hash_text, algo))


def make_store_path(kind: str, algo: str, digest: bytes, name: str, store_dir: str) -> str:
    """`StoreDirConfig::makeStorePath`."""
    clear = f"{kind}:{algo}:{digest.hex()}:{store_dir}:{name}"
    compressed = compress_hash(hashlib.sha256(clear.encode()).digest(), 20)
    return f"{store_dir}/{nix32_encode(compressed)}-{name}"


def _make_type(kind: str, references: Iterable[str], self_reference: bool) -> str:
    """`makeType` at `store-dir-config.cc:93`. `StorePathSet` is ordered by path."""
    parts = [kind, *sorted(references)]
    if self_reference:
        parts.append("self")
    return ":".join(parts)


def content_addressed_path(
    ca: ContentAddress,
    name: str,
    references: Iterable[str],
    store_dir: str,
    *,
    self_reference: bool = False,
) -> str:
    """`makeFixedOutputPathFromCA` at `store-dir-config.cc:129`.

    *references* are the others; a path that refers to itself says so in
    *self_reference*, which a text address cannot.
    """
    references = set(references)
    if ca.method is Method.TEXT:
        if ca.algo != "sha256":
            raise ValueError("text hashing requires sha256")
        if self_reference:
            raise ValueError("a text path cannot refer to itself")
        return make_store_path(_make_type("text", references, False), ca.algo, ca.digest, name, store_dir)
    if ca.method is Method.GIT and ca.algo not in ("sha1", "sha256"):
        raise ValueError(f"Git file ingestion must use SHA-1 or SHA-256 hash, but instead using: {ca.algo}")
    if ca.algo == "sha256" and ca.method is Method.NAR:
        return make_store_path(_make_type("source", references, self_reference), ca.algo, ca.digest, name, store_dir)
    if references or self_reference:
        raise ValueError(
            f"fixed output derivation '{name}' is not allowed to refer to other store paths.\n"
            "You may need to use the 'unsafeDiscardReferences' derivation attribute, see the manual for more details."
        )
    payload = f"fixed:out:{ca.method}{ca.algo}:{ca.digest.hex()}:"
    return make_store_path("output:out", "sha256", hashlib.sha256(payload.encode()).digest(), name, store_dir)
