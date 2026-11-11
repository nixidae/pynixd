"""Content addresses and the store paths they give, as Nix computes them.

`content-address.cc` of Nix parses and renders the address, and
`store-dir-config.cc` turns it into a path (`makeFixedOutputPathFromCA`).
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

import blake3

from .hintfmt import magenta, missing_feature
from .utils import compress_hash, nix32_decode, nix32_encode

if TYPE_CHECKING:
    from collections.abc import Iterable

HASH_SIZES = {"blake3": 32, "md5": 16, "sha1": 20, "sha256": 32, "sha512": 64}


class Hasher(Protocol):
    def update(self, data: bytes, /) -> object: ...
    def digest(self) -> bytes: ...


def new_hash(algo: str) -> Hasher:
    """The `HashSink` of *algo* (`hash.cc:296`). `hashlib` has no BLAKE3, so that one comes from `blake3`."""
    if algo == "blake3":
        return blake3.blake3()
    return hashlib.new(algo)


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


def parse_any_sha256(text: str) -> bytes:
    """`Hash::parseAny(text, SHA256)`: the forms a NAR hash takes on the wire.

    Base16 from a daemon, Nix32 from a narinfo, Base64, each with or without
    `sha256:`, or SRI. `ValueError` for anything else.
    """
    if text.startswith("sha256-"):
        return base64.b64decode(text.removeprefix("sha256-"), validate=True)
    return parse_digest(text.removeprefix("sha256:"), "sha256")


def parse_method_algo(text: str, features: frozenset[str]) -> tuple[Method, str]:
    """`ContentAddressMethod::parseWithAlgo` (`content-address.cc:203`): `fixed:r:sha256` and the like.

    `ValueError` in Nix's words for any form Nix refuses, and for a feature *features* lacks. Nix appends
    a `:` and parses up to the algorithm, so it ignores what follows it.
    """
    whole = f"{text}:"
    prefix, _, rest = whole.partition(":")
    if prefix == "text":
        method = Method.TEXT
    elif prefix == "fixed":
        method = next((m for m in (Method.NAR, Method.GIT) if rest.startswith(m)), Method.FLAT)
        if method is Method.GIT and "git-hashing" not in features:
            raise ValueError(missing_feature("git-hashing"))
        rest = rest.removeprefix(method)
    else:
        raise ValueError(
            f"content address prefix '{magenta(prefix)}' is unrecognized. Recogonized prefixes are 'text' or 'fixed'"
        )
    algo, sep, _ = rest.partition(":")
    if not sep:
        raise ValueError(f"content address hash must be in form '<algo>:<hash>', but found: {magenta(whole)}")
    # `parseHashAlgoOpt` (`hash.cc:472`).
    if algo == "blake3":
        if "blake3-hashes" not in features:
            raise ValueError(missing_feature("blake3-hashes"))
    elif algo not in HASH_SIZES:
        raise ValueError(
            f"unknown hash algorithm '{magenta(algo)}', expect 'blake3', 'md5', 'sha1', 'sha256', or 'sha512'"
        )
    return method, algo


def text_needs_sha256(name: str, algo: str) -> str:
    """The refusal of a text address with another algorithm than SHA-256, in the words of `remote-store.cc:363`.

    NIX-DEFECT (#23): the daemon (protocol >= 1.25) parses `text:sha1` and
    adds the dump; `makeFixedOutputPathFromCA` then asserts SHA-256
    (`store-dir-config.cc:137`), and the worker aborts. The client reads a
    closed connection, not an error. pynixd refuses with the client's own
    check (`remote-store.cc:363`) instead. Lillecarl/nix#414.
    """
    return f"When adding text-hashed data called '{magenta(name)}', only SHA-256 is supported but '{magenta(algo)}' was given"


def output_method(method: str) -> tuple[Method, str]:
    """The method and algorithm a content-addressed output of a derivation names: `r:sha256`, say."""
    found = next((m for m in (Method.TEXT, Method.NAR, Method.GIT) if method.startswith(m)), Method.FLAT)
    return found, method.removeprefix(found)


def fixed_output_address(method: str, hash_text: str) -> ContentAddress:
    """The address a fixed output of a derivation names: `r:sha256` and its hash, say."""
    found, algo = output_method(method)
    return ContentAddress(found, algo, parse_digest(hash_text, algo))


MAX_NAME_LEN = 211
"""`StorePath::MaxPathLen`."""


def check_name(name: str) -> None:
    """`checkName` (`path.cc:12`), with Nix's messages: `ValueError` for a name Nix refuses."""
    if not name:
        raise ValueError("name must not be empty")
    if len(name) > MAX_NAME_LEN:
        raise ValueError(f"name '{magenta(name)}' must be no longer than {magenta(MAX_NAME_LEN)} characters")
    if name in {".", ".."}:
        raise ValueError(f"name '{magenta(name)}' is not valid")
    for dots in (".", ".."):
        if name.startswith(dots + "-"):
            raise ValueError(
                f"name '{magenta(name)}' is not valid: first dash-separated component must not be '{magenta(dots)}'"
            )
    for char in name:
        if not (char.isascii() and (char.isalnum() or char in "+-._?=")):
            raise ValueError(f"name '{magenta(name)}' contains illegal character '{magenta(char)}'")


def make_store_path(kind: str, algo: str, digest: bytes, name: str, store_dir: str) -> str:
    """`StoreDirConfig::makeStorePath`. Like the `StorePath` it makes, it refuses a name `checkName` refuses."""
    clear = f"{kind}:{algo}:{digest.hex()}:{store_dir}:{name}"
    base = f"{nix32_encode(compress_hash(hashlib.sha256(clear.encode()).digest(), 20))}-{name}"
    try:
        check_name(name)
    except ValueError as error:
        # `checkPathName` (`path.cc:40`).
        raise ValueError(f"path '{magenta(base)}' is not a valid store path: {error}") from None
    return f"{store_dir}/{base}"


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
            raise ValueError(text_needs_sha256(name, ca.algo))
        if self_reference:
            raise ValueError("self-reference not allowed with text hashing")
        return make_store_path(_make_type("text", references, False), ca.algo, ca.digest, name, store_dir)
    if ca.method is Method.GIT and ca.algo not in ("sha1", "sha256"):
        raise ValueError(f"Git file ingestion must use SHA-1 or SHA-256 hash, but instead using: {magenta(ca.algo)}")
    if ca.algo == "sha256" and ca.method is Method.NAR:
        return make_store_path(_make_type("source", references, self_reference), ca.algo, ca.digest, name, store_dir)
    if references or self_reference:
        raise ValueError(
            f"fixed output derivation '{magenta(name)}' is not allowed to refer to other store paths.\n"
            "You may need to use the 'unsafeDiscardReferences' derivation attribute, see the manual for more details."
        )
    payload = f"fixed:out:{ca.method}{ca.algo}:{ca.digest.hex()}:"
    return make_store_path("output:out", "sha256", hashlib.sha256(payload.encode()).digest(), name, store_dir)


def store_path_name(path: str) -> str:
    """The name of a printed store path: what follows the hash part."""
    return path.rsplit("/", 1)[-1].split("-", 1)[1]


def is_content_addressed(path: str, ca: str, references: Iterable[str], store_dir: str) -> bool:
    """`ValidPathInfo::isContentAddressed` at `path-info.cc:108`: whether *ca* and *references* give *path*.

    A malformed *ca* raises `ValueError`; an address that Nix cannot turn into a path is `False`.
    """
    if not ca:
        return False
    address = ContentAddress.parse(ca)
    references = set(references)
    try:
        expected = content_addressed_path(
            address, store_path_name(path), references - {path}, store_dir, self_reference=path in references
        )
    except ValueError:
        return False
    return expected == path
