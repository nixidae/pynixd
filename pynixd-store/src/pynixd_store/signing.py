"""Nix-compatible Ed25519 path signing.

Uses the same key format and fingerprint construction as Nix:
- Keys: ``<name>:<base64>`` (secret key = 32-byte seed, public key = 32 bytes)
- Signatures: ``<name>:<base64>`` (64-byte Ed25519 detached signature)
- Fingerprint: ``1;<store-path>;sha256:<nix32-hash>;<nar-size>;<refs>``
"""

from __future__ import annotations

import json
import os
import sys
from base64 import b64decode, b64encode
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import nacl.bindings
import nacl.exceptions
import nacl.signing

from .content_address import is_content_addressed
from .hintfmt import magenta
from .utils import nix32_encode

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from nix_daemon_protocol.nar_hash import NARHash
    from nix_daemon_protocol.signature import Signature
    from nix_daemon_protocol.store_path import StorePath
    from nix_daemon_protocol.valid_path_info import ValidPathInfo


# Nix32 alphabet (kept for reference; encoding is in utils.py)


def get_default_signing_key() -> SecretKey | None:
    """Get the default signing key from PYNIXD_SIGNING_KEY env var."""
    val = os.environ.get("PYNIXD_SIGNING_KEY", "")
    if not val:
        return None
    return SecretKey.from_string(val)


@dataclass
class SecretKey:
    """An Ed25519 secret key in Nix format.

    The key file contains ``<name>:<base64>`` where the base64 decodes to
    a 32-byte Ed25519 seed.
    """

    _signing_key: nacl.signing.SigningKey = field(repr=False)
    name: str = ""

    @classmethod
    def from_file(cls, path: Path) -> SecretKey:
        """Load a secret key from a Nix-format file."""
        text = path.read_text().strip()
        return cls._parse(text)

    @classmethod
    def from_string(cls, text: str) -> SecretKey:
        """Parse a secret key from a Nix-format string."""
        return cls._parse(text.strip())

    @classmethod
    def _parse(cls, text: str) -> SecretKey:
        split = text.split(":", 1)
        name = split[0]
        raw = b64decode(split[1])
        if len(raw) == 64:
            # Full libsodium secret key (seed + public key)
            seed = raw[:32]
        elif len(raw) == 32:
            # Seed-only
            seed = raw
        else:
            raise ValueError(f"Secret key must be 32 or 64 bytes, got {len(raw)}")
        return cls(
            _signing_key=nacl.signing.SigningKey(seed),
            name=name,
        )

    @property
    def public_key_bytes(self) -> bytes:
        """The 32-byte Ed25519 public key."""
        return bytes(self._signing_key.verify_key)

    def sign(self, data: bytes) -> bytes:
        """Sign data, returning a 64-byte Ed25519 signature."""
        return self._signing_key.sign(data).signature

    def sign_fingerprint(self, fingerprint: str) -> str:
        """Sign a fingerprint string, return ``<name>:<base64>`` signature."""
        sig = self.sign(fingerprint.encode("utf-8"))
        return f"{self.name}:{b64encode(sig).decode()}"

    def public_key_string(self) -> str:
        """Return the public key in Nix ``<name>:<base64>`` format."""
        return f"{self.name}:{b64encode(self.public_key_bytes).decode()}"


def nar_hash_for_a_fingerprint(nar_hash: NARHash) -> str:
    """The NAR hash as a fingerprint of Nix writes it.

    `ValidPathInfo::fingerprint` at `path-info.cc:48` writes
    `narHash.to_string(HashFormat::Nix32, true)`: the name of the algorithm, a
    colon, and the digest in the base-32 alphabet of Nix.

    **The wire carries base 16 and no name**, at `worker-protocol.cc:356`, so
    the value that reaches pynixd is in the other form and this converts it. A
    fingerprint over the base-16 digest is a different string, so a verifier
    of Nix reads the signature as false.

    **The name is always `sha256`, so this writes it rather than reading
    one.** The read side of the protocol fixes the algorithm:
    `Hash::parseAny(readString(conn.from), HashAlgorithm::SHA256)`,
    `src/libstore/worker-protocol.cc:424` of Nix. `NARHash` holds the digest
    alone and takes off a name that a source put in front, so no value that
    reaches here carries one. This used to parse for a name anyway, and the
    only callers that could reach that branch were its own tests.

    A value that is already in the base-32 form passes through, and so does an
    empty one: a path with no NAR hash has no fingerprint to sign, and the
    caller decides what to do about that.
    """
    digest = str(nar_hash)
    if not digest:
        return ""
    try:
        raw = bytes.fromhex(digest)
    except ValueError:
        # Not base 16, so it is the base-32 form already.
        return f"sha256:{digest}"
    return f"sha256:{nix32_encode(raw)}"


def fingerprint(
    store_path: StorePath | str,
    nar_hash: NARHash,
    nar_size: int,
    references: Iterable[StorePath | str],
) -> str:
    """Build the Nix fingerprint string that gets signed.

    Format: ``1;<store-path>;sha256:<nix32-hash>;<nar-size>;<comma-separated-refs>``

    `ValidPathInfo::fingerprint` at `path-info.cc:48` of Nix. The references
    are the store paths, sorted, and separated by a comma.

    **A path reaches here in either form.** `sign_path_info` passes the
    `StorePath` of a `ValidPathInfo`, `DaemonStore._sign` passes the printed
    string, and `tests/parity` passes what `nix store add-file` wrote. Both
    forms print the whole path, which is what the fingerprint carries.
    """
    refs = ",".join(sorted(str(r) for r in references))
    return f"1;{store_path};{nar_hash_for_a_fingerprint(nar_hash)};{nar_size};{refs}"


def realisation_fingerprint(drv_name: str, output_name: str, out_name: str) -> str:
    """`UnkeyedRealisation::fingerprint` (`realisation.cc:36`): the realisation's JSON without its signatures.

    Each path is a base name, as `StorePath`'s JSON is. nlohmann orders the keys.
    """
    key = f'{{"drvPath":{json.dumps(drv_name)},"outputName":{json.dumps(output_name)}}}'
    return f'{{"key":{key},"value":{{"outPath":{json.dumps(out_name)}}}}}'


def hash_keyed_realisation_fingerprint(drv_hash: str, output_name: str, out_name: str) -> str:
    """Nix 2.34's `UnkeyedRealisation::fingerprint` (`realisation.cc:28` of 2.34): `Realisation`'s JSON, no signatures.

    *drv_hash* is `sha256:<hex>`. `dependentRealisations` is always empty (`realisation.cc:105` of 2.34).
    """
    drv_output = json.dumps(f"{drv_hash}!{output_name}")
    return f'{{"dependentRealisations":{{}},"id":{drv_output},"outPath":{json.dumps(out_name)}}}'


def sign_path_info(
    key: SecretKey,
    info: ValidPathInfo,
) -> str:
    """Sign a path info and return the signature string.

    Args:
        key: The signing key
        info: The path info to sign

    Returns:
        Signature in ``<name>:<base64>`` format
    """
    fp = fingerprint(
        info.path,
        info.info.nar_hash,
        info.info.nar_size,
        info.info.references,
    )
    return key.sign_fingerprint(fp)


#: `ValidPathInfo::maxSigs`: what a content-addressed path counts as, since it needs no key.
MAX_SIGS = sys.maxsize

_BASE64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
_BASE64_DIGITS = {c: i for i, c in enumerate(_BASE64)}


def decode_base64(text: str) -> bytes:
    """`base64::decode` at `base-n.cc:76`: stops at the first `=`, skips newlines, refuses anything else.

    Not `b64decode`, which drops every character outside the alphabet and so accepts a key Nix refuses.
    """
    out = bytearray()
    bits = acc = 0
    for c in text:
        if c == "=":
            break
        if c == "\n":
            continue
        digit = _BASE64_DIGITS.get(c)
        if digit is None:
            raise ValueError(f"invalid character in Base64 string: '{magenta(c)}'")
        acc = (acc << 6 | digit) & 0xFFFF
        bits += 6
        if bits >= 8:
            out.append(acc >> (bits - 8) & 0xFF)
            bits -= 8
    return bytes(out)


def _parse_colon_base64(text: str, kind: str) -> tuple[str, bytes]:
    """`parseColonBase64` at `local-keys.cc:21`."""
    name, sep, data = text.partition(":")
    if not sep or not name:
        raise ValueError(f"{magenta(kind)} is corrupt")
    raw = decode_base64(data)
    if not raw:
        raise ValueError(f"{magenta(kind)} is corrupt")
    return name, raw


@dataclass(frozen=True)
class PublicKey:
    """An Ed25519 public key in Nix's `<name>:<base64>` form (`local-keys.cc:139`)."""

    name: str
    key: bytes

    @classmethod
    def parse(cls, text: str) -> PublicKey:
        name, raw = _parse_colon_base64(text, "key")
        if len(raw) != nacl.bindings.crypto_sign_PUBLICKEYBYTES:
            raise ValueError("public key is not valid")
        return cls(name, raw)

    def verify(self, data: bytes, signature: bytes) -> bool:
        """`PublicKey::verifyDetachedAnon` at `local-keys.cc:154`; the key name is not compared."""
        if len(signature) != nacl.bindings.crypto_sign_BYTES:
            raise ValueError("signature is not valid")
        try:
            nacl.signing.VerifyKey(self.key).verify(data, signature)
        except nacl.exceptions.BadSignatureError:
            return False
        return True


def public_keys(trusted_public_keys: Iterable[str], secret_key_files: Iterable[str]) -> dict[str, PublicKey]:
    """`getDefaultPublicKeys` at `keys.cc:7`. The first key of a name wins, as `std::map::emplace` keeps it.

    A secret key file pynixd cannot read is skipped, as Nix skips one: normal in a multi-user install.
    """
    keys: dict[str, PublicKey] = {}
    for text in trusted_public_keys:
        key = PublicKey.parse(text)
        keys.setdefault(key.name, key)
    for file in secret_key_files:
        try:
            text = Path(file).read_text()
        except OSError:
            continue
        name, raw = _parse_colon_base64(text, "key")
        if len(raw) != nacl.bindings.crypto_sign_SECRETKEYBYTES:
            raise ValueError("secret key is not valid")
        # `crypto_sign_ed25519_sk_to_pk` copies the second half; it derives nothing from the seed.
        keys.setdefault(name, PublicKey(name, raw[nacl.bindings.crypto_sign_SEEDBYTES :]))
    return keys


_KEY_SETTINGS = {
    "trusted-public-keys": "trusted-public-keys",
    "binary-cache-public-keys": "trusted-public-keys",
    "secret-key-files": "secret-key-files",
}


KeyOverrides = tuple[tuple[str, str], ...]
"""A client's overrides of the settings `key_settings` reads, in name order."""


def key_overrides(overrides: Mapping[str, str]) -> KeyOverrides:
    """The overrides of *overrides* that `key_settings` reads: two clients that differ here trust different keys."""
    return tuple(sorted((k, v) for k, v in overrides.items() if k.removeprefix("extra-") in _KEY_SETTINGS))


def key_settings(
    trusted_public_keys: Sequence[str], secret_key_files: Sequence[str], overrides: Mapping[str, str]
) -> tuple[list[str], list[str]]:
    """The two settings `getDefaultPublicKeys` reads, after a client's `SetOptions`.

    `ClientSettings::apply` (`daemon.cc:233`) walks the overrides in name order
    and calls `settings.set`, so `extra-` appends and a plain name replaces. An
    untrusted client's overrides reach here filtered already (`restrict_overrides`).
    """
    values = {"trusted-public-keys": list(trusted_public_keys), "secret-key-files": list(secret_key_files)}
    for name, value in sorted(overrides.items()):
        target = _KEY_SETTINGS.get(name.removeprefix("extra-"))
        if target is None:
            continue
        given = value.split()
        values[target] = [*values[target], *given] if name.startswith("extra-") else given
    return values["trusted-public-keys"], values["secret-key-files"]


def _verify_detached(data: bytes, signature: Signature, keys: Mapping[str, PublicKey]) -> bool:
    """`verifyDetached` at `local-keys.cc:167`."""
    _, raw = _parse_colon_base64(str(signature), "signature")
    key = keys.get(signature.name)
    return key is not None and key.verify(data, raw)


def check_signatures(
    info: ValidPathInfo,
    keys: Mapping[str, PublicKey],
    store_dir: str,
    warn: Callable[[str], None] | None = None,
) -> int:
    """`ValidPathInfo::checkSignatures` at `path-info.cc:125`: how many signatures *keys* vouch for.

    A content-addressed path counts as `MAX_SIGS` with no signature at all.
    *warn* takes the message `isContentAddressed` prints for a false address.
    """
    path = str(info.path)
    unkeyed = info.info
    references = [str(ref) for ref in unkeyed.references]
    if unkeyed.ca:
        if is_content_addressed(path, str(unkeyed.ca), references, store_dir):
            return MAX_SIGS
        if warn is not None:
            # `printError` formats with plain `fmt`: no colour.
            warn(f"warning: path '{path}' claims to be content-addressed but isn't")
    if not unkeyed.sigs:
        return 0
    if unkeyed.nar_size == 0:
        raise ValueError(f"cannot calculate fingerprint of path '{magenta(path)}' because its size is not known")
    data = fingerprint(path, unkeyed.nar_hash, unkeyed.nar_size, references).encode()
    return sum(1 for sig in unkeyed.sigs if _verify_detached(data, sig, keys))
