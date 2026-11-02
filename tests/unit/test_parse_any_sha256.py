"""`Hash::parseAny` for a NAR hash: a narinfo gives Nix32, a daemon's wire Base16 (`worker-protocol.cc:372`).

The substitution import compared a narinfo's Nix32 digest with Base16 and
failed every path with "non-hexadecimal number found in fromhex()".
"""

from __future__ import annotations

import base64
import hashlib

import pytest

from pynixd.content_address import parse_any_sha256
from pynixd.utils import nix32_encode

DIGEST = hashlib.sha256(b"a NAR").digest()


@pytest.mark.parametrize(
    "text",
    [
        DIGEST.hex(),
        f"sha256:{DIGEST.hex()}",
        nix32_encode(DIGEST),
        f"sha256:{nix32_encode(DIGEST)}",
        base64.b64encode(DIGEST).decode(),
        f"sha256-{base64.b64encode(DIGEST).decode()}",
    ],
)
def test_every_form_gives_the_digest(text: str) -> None:
    assert parse_any_sha256(text) == DIGEST


@pytest.mark.parametrize("text", ["", "abc", "sha256:" + "z" * 64])
def test_a_malformed_hash_is_refused(text: str) -> None:
    with pytest.raises(ValueError, match=r"length|non-hexadecimal|character"):
        parse_any_sha256(text)
