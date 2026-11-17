"""What a provisioner hands a builder it launches, as the machine's user data. nixidae/nixidae#1198.

A provider whose user data is public seals the ticket: a GitHub Actions
`workflow_dispatch` input shows on the run's page (nixidae/nixidae#1473).
"""

from __future__ import annotations

import base64
import binascii
from typing import Final

import nacl.exceptions
import nacl.utils
from nacl.bindings import (
    crypto_aead_xchacha20poly1305_ietf_ABYTES,
    crypto_aead_xchacha20poly1305_ietf_decrypt,
    crypto_aead_xchacha20poly1305_ietf_encrypt,
    crypto_aead_xchacha20poly1305_ietf_KEYBYTES,
    crypto_aead_xchacha20poly1305_ietf_NPUBBYTES,
)
from pydantic import BaseModel, ConfigDict

SEALED_PREFIX: Final = "pynixd-ticket-v1:"
KEY_BYTES: Final = crypto_aead_xchacha20poly1305_ietf_KEYBYTES
_NONCE_BYTES: Final = crypto_aead_xchacha20poly1305_ietf_NPUBBYTES
_TAG_BYTES: Final = crypto_aead_xchacha20poly1305_ietf_ABYTES


class JoinTicket(BaseModel):
    """The builder reads this at each dial (`ReverseInitiatorSettings.join_ticket`)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    token: str
    """Admits one registration; the acceptor stores only its SHA-256."""
    acceptor_host: str | None = None
    acceptor_port: int | None = None
    controller_key: str | None = None
    """The acceptor's public key, OpenSSH format. The builder serves only it,
    so the token goes to no other party that answers the dial."""
    system_features: list[str] = []
    """The pool's features. A machine set up from the ticket, such as a cloud
    image, declares them; the builder's pynixd does not read them."""
    max_jobs: int | None = None
    """The pool's slots, for the same machine set-up."""


class SealError(ValueError):
    """A sealed ticket that does not open: a wrong key, another context, or a changed text."""


def ticket_key(text: str) -> bytes:
    """The shared key from its text form: base64 of `KEY_BYTES` random bytes, as a secret store holds it."""
    try:
        key = base64.b64decode(text.strip(), validate=True)
    except binascii.Error as error:
        raise SealError(f"the ticket key is not base64: {error}") from None
    if len(key) != KEY_BYTES:
        raise SealError(f"the ticket key holds {len(key)} bytes, not {KEY_BYTES}")
    return key


def seal(ticket: str, key: bytes, context: str) -> str:
    """*ticket* encrypted under *key* with XChaCha20-Poly1305, bound to *context*.

    The context is public: the launch's tags. A sealed ticket opens only with the same context, so
    one launch's ticket cannot be replayed in another launch's run.
    """
    nonce = nacl.utils.random(_NONCE_BYTES)
    sealed = crypto_aead_xchacha20poly1305_ietf_encrypt(ticket.encode(), context.encode(), nonce, key)
    return SEALED_PREFIX + base64.urlsafe_b64encode(nonce + sealed).decode()


def unseal(sealed: str, key: bytes, context: str) -> str:
    if not sealed.startswith(SEALED_PREFIX):
        raise SealError(f"a sealed ticket starts with {SEALED_PREFIX!r}")
    try:
        raw = base64.urlsafe_b64decode(sealed.removeprefix(SEALED_PREFIX))
    except binascii.Error as error:
        raise SealError(f"the sealed ticket is not base64: {error}") from None
    if len(raw) < _NONCE_BYTES + _TAG_BYTES:
        raise SealError("the sealed ticket is too short")
    nonce, body = raw[:_NONCE_BYTES], raw[_NONCE_BYTES:]
    try:
        plain = crypto_aead_xchacha20poly1305_ietf_decrypt(body, context.encode(), nonce, key)
    except nacl.exceptions.CryptoError:
        raise SealError("the sealed ticket does not open with this key and context") from None
    return plain.decode()
