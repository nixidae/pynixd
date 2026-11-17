"""What a provisioner hands a builder it launches, as the machine's user data. nixidae/nixidae#1198."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


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
