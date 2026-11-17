"""A provisioned builder registers by its join token. nixidae/nixidae#1198."""

from __future__ import annotations

import asyncssh

from nix_daemon_protocol.ids import StoreId
from pynixd.reverse_client import _ReverseSSHServer
from pynixd.reverse_server import Admitted, admit, reverse_spec


def _key() -> asyncssh.SSHKey:
    return asyncssh.import_public_key(asyncssh.generate_private_key("ssh-ed25519").export_public_key())


class _Joiner:
    def __init__(self, tokens: dict[str, str]) -> None:
        self.tokens = tokens
        self.asked: list[str] = []

    async def join(self, token: str) -> Admitted | None:
        self.asked.append(token)
        store_id = self.tokens.pop(token, None)
        return None if store_id is None else Admitted(StoreId(store_id), frozenset({"aws"}))

    async def left(self, store_id: StoreId) -> None:
        del store_id


async def test_a_live_token_admits_the_builder_under_its_instance_id() -> None:
    joiner = _Joiner({"good": "provisioned-p1"})
    reg: dict[str, object] = {"store_id": "the-local-store", "join_token": "good"}

    assert await admit(reg, _key(), [_key()], joiner, "fallback") == Admitted(
        StoreId("provisioned-p1"), frozenset({"aws"})
    )
    assert await admit(reg, _key(), [], joiner, "fallback") is None, "a token admitted twice"


async def test_a_wrong_token_is_refused_even_from_a_pinned_key() -> None:
    pinned = _key()
    joiner = _Joiner({"good": "provisioned-p1"})

    assert await admit({"store_id": "b", "join_token": "bad"}, pinned, [pinned], joiner, "fallback") is None
    assert joiner.asked == ["bad"]


async def test_a_token_at_an_acceptor_that_provisions_nothing_is_refused() -> None:
    assert await admit({"join_token": "good"}, None, [], None, "fallback") is None


async def test_without_a_token_the_key_must_be_pinned() -> None:
    pinned = _key()
    joiner = _Joiner({})

    assert await admit({"store_id": "b"}, _key(), [pinned], joiner, "fallback") is None
    assert await admit({"store_id": "b"}, pinned, [pinned], joiner, "fallback") == Admitted(StoreId("b"))
    assert await admit({}, _key(), [], None, "fallback") == Admitted(StoreId("fallback"))


def test_a_ticketed_builder_serves_only_the_controller_it_names() -> None:
    controller = _key()
    named = _ReverseSSHServer(controller)

    assert named.validate_public_key("controller", controller)
    assert not named.validate_public_key("controller", _key())
    assert _ReverseSSHServer().validate_public_key("controller", _key())


def test_a_provisioned_store_carries_its_pools_mandatory_features() -> None:
    spec = reverse_spec({"systems": ["x86_64-linux"], "system_features": ["aws"]}, "provisioned-p1", frozenset({"aws"}))
    assert spec.mandatory_features == {"aws"}
    assert reverse_spec({}, "b").mandatory_features == set()
