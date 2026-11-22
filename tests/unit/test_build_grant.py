"""A build pod's key reads the store and adds its derivation's outputs, and nothing else. nixidae/nixidae#1560, spike."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

import asyncssh
import pytest

from nix_daemon_protocol.add_multiple_to_store import path_info_version
from nix_daemon_protocol.add_to_store_nar import AddToStoreNarRequest
from nix_daemon_protocol.build_derivation import BuildDerivationRequest
from nix_daemon_protocol.constants import PROTOCOL_VERSION
from nix_daemon_protocol.content_address import ContentAddress
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_path import StorePath
from nix_daemon_protocol.valid_path_info import ValidPathInfo
from pynixd.build_grant import BuildGrant
from pynixd.config import PynixdSettings
from pynixd.connection import Connection
from pynixd.context import PynixdContext
from pynixd.exceptions import ClosingError
from pynixd.handlers.add_multiple_to_store import _paths
from pynixd.handlers.add_to_store_nar import AddToStoreNarHandler
from pynixd.serde.auth import Role
from pynixd.serde.context import ReadContext, WriteContext
from pynixd.ssh_server import start_ssh_server
from pynixd.wire import BytesReader, BytesWriter, FramedReader, SSHNixReader, SSHNixWriter
from pynixd_store.content_address import ContentAddress as StoreContentAddress, content_addressed_path
from pynixd_store.utils import nix32_encode
from tests.functional.mock_store import MockStore
from tests.unit.test_untrusted_repair import PATH, Upstream, _info

if TYPE_CHECKING:
    from nix_daemon_protocol.wire_ops import WireRequest

OUTPUT = "/nix/store/00000000000000000000000000000002-out"
GRANT = BuildGrant(drv_path="/nix/store/00000000000000000000000000000003-x.drv", outputs=frozenset({OUTPUT}))


async def _framed(request: WireRequest, frames: bytes) -> bytes:
    writer = BytesWriter("test")
    await request.to_writer(WriteContext(writer=writer, version=PROTOCOL_VERSION))
    return writer.get_bytes()[8:] + frames


def _frame(data: bytes) -> bytes:
    return len(data).to_bytes(8, "little") + data


async def _handle(handler: Any, body: bytes, upstream: Upstream) -> None:
    proxy = SimpleNamespace(
        r=BytesReader(body, "test"),
        version=PROTOCOL_VERSION,
        standard_features=frozenset(),
        client=None,
        local_store=upstream,
        store_for_output_path=lambda _path: None,
        metrics_enabled=False,
        grant=GRANT,
    )
    proxy.read_context = ReadContext(reader=proxy.r, version=proxy.version, features=proxy.standard_features)
    await handler.handle(cast("Any", SimpleNamespace(proxy=proxy, role=Role.USER)))


def test_a_grant_refuses_an_op_that_builds_and_admits_an_add() -> None:
    with pytest.raises(ClosingError, match="may not run operation 36"):
        GRANT.check_op(BuildDerivationRequest.op)
    GRANT.check_op(AddToStoreNarRequest.op)


@pytest.mark.anyio
async def test_a_nar_of_another_path_is_refused_before_the_daemon_sees_it() -> None:
    upstream = Upstream()
    body = await _framed(AddToStoreNarRequest(info=_info(), repair=0, dont_check_sigs=0), _frame(b""))

    with pytest.raises(ClosingError, match="it is not an output"):
        await _handle(AddToStoreNarHandler(), body, upstream)

    assert upstream.written == b""


@pytest.mark.anyio
async def test_a_stream_with_another_path_is_refused_before_its_nar() -> None:
    version, features = path_info_version(PROTOCOL_VERSION, frozenset())
    writer = BytesWriter("test")
    await _info().to_writer(WriteContext(writer=writer, version=version, features=features))
    frames = FramedReader(BytesReader(_frame(writer.get_bytes() + b"\0" * 8) + _frame(b""), "test"))
    yielded = []

    with pytest.raises(ClosingError, match=f"may not add {PATH}"):
        async for path in _paths(frames, 1, version, features, permit=GRANT.check_add):
            yielded.append(path)

    assert yielded == []


def _text_addressed(name: str, references: frozenset[str] = frozenset()) -> ValidPathInfo:
    """A text-addressed path, as `nix-store --add` in a recursive build makes one, and its info."""
    ca = f"text:sha256:{nix32_encode(hashlib.sha256(name.encode()).digest())}"
    path = content_addressed_path(StoreContentAddress.parse(ca), name, references, "/nix/store")
    info = _info().info.model_copy(update={"ca": ContentAddress(ca), "references": set(map(StorePath, references))})
    return ValidPathInfo(path=StorePath(path), info=info)


def test_a_grant_admits_a_path_its_content_address_names() -> None:
    GRANT.check_add(_text_addressed("added"))
    GRANT.check_add(_text_addressed("refers", frozenset({OUTPUT})))


def test_a_grant_refuses_an_address_that_names_another_path() -> None:
    # The files may match the address, and the name still claims a path the address does not give.
    claimed = _text_addressed("added").model_copy(update={"path": PATH})
    with pytest.raises(ClosingError, match="not content-addressed"):
        GRANT.check_add(claimed)
    malformed = claimed.model_copy(update={"info": claimed.info.model_copy(update={"ca": ContentAddress("x:y")})})
    with pytest.raises(ClosingError, match="not content-addressed"):
        GRANT.check_add(malformed)


@pytest.mark.anyio
async def test_a_granted_key_logs_in_as_a_user_with_its_grant() -> None:
    store = MockStore("local", feature_matrix={"x86_64-linux": set()})
    ctx = PynixdContext(settings=PynixdSettings(), _stores={StoreId("local"): store})
    key = asyncssh.generate_private_key("ssh-ed25519")
    ctx.grants.grant(key.export_public_key().decode(), GRANT)
    server = await start_ssh_server(ctx, port=0)
    try:
        async with asyncssh.connect(
            "127.0.0.1", server.get_port(), username="pod", client_keys=[key], known_hosts=None, agent_path=None
        ) as ssh:
            process = await ssh.create_process("nix-daemon --stdio", encoding=None)
            conn = Connection(
                SSHNixReader(process.stdout, identifier="t"), SSHNixWriter(process.stdin, identifier="t"), "t"
            )
            await conn.connect()
            (proxy,) = ctx.sessions.proxies()
            assert (proxy.role, proxy.grant) == (Role.USER, GRANT)
            process.stdin.write_eof()

        ctx.grants.revoke(key.export_public_key().decode())
        with pytest.raises(asyncssh.PermissionDenied):
            await asyncssh.connect(
                "127.0.0.1", server.get_port(), username="pod", client_keys=[key], known_hosts=None, agent_path=None
            )
    finally:
        server.close()
        await server.wait_closed()
