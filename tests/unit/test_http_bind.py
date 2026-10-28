"""Which address families the HTTP server answers on.

The probes nixkube renders are `httpGet` on this port, and a single-stack
IPv6 cluster dials the Pod's v6 address. An IPv4-only bind answers that with
ECONNREFUSED, so the probe never passes and the container restarts for ever.
Measured on a live cluster (nixlab2, cluster-cidr `2a01:...::/80`): the HTTP
server held `0.0.0.0:8080` while the SSH server held `:::22`, and the startup
probe failed 61 times in 14 minutes.
"""

from __future__ import annotations

import socket

import aiohttp

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.config import LocalSocketStoreSpec, PynixdSettings
from pynixd.http_server import PynixdHttpServer
from pynixd.store.local_daemon import LocalStore


def _server() -> PynixdHttpServer:
    spec = LocalSocketStoreSpec(store_id=LOCAL_STORE_ID, monitor=False, use_db=False)
    return PynixdHttpServer(LocalStore(spec), enable_metrics=True)


def _free_port() -> int:
    """A port both families can take.

    Not `port=0`: with an every-interface host the server binds one socket per
    family, and each gets its own ephemeral port. One number, asked for
    explicitly, is the same on both.
    """
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as probe:
        probe.bind(("::", 0))
        port: int = probe.getsockname()[1]
        return port


async def _answers(host: str, port: int) -> bool:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"http://{host}:{port}/metrics") as response:
                return response.status == 200
        except aiohttp.ClientConnectorError:
            return False


class TestTheDefaultBind:
    def test_the_default_is_every_interface(self) -> None:
        """`""`, which asyncio reads as every interface of every family.
        `"0.0.0.0"` is the value that caused the crash loop."""
        assert PynixdSettings().http_host == ""

    async def test_it_answers_over_ipv6(self) -> None:
        port = _free_port()
        runner, _ = await _server().start(host=PynixdSettings().http_host, port=port)
        try:
            assert await _answers("[::1]", port)
        finally:
            await runner.cleanup()

    async def test_it_still_answers_over_ipv4(self) -> None:
        port = _free_port()
        runner, _ = await _server().start(host=PynixdSettings().http_host, port=port)
        try:
            assert await _answers("127.0.0.1", port)
        finally:
            await runner.cleanup()

    async def test_an_ipv4_bind_refuses_ipv6(self) -> None:
        """The negative control. Without it the two tests above pass on a host
        whose IPv6 loopback happens to reach an IPv4 socket."""
        port = _free_port()
        runner, _ = await _server().start(host="0.0.0.0", port=port)
        try:
            assert not await _answers("[::1]", port)
        finally:
            await runner.cleanup()
