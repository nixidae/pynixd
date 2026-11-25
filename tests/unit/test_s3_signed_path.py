"""The path pynixd signs and sends is the one a server's SigV4 check computes (nixidae/nixidae#1589).

The check is this file's own. moto verifies with botocore, which signs a path as it is given, so moto accepts a path
that a real server refuses.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import TYPE_CHECKING
from urllib.parse import quote, unquote

import pytest
from aiohttp import web

from nix_daemon_protocol.ids import StoreId
from pynixd.config import S3BinaryCacheSpec

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

SECRET = "secret"
NAME = "build-trace-v2/00000000000000000000000000000000-toml_edit-0.25.13+spec-1.1.0.drv/out.doi"


def _signature(request: web.Request, body: bytes) -> str:
    """The signature that the AWS documentation defines, over the path encoded by this server."""
    credential, signed_headers, _ = (
        part.partition("=")[2]
        for part in request.headers["Authorization"].removeprefix("AWS4-HMAC-SHA256 ").split(", ")
    )
    _key, day, region, service, _ = credential.split("/")
    names = signed_headers.split(";")
    canonical = "\n".join(
        [
            request.method,
            quote(unquote(request.rel_url.raw_path), safe="/-_.~"),
            request.rel_url.raw_query_string,
            *(f"{name}:{request.headers[name].strip()}" for name in names),
            "",
            signed_headers,
            hashlib.sha256(body).hexdigest(),
        ]
    )
    scope = f"{day}/{region}/{service}/aws4_request"
    digest = hashlib.sha256(canonical.encode()).hexdigest()
    text = f"AWS4-HMAC-SHA256\n{request.headers['X-Amz-Date']}\n{scope}\n{digest}"
    key = f"AWS4{SECRET}".encode()
    for part in (day, region, service, "aws4_request"):
        key = hmac.digest(key, part.encode(), "sha256")
    return hmac.new(key, text.encode(), "sha256").hexdigest()


@pytest.fixture
async def endpoint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> AsyncIterator[str]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET)
    monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))

    async def answer(request: web.Request) -> web.Response:
        sent = request.headers["Authorization"].rpartition("Signature=")[2]
        if sent != _signature(request, await request.read()):
            return web.Response(status=403)
        if request.path.endswith("nix-cache-info"):
            return web.Response(text="StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n")
        return web.Response(text=f"found {request.path}")

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", answer)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    yield f"http://127.0.0.1:{port}"
    await runner.cleanup()


@pytest.mark.parametrize("name", [NAME, "log/00000000000000000000000000000000-a=b.drv", "plain.narinfo"])
async def test_a_server_that_checks_the_canonical_path_accepts_the_request(endpoint: str, name: str) -> None:
    spec = S3BinaryCacheSpec(
        store_id=StoreId("cache"), bucket="cache", endpoint=endpoint, region="eu-north-1", addressing_style="path"
    )
    async with spec.to_store("cache") as store:
        assert await store._get_file(name) == f"found /cache/{name}"
