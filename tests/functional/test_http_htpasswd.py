"""Functional tests for HTTP binary cache htpasswd authentication."""

from __future__ import annotations

from pathlib import Path

import aiohttp
from passlib.apache import HtpasswdFile

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store import LocalSocketStore
from tests.conftest import make_test_spec


async def test_htpasswd_auth(tmp_path: Path) -> None:
    """Test HTTP cache authentication using an htpasswd file.

    Store operations triggered:
    - None: This test only checks HTTP authentication without triggering Store operations
    """
    htpasswd_path = tmp_path / "htpasswd"
    ht = HtpasswdFile(str(htpasswd_path), new=True)
    ht.set_password("alice", "password123")
    ht.set_password("bob", "secret456")
    ht.save()

    local_store = LocalSocketStore(
        make_test_spec(daemon=True, store_id="local", store_path=Path("/"), no_probe=True),
    )

    async with Server(
        stores={StoreId("local"): local_store},
        http_port=0,
        http_htpasswd=htpasswd_path,
    ) as server:
        base_url = f"http://127.0.0.1:{server.http_bound_port}"

        async with aiohttp.ClientSession() as session:
            # 1. No auth -> 401
            async with session.get(f"{base_url}/nix-cache-info") as resp:
                assert resp.status == 401
                assert "WWW-Authenticate" in resp.headers

            # 2. Valid auth (alice) -> 200
            auth = aiohttp.BasicAuth("alice", "password123")
            async with session.get(f"{base_url}/nix-cache-info", auth=auth) as resp:
                assert resp.status == 200
                text = await resp.text()
                assert "StoreDir: /nix/store" in text

            # 3. Valid auth (bob) -> 200
            auth = aiohttp.BasicAuth("bob", "secret456")
            async with session.get(f"{base_url}/nix-cache-info", auth=auth) as resp:
                assert resp.status == 200

            # 4. Invalid password -> 403
            auth = aiohttp.BasicAuth("alice", "wrong")
            async with session.get(f"{base_url}/nix-cache-info", auth=auth) as resp:
                assert resp.status == 403

            # 5. Invalid user -> 403
            auth = aiohttp.BasicAuth("charlie", "password123")
            async with session.get(f"{base_url}/nix-cache-info", auth=auth) as resp:
                assert resp.status == 403


async def test_htpasswd_fallback_to_single_user(tmp_path: Path) -> None:
    """Test that htpasswd takes precedence but single user still works if no htpasswd.

    Store operations triggered:
    - None: This test only checks HTTP authentication fallback without triggering Store operations
    """
    local_store = LocalSocketStore(
        make_test_spec(daemon=True, store_id="local", store_path=Path("/"), no_probe=True),
    )

    # No htpasswd, just single user/pass
    async with Server(
        stores={StoreId("local"): local_store},
        http_port=0,
        http_user="admin",
        http_pass="password",
    ) as server:
        base_url = f"http://127.0.0.1:{server.http_bound_port}"
        async with aiohttp.ClientSession() as session:
            auth = aiohttp.BasicAuth("admin", "password")
            async with session.get(f"{base_url}/nix-cache-info", auth=auth) as resp:
                assert resp.status == 200

    # Both htpasswd and single user/pass. htpasswd should win.
    htpasswd_path = tmp_path / "htpasswd"
    ht = HtpasswdFile(str(htpasswd_path), new=True)
    ht.set_password("alice", "password123")
    ht.save()

    async with Server(
        stores={StoreId("local"): local_store},
        http_port=0,
        http_user="admin",
        http_pass="password",
        http_htpasswd=htpasswd_path,
    ) as server:
        base_url = f"http://127.0.0.1:{server.http_bound_port}"
        async with aiohttp.ClientSession() as session:
            # alice works
            auth = aiohttp.BasicAuth("alice", "password123")
            async with session.get(f"{base_url}/nix-cache-info", auth=auth) as resp:
                assert resp.status == 200

            # admin should NOT work because htpasswd is present and it doesn't contain admin
            auth = aiohttp.BasicAuth("admin", "password")
            async with session.get(f"{base_url}/nix-cache-info", auth=auth) as resp:
                assert resp.status == 403
