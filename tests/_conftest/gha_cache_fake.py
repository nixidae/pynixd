"""A GitHub Actions cache service on localhost: the Twirp calls and the blob storage behind them.

It encodes pynixd's reading of the protocol (`@actions/cache`, `cacheTwirpClient.ts`, `cache.ts`),
so a pass here proves pynixd and this reading agree. Only a run on a GitHub runner proves the
reading. Replies carry the proto field names, as GitHub's service sends them (measured on a runner,
2026-11-16). nixidae/nixidae#1377.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from aiohttp import web
from aiohttp.test_utils import TestServer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

_SERVICE = "/twirp/github.actions.results.api.v1.CacheService/"


@dataclass
class _Entry:
    blocks: dict[str, bytes] = field(default_factory=dict)
    body: bytes | None = None
    final: bool = False


@dataclass
class FakeGhaCache:
    """The service's state: entries by key and version, and what each call was asked."""

    base: str = ""
    entries: dict[tuple[str, str], _Entry] = field(default_factory=dict)
    calls: Counter[str] = field(default_factory=Counter)
    limited: int = 0
    """How many Twirp calls to come answer 429."""
    retry_after: int = 1
    camel_case: bool = False
    """Answer the lowerCamelCase JSON names, which a proto3 JSON writer may also send."""
    orphans: set[str] = field(default_factory=set)
    """Keys a dead job created and never finalized, in every version: taken, and a miss."""

    def credentials(self, path: Path, mode: str = "write") -> Path:
        """Write the job's credentials for this service to *path*, as the ghanix step does."""
        path.write_text(
            json.dumps(
                {
                    "ACTIONS_RESULTS_URL": f"{self.base}/",
                    "ACTIONS_RUNTIME_TOKEN": "fake-token",
                    "ACTIONS_CACHE_SERVICE_V2": "True",
                    "ACTIONS_CACHE_MODE": mode,
                }
            )
        )
        return path

    def _reply(self, fields: dict[str, object]) -> web.Response:
        if self.camel_case:
            fields = {re.sub(r"_(.)", lambda m: m.group(1).upper(), k): v for k, v in fields.items()}
        return web.json_response(fields)

    def final_keys(self) -> set[str]:
        return {key for (key, _), entry in self.entries.items() if entry.final}

    def _url(self, key: str) -> str:
        expiry = (datetime.now(UTC) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"{self.base}/blob/{key}?sv=2023-11-03&se={expiry}&sig=secret"

    async def twirp(self, request: web.Request) -> web.Response:
        method = request.match_info["method"]
        self.calls[method] += 1
        if request.headers.get("Authorization") != "Bearer fake-token":
            return web.json_response({"code": "unauthenticated", "msg": "no token"}, status=401)
        if self.limited > 0:
            self.limited -= 1
            return web.json_response(
                {"code": "resource_exhausted", "msg": "rate limited"},
                status=429,
                headers={"Retry-After": str(self.retry_after)},
            )
        body = await request.json()
        key, version = body["key"], body["version"]
        entry = self.entries.get((key, version))
        if method == "CreateCacheEntry":
            if entry is not None or key in self.orphans:
                # GitHub's answer, finalized or not (measured, 2026-11-16).
                return web.json_response(
                    {
                        "code": "already_exists",
                        "msg": "cache entry with the same key, version, and scope already exists",
                    },
                    status=409,
                )
            self.entries[key, version] = _Entry()
            return self._reply({"ok": True, "signed_upload_url": self._url(key)})
        if method == "FinalizeCacheEntryUpload":
            if entry is None or entry.body is None or len(entry.body) != int(body["sizeBytes"]):
                return web.json_response({"ok": False, "message": "size mismatch"})
            entry.final = True
            return self._reply({"ok": True, "entry_id": "1"})
        if method == "GetCacheEntryDownloadURL":
            # The key itself first, then the newest entry a restore key is a prefix of (`cache.ts`).
            prefixes = body.get("restoreKeys", [])
            found = [
                k
                for (k, v), e in self.entries.items()
                if v == version and e.final and (k == key or any(k.startswith(p) for p in prefixes))
            ]
            if not found:
                return self._reply({"ok": False, "signed_download_url": "", "matched_key": ""})
            matched = key if key in found else found[-1]
            return self._reply({"ok": True, "signed_download_url": self._url(matched), "matched_key": matched})
        return web.json_response({"code": "bad_route"}, status=404)

    def _entry_of(self, request: web.Request) -> _Entry:
        key = request.match_info["key"]
        if request.query.get("sig") != "secret":
            raise web.HTTPForbidden
        found = [entry for (k, _), entry in self.entries.items() if k == key]
        if not found:
            raise web.HTTPNotFound
        return found[0]

    async def put_blob(self, request: web.Request) -> web.Response:
        entry = self._entry_of(request)
        data = await request.read()
        comp = request.query.get("comp")
        self.calls[f"blob {comp}"] += 1
        if comp == "block":
            entry.blocks[request.query["blockid"]] = data
        elif comp == "blocklist":
            ids = re.findall(r"<Latest>([^<]+)</Latest>", data.decode())
            entry.body = b"".join(entry.blocks[block_id] for block_id in ids)
        else:
            return web.Response(status=400)
        return web.Response(status=201)

    async def get_blob(self, request: web.Request) -> web.Response:
        entry = self._entry_of(request)
        if entry.body is None:
            raise web.HTTPNotFound
        self.calls["blob read"] += 1
        span = request.headers.get("x-ms-range", "")
        match = re.fullmatch(r"bytes=(\d+)-(\d+)", span)
        if match is None:
            return web.Response(body=entry.body)
        start, end = int(match.group(1)), int(match.group(2))
        return web.Response(body=entry.body[start : end + 1], status=206)


@asynccontextmanager
async def serve_fake_gha_cache() -> AsyncIterator[FakeGhaCache]:
    fake = FakeGhaCache()
    app = web.Application(client_max_size=256 * 1024 * 1024)
    app.router.add_post(_SERVICE + "{method}", fake.twirp)
    app.router.add_put("/blob/{key}", fake.put_blob)
    app.router.add_get("/blob/{key}", fake.get_blob)
    async with TestServer(app, host="127.0.0.1") as server:
        fake.base = str(server.make_url("")).rstrip("/")
        yield fake
