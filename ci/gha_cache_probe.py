"""Measure GitHub's Actions cache service where its documentation is silent: nixidae/nixidae#1377.

Runs on a runner with the runner's python3 and the standard library only, so it measures the
service and not pynixd's client. Credentials come from the file ghanix's
`ghaCacheCredentials` step writes. Every result is one JSON line on stdout. Signed URLs carry a
`sig` and never reach the output.

- `orphan TAG`: an entry created and never finalized, bare and with its blob written. What a
  second create and a lookup answer, in the same job.
- `orphan-check TAG`: the same questions from a later job, or a later run.
- `reads TAG`: does a ranged blob read count against the 1500 downloads a minute? Many ranged reads
  of one entry, then one lookup; then lookups alone until the first 429.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Final

_SERVICE: Final = "twirp/github.actions.results.api.v1.CacheService"
_BLOB_VERSION: Final = "2023-11-03"
_VERSION: Final = hashlib.sha256(b"pynixd-gha-cache-probe 1").hexdigest()
_ENTRY_SIZE: Final = 1024 * 1024


@dataclass(frozen=True)
class Reply:
    status: int
    body: dict[str, object]
    headers: dict[str, str]

    def public(self) -> dict[str, object]:
        """The reply without its signed URLs."""
        body = {k: ("<signed url>" if "url" in k.lower() else v) for k, v in self.body.items()}
        keep = {k: v for k, v in self.headers.items() if k.lower().startswith(("retry-after", "x-ratelimit"))}
        return {"status": self.status, "body": body, "headers": keep}


@dataclass(frozen=True)
class Service:
    results_url: str
    token: str

    @classmethod
    def from_file(cls, path: Path) -> Service:
        env = json.loads(path.read_text())
        return cls(env["ACTIONS_RESULTS_URL"].rstrip("/"), env["ACTIONS_RUNTIME_TOKEN"])

    def call(self, method: str, body: dict[str, object]) -> Reply:
        request = urllib.request.Request(
            f"{self.results_url}/{_SERVICE}/{method}",
            data=json.dumps(body).encode(),
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
            method="POST",
        )
        return _send(request)

    def lookup(self, key: str) -> Reply:
        return self.call("GetCacheEntryDownloadURL", {"key": key, "restoreKeys": [key], "version": _VERSION})

    def create(self, key: str) -> Reply:
        return self.call("CreateCacheEntry", {"key": key, "version": _VERSION})

    def finalize(self, key: str, size: int) -> Reply:
        return self.call("FinalizeCacheEntryUpload", {"key": key, "version": _VERSION, "sizeBytes": str(size)})


def _send(request: urllib.request.Request) -> Reply:
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            status, raw, headers = response.status, response.read(), dict(response.headers)
    except urllib.error.HTTPError as e:
        status, raw, headers = e.code, e.read(), dict(e.headers)
    except (urllib.error.URLError, OSError) as e:
        return Reply(0, {"error": repr(e)}, {})
    try:
        parsed: object = json.loads(raw) if raw else {}
    except ValueError:
        parsed = {"text": raw[:200].decode(errors="replace")}
    return Reply(status, parsed if isinstance(parsed, dict) else {}, headers)


def _blob(url: str, method: str, query: str = "", data: bytes | None = None, span: str = "") -> str:
    """The status, and Azure's error code when it refused."""
    headers = {"x-ms-version": _BLOB_VERSION, "Content-Type": "application/octet-stream"}
    if span:
        headers["x-ms-range"] = span
    request = urllib.request.Request(f"{url}&{query}" if query else url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            response.read()
            return str(response.status)
    except urllib.error.HTTPError as e:
        code = re.search(rb"<Code>([^<]+)</Code>", e.read())
        return f"{e.code} {code.group(1).decode() if code else ''}".strip()
    except (urllib.error.URLError, OSError) as e:
        return f"0 {e!r}"


def _write_blob(url: str, data: bytes) -> list[str]:
    block_id = base64.b64encode(b"00000000").decode()
    listing = f'<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>{block_id}</Latest></BlockList>'
    return [
        _blob(url, "PUT", f"comp=block&blockid={urllib.parse.quote(block_id, safe='')}", data),
        _blob(url, "PUT", "comp=blocklist", listing.encode()),
    ]


def _emit(step: str, **fields: object) -> None:
    print(json.dumps({"step": step, "t": round(time.monotonic(), 3), **fields}), flush=True)


def _signed(reply: Reply, field: str) -> str:
    """*field* under its proto name or its JSON name: the service has sent the proto name."""
    head, *rest = field.split("_")
    return str(reply.body.get(field) or reply.body.get(head + "".join(w.title() for w in rest)) or "")


def _orphan_keys(tag: str) -> dict[str, str]:
    return {"bare": f"probe-orphan-{tag}-bare", "staged": f"probe-orphan-{tag}-staged"}


def orphan(service: Service, tag: str) -> None:
    for kind, key in _orphan_keys(tag).items():
        created = service.create(key)
        _emit("create", kind=kind, key=key, reply=created.public())
        if kind == "staged" and (url := _signed(created, "signed_upload_url")):
            _emit("blob", kind=kind, statuses=_write_blob(url, os.urandom(1024)))
    orphan_check(service, tag)


def orphan_check(service: Service, tag: str) -> None:
    for kind, key in _orphan_keys(tag).items():
        _emit("lookup", kind=kind, key=key, reply=service.lookup(key).public())
        _emit("create-again", kind=kind, key=key, reply=service.create(key).public())


def reads(service: Service, tag: str, count: int, lookups: int, concurrency: int) -> None:
    key = f"probe-reads-{tag}"
    created = service.create(key)
    _emit("create", key=key, reply=created.public())
    _emit("blob", statuses=_write_blob(_signed(created, "signed_upload_url"), os.urandom(_ENTRY_SIZE)))
    _emit("finalize", reply=service.finalize(key, _ENTRY_SIZE).public())
    found = service.lookup(key)
    _emit("lookup", reply=found.public())
    url = _signed(found, "signed_download_url")

    def ranged(i: int) -> str:
        start = (i * 4096) % (_ENTRY_SIZE - 4096)
        return _blob(url, "GET", span=f"bytes={start}-{start + 4095}")

    began = time.monotonic()
    with ThreadPoolExecutor(concurrency) as pool:
        statuses = Counter(pool.map(ranged, range(count)))
    _emit("ranged-reads", count=count, seconds=round(time.monotonic() - began, 2), statuses=dict(statuses))
    _emit("lookup-after-reads", reply=service.lookup(key).public())

    # Stop at the first refusal: past it, every call keeps the whole repository's cache refused.
    began = time.monotonic()
    refused = threading.Event()
    lock = threading.Lock()
    seen: Counter[str] = Counter()
    first: list[dict[str, object]] = []

    def lookup(_: int) -> None:
        if refused.is_set():
            return
        reply = service.lookup(key)
        with lock:
            found = reply.status == 200 and reply.body.get("ok") is True
            seen[f"{reply.status} ok={found}"] += 1
            if not found and not first:
                refused.set()
                first.append(
                    {
                        "after": sum(seen.values()),
                        "seconds": round(time.monotonic() - began, 2),
                        "reply": reply.public(),
                    }
                )

    with ThreadPoolExecutor(concurrency) as pool:
        list(pool.map(lookup, range(lookups)))
    _emit(
        "lookups",
        asked=sum(seen.values()),
        seconds=round(time.monotonic() - began, 2),
        statuses=dict(seen),
        first_refused=first[:1],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("what", choices=["orphan", "orphan-check", "reads"])
    parser.add_argument("tag")
    parser.add_argument(
        "--credentials", type=Path, default=Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "pynixd-gha-cache.json"
    )
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--lookups", type=int, default=1700)
    parser.add_argument("--concurrency", type=int, default=32)
    args = parser.parse_args()
    service = Service.from_file(args.credentials)
    if args.what == "orphan":
        orphan(service, args.tag)
    elif args.what == "orphan-check":
        orphan_check(service, args.tag)
    else:
        reads(service, args.tag, args.count, args.lookups, args.concurrency)


if __name__ == "__main__":
    main()
