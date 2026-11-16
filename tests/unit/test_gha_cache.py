"""The GitHub Actions cache client and entry format, against the fake service. nixidae/nixidae#1377."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import aiohttp
import pytest

from pynixd.store import gha_cache_api
from pynixd.store.gha_cache import ENTRY_FORMAT, cache_version, entry_head, parse_head
from pynixd.store.gha_cache_api import CacheMode, GhaCacheApi, GhaCredentials, RateLimited
from tests._conftest.gha_cache_fake import FakeGhaCache, serve_fake_gha_cache

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable
    from pathlib import Path


def _api(
    fake: FakeGhaCache, tmp_path: Path, session: aiohttp.ClientSession, now: Callable[[], float] = time.monotonic
) -> GhaCacheApi:
    credentials = GhaCredentials.from_file(fake.credentials(tmp_path / "creds.json"))
    assert credentials is not None
    return GhaCacheApi(credentials, cache_version("/nix/store", ""), session, now=now)


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


async def test_an_entry_goes_up_in_blocks_and_comes_back_by_range(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gha_cache_api, "BLOCK_SIZE", 4)
    async with serve_fake_gha_cache() as fake, aiohttp.ClientSession() as session:
        api = _api(fake, tmp_path, session)
        url = await api.create("abc")
        assert url is not None
        size = await api.put_blob(url, _chunks(b"hello ", b"world", b"!"))
        await api.finalize("abc", size)
        assert fake.calls["blob block"] == 3
        located = await api.download_url("abc")
        assert located is not None
        async with api.open_range(located, 6, 10) as body:
            assert await body.read() == b"world"


async def test_a_second_create_of_one_key_is_a_skip(tmp_path: Path) -> None:
    async with serve_fake_gha_cache() as fake, aiohttp.ClientSession() as session:
        api = _api(fake, tmp_path, session)
        assert await api.create("abc") is not None
        assert await api.create("abc") is None


async def test_an_unfinished_entry_is_a_miss(tmp_path: Path) -> None:
    async with serve_fake_gha_cache() as fake, aiohttp.ClientSession() as session:
        api = _api(fake, tmp_path, session)
        assert await api.create("abc") is not None
        assert await api.download_url("abc") is None


async def test_a_429_holds_every_call_until_its_retry_after(tmp_path: Path) -> None:
    """The breaker of `api.rs:805-852` stays open for the run; this one closes after `Retry-After`."""
    clock = [1000.0]
    async with serve_fake_gha_cache() as fake, aiohttp.ClientSession() as session:
        api = _api(fake, tmp_path, session, now=lambda: clock[0])
        fake.limited, fake.retry_after = 1, 30
        with pytest.raises(RateLimited) as caught:
            await api.download_url("abc")
        assert caught.value.retry_after == 30
        asked = fake.calls["GetCacheEntryDownloadURL"]
        with pytest.raises(RateLimited):
            await api.download_url("abc")
        assert fake.calls["GetCacheEntryDownloadURL"] == asked, "a held call reached the service"
        clock[0] += 31
        assert await api.download_url("abc") is None


def test_the_credentials_need_the_v2_service(tmp_path: Path) -> None:
    env = {"ACTIONS_RESULTS_URL": "https://results/", "ACTIONS_RUNTIME_TOKEN": "runtime-secret"}
    assert GhaCredentials.from_environment(env) is None
    found = GhaCredentials.from_environment(env | {"ACTIONS_CACHE_SERVICE_V2": "True"})
    assert found is not None
    assert found.mode is CacheMode.READ_WRITE
    assert "runtime-secret" not in repr(found)
    assert GhaCredentials.from_file(tmp_path / "absent.json") is None


@pytest.mark.parametrize(
    ("raw", "readable", "writable"),
    [("read", True, False), ("write-only", False, True), ("none", False, False), ("", True, True), ("x", True, True)],
)
def test_the_cache_mode_reads_as_actions_cache_does(raw: str, readable: bool, writable: bool) -> None:
    mode = CacheMode.parse(raw)
    assert (mode.readable, mode.writable) == (readable, writable)


def test_an_entry_head_names_its_narinfo() -> None:
    narinfo = "StorePath: /nix/store/x\nURL: nar\n"
    data = entry_head(narinfo) + b"\x28\xb5\x2f\xfd"
    head = parse_head(data)
    assert head is not None
    start, length = head
    assert data[start : start + length].decode() == narinfo
    assert parse_head(b"pynixd-gha-cache 0 12\nrest") is None
    assert parse_head(ENTRY_FORMAT.encode()) is None


def test_the_version_separates_store_directories_and_salts() -> None:
    assert cache_version("/nix/store", "") == cache_version("/nix/store", "")
    assert cache_version("/nix/store", "") != cache_version("/other/store", "")
    assert cache_version("/nix/store", "") != cache_version("/nix/store", "x")
