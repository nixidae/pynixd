"""`nix-cache-info` must state the directory of the store that pynixd serves.

A Nix client compares `StoreDir` with its own and refuses a cache when the two
differ. pynixd answered `/nix/store` for every store, so a client of another
store got a wrong answer rather than a clear refusal. Issue nixidae/nixidae#212.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from nix_daemon_protocol.store_dir import reset_store_dir, set_store_dir
from pynixd.config import LocalSocketStoreSpec
from pynixd.http_server import PynixdHttpServer
from pynixd.store.local_daemon import LocalStore

OTHER = "/scratch/root/nix/store"


@pytest.fixture
def other_store():
    set_store_dir(OTHER)
    yield
    reset_store_dir()


def _server() -> PynixdHttpServer:
    spec = LocalSocketStoreSpec(store_id=LOCAL_STORE_ID, monitor=False, use_db=False)
    return PynixdHttpServer(LocalStore(spec), enable_metrics=False)


async def test_cache_info_states_the_default_store_dir():
    response = await _server().handle_cache_info(None)  # pyright: ignore[reportArgumentType]
    assert response.text is not None
    assert "StoreDir: /nix/store" in response.text


async def test_cache_info_states_the_store_dir_in_use(other_store):
    response = await _server().handle_cache_info(None)  # pyright: ignore[reportArgumentType]
    assert response.text is not None
    assert f"StoreDir: {OTHER}" in response.text
