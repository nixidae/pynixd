from pathlib import Path

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import LocalSocketStoreSpec, PynixdSettings


async def test_start_stop(tmp_path) -> None:
    settings = PynixdSettings(config=Path("/etc/pynixd/pynixd.json"))
    settings.unix_path = None
    settings.stores["local"] = LocalSocketStoreSpec(
        store_path=tmp_path / "store",
        use_db=True,
    )

    all_stores = settings.to_stores()
    local_store = all_stores[StoreId("local")]

    async with Server(stores={StoreId("local"): local_store}, settings=settings):
        pass
