"""A local store's `nix_config` reaches its Nix: `LocalSocketStoreSpec.environment`.

Without it a store's `system-features` miss its own builder, which then refuses the builds the store advertises
(measured: the provisioning-kubernetes test's `helper` store).
"""

from __future__ import annotations

from nix_daemon_protocol.ids import LOCAL_STORE_ID
from pynixd.config import LocalSocketStoreSpec
from pynixd.nix_config import NixConfig


def test_nix_config_goes_after_the_nix_config_extra_env_sets() -> None:
    spec = LocalSocketStoreSpec(
        store_id=LOCAL_STORE_ID,
        nix_config=NixConfig(system_features=["pynixd-helper"]),
        extra_env={"NIX_CONFIG": "max-jobs = 2", "HOME": "/build"},
    )

    assert spec.environment == {"NIX_CONFIG": "max-jobs = 2\nsystem-features = pynixd-helper", "HOME": "/build"}


def test_no_nix_config_leaves_extra_env_alone() -> None:
    spec = LocalSocketStoreSpec(store_id=LOCAL_STORE_ID, extra_env={"HOME": "/build"})

    assert spec.environment == {"HOME": "/build"}
    assert LocalSocketStoreSpec(store_id=LOCAL_STORE_ID).environment == {}
