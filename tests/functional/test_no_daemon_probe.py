"""A store with no daemon takes only the builds pynixd's sandbox proved it builds (nixidae/nixidae#1541).

sg1's pynixd-0 advertised nix.conf's systems unproven. Its sandbox could not
mount `/proc`, so it took every build and failed each in 0.33 s.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from structlog.testing import capture_logs

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.store import LocalDBStore
from pynixd.store.no_daemon import NoDaemonStore
from pynixd.store.probe import BUILT_FEATURES
from tests._conftest.nix_config import for_test_store
from tests.conftest import make_test_spec

if TYPE_CHECKING:
    from pathlib import Path

    from pynixd.nix_config import NixConfig


async def _started(tmp_path: Path, nix_config: NixConfig, **spec_args: Any) -> tuple[NoDaemonStore, list[Any]]:
    """Start a store with no daemon in a server, and answer its daemon side and the logs of its start."""
    spec = make_test_spec(
        store_id="local", store_path=tmp_path / "store", nix_config=nix_config, daemon=False, **spec_args
    )
    store = LocalDBStore(spec)
    with capture_logs() as logs:
        async with Server(
            stores={StoreId("local"): store}, unix_path=tmp_path / "pynixd.sock", ssh_port=None, http_port=None
        ):
            pass
    daemon = store.daemon_client
    assert isinstance(daemon, NoDaemonStore)
    return daemon, logs


def _sandboxed(**settings: Any) -> NixConfig:
    nix_config = for_test_store(substituters=())
    nix_config.sandbox = "true"
    for name, value in settings.items():
        setattr(nix_config, name, value)
    return nix_config


def _events(logs: list[Any], event: str) -> list[Any]:
    return [entry for entry in logs if entry["event"] == event]


async def test_the_sandbox_proves_the_system_nix_conf_names(tmp_path: Path) -> None:
    daemon, logs = await _started(tmp_path, _sandboxed())
    [probing] = _events(logs, "store_probing")
    assert probing["systems"], "nix.conf names this machine's system"
    assert set(daemon.feature_matrix) == set(probing["systems"]), "an echo builds on every system nix.conf names"
    for features in daemon.feature_matrix.values():
        assert features <= set(probing["features"]), "a feature nix.conf does not name is never advertised"
        if not os.access("/dev/kvm", os.W_OK):
            assert "kvm" not in features, "kvm is a feature the sandbox proves with a build"
        elif "kvm" in probing["features"]:
            assert "kvm" in features, "the sandbox binds /dev/kvm, so its probe proves kvm (nixidae/nixidae#1648)"
    # A store waits for its probes before it serves: a build for every feature took a median of 21 s.
    [built] = _events(logs, "features_probed")
    for features in built["feature_matrix"].values():
        assert set(features) <= BUILT_FEATURES, "a feature that only labels the machine needs no build"


async def test_a_sandbox_that_cannot_build_advertises_nothing(tmp_path: Path) -> None:
    """The negative control: the sandbox fails as it sets up, as sg1's failed to mount `/proc`, so it takes no build.

    A path the sandbox must bind and cannot find fails every build there. `sandbox_paths=[]` writes nothing.
    """
    daemon, logs = await _started(tmp_path, _sandboxed(sandbox_paths=["/probe-missing=/nonexistent-probe-path"]))
    assert daemon.feature_matrix == {}
    [probing] = _events(logs, "store_probing")
    for system in probing["systems"]:
        assert not daemon.supports_derivation(system)
    [warning] = _events(logs, "store_probed_no_system")
    assert all(warning["refusals"].values()), "each refusal says why"


async def test_probe_false_builds_no_probe(tmp_path: Path) -> None:
    daemon, logs = await _started(tmp_path, _sandboxed(), probe=False)
    assert not _events(logs, "store_probing")
    assert daemon.feature_matrix == {}, "unprobed and unrestricted, as a store with a daemon and `probe: false`"
    assert daemon.supports_derivation("x86_64-linux")


async def test_a_configured_feature_matrix_builds_no_probe(tmp_path: Path) -> None:
    configured = {"x86_64-linux": {"big-parallel"}}
    daemon, logs = await _started(tmp_path, _sandboxed(), feature_matrix=configured)
    assert not _events(logs, "store_probing")
    assert daemon.feature_matrix == configured
