"""A trusted client's `builders` become stores that only its builds use.

nix-daemon reads the client's `builders` in place of its own
(`daemon.cc:234`), and `build-remote.sh:20-25` names the three kinds of
machine Nix's suite builds on. nixidae/nixidae#1242.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.allocator import BuildAllocator, TelemetryStoreRanker, outside_the_session
from pynixd.config import LocalSocketStoreSpec, PynixdSettings, SSHSubprocessStoreSpec, StoreRankingSettings
from pynixd.machines import parse_config
from pynixd.psi import DiskSpace, SystemHealth
from pynixd.session_builders import SessionBuilderError, SessionBuilders, builder_spec
from pynixd.store.local_daemon import LocalStore
from tests._conftest.placement import FakeBuild, FakeMonitor, FakeStore

SYSTEM = "x86_64-linux"
GIB = 1024**3


def _spec(line: str, tmp_path: Path) -> Any:
    [machine] = parse_config({SYSTEM}, line)
    return builder_spec(machine, PynixdSettings(unix_path=tmp_path / "pynixd.sock"), "/bin/nix")


def test_a_local_path_is_a_daemon_over_that_root(tmp_path: Path) -> None:
    spec = _spec("/t/machine2 - - 1 1 bar", tmp_path)
    assert isinstance(spec, LocalSocketStoreSpec)
    assert spec.store_path == Path("/t/machine2")
    assert spec.managed is True
    assert spec.nix_bin == "/bin/nix"
    assert spec.extra_args == ["--option", "system-features", "bar"]
    assert spec.systems == {SYSTEM}
    assert spec.system_features == {"bar"}
    # A socket under the test's long root passes the 107 bytes of `sun_path`.
    assert spec.socket_path.parent == tmp_path
    assert spec.max_connections == 2
    assert spec.transfer_connections == 1


def test_ssh_ng_localhost_is_nix_fake_ssh(tmp_path: Path) -> None:
    """`ssh.cc:84`: Nix runs `remote-store`'s daemon on this host, with no SSH."""
    spec = _spec("ssh-ng://localhost?remote-store=/t/machine3?system-features=baz - - 1 1 baz", tmp_path)
    assert isinstance(spec, LocalSocketStoreSpec)
    assert spec.store_path == Path("/t/machine3")
    assert spec.extra_args == ["--option", "system-features", "baz"]


def test_ssh_ng_host_is_an_ssh_builder(tmp_path: Path) -> None:
    spec = _spec("ssh-ng://nix@builder.example aarch64-linux /k/id 4 1 kvm", tmp_path)
    assert isinstance(spec, SSHSubprocessStoreSpec)
    assert (spec.host, spec.username) == ("builder.example", "nix")
    assert [str(key) for key in spec.client_keys] == ["/k/id"]
    assert spec.systems == {"aarch64-linux"}
    assert spec.system_features == {"kvm"}
    assert not spec.no_schedule
    assert spec.max_connections == 5


@pytest.mark.parametrize("line", ["ssh://localhost?remote-store=/t/machine1 - - 1 1 foo", "builder.example"])
def test_the_legacy_protocol_is_refused(line: str, tmp_path: Path) -> None:
    """A bare host is `ssh://` (`machines.cc:21`), and pynixd speaks no serve protocol."""
    with pytest.raises(SessionBuilderError, match="ssh-ng:// only"):
        _spec(line, tmp_path)


def test_mandatory_features_hold_back_other_builds(tmp_path: Path) -> None:
    """`Machine::mandatoryMet`: a store with a mandatory feature takes only builds that require it."""
    store = LocalStore(
        LocalSocketStoreSpec(
            store_id=StoreId("s"),
            store_path=tmp_path,
            socket_path=tmp_path / "d.socket",
            systems={SYSTEM},
            system_features={"benchmark"},
            mandatory_features={"benchmark"},
        )
    )
    assert not store.supports_derivation(SYSTEM, set())
    assert store.supports_derivation(SYSTEM, {"benchmark"})


def _store(store_id: StoreId, *, session_only: bool = False) -> FakeStore:
    disk = DiskSpace(free_bytes=100 * GIB, total_bytes=100 * GIB)
    return FakeStore(store_id, session_only=session_only, monitor=FakeMonitor(SystemHealth(disk=disk)))


FLEET = StoreId("fleet")
MINE = StoreId("session-mine")
THEIRS = StoreId("session-theirs")


def _rank(builders: frozenset[StoreId] | None) -> set[StoreId]:
    local = _store(LOCAL_STORE_ID)
    stores = {
        LOCAL_STORE_ID: local,
        FLEET: _store(FLEET),
        MINE: _store(MINE, session_only=True),
        THEIRS: _store(THEIRS, session_only=True),
    }
    ranker = TelemetryStoreRanker(StoreRankingSettings())
    allocator = BuildAllocator(stores, local, ranker)
    return {candidate.store_id for candidate in allocator.rank_stores(FakeBuild.on(SYSTEM, builders=builders), {})}


def test_a_client_with_builders_builds_on_them_and_locally() -> None:
    """Nix replaces its own machines with the client's, so pynixd's fleet takes none of these builds."""
    assert _rank(frozenset({MINE})) == {LOCAL_STORE_ID, MINE}


def test_another_client_never_builds_on_a_session_store() -> None:
    assert _rank(None) == {LOCAL_STORE_ID, FLEET}


def test_outside_the_session_names_why() -> None:
    build = FakeBuild(builders=frozenset({MINE}))
    assert outside_the_session(_store(FLEET), build) == "not one of the client's builders"
    assert outside_the_session(_store(THEIRS, session_only=True), FakeBuild()) == "another client's builder"


async def test_one_store_per_machine_for_every_session(tmp_path: Path) -> None:
    added: list[Any] = []

    async def add_store(store: Any) -> bool:
        added.append(store)
        return True

    builders = SessionBuilders(PynixdSettings(unix_path=tmp_path / "pynixd.sock"), "/bin/nix", add_store)
    line = f"{tmp_path}/m2 - - 1 1 bar"
    first = await builders.stores_for(line)
    again = await builders.stores_for(line)
    assert first == again
    assert first is not None
    assert len(first) == 1
    assert len(added) == 1
    assert added[0].session_only
    # Another value that names the same machine reuses its store.
    await builders.stores_for(f"{line} ; ")
    assert len(added) == 1


async def test_a_value_with_no_machine_keeps_pynixds_placement(tmp_path: Path) -> None:
    """NIX-DEVIATION (#27) in `pynixd/session_builders.py`: no machine means pynixd's own stores."""

    async def add_store(store: Any) -> bool:
        raise AssertionError(store)

    builders = SessionBuilders(PynixdSettings(unix_path=tmp_path / "pynixd.sock"), "/bin/nix", add_store)
    assert await builders.stores_for(f"@{tmp_path}/no-such-file") is None
    assert await builders.stores_for("# only a comment") is None
