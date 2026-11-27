"""The stores `pynixd.store_url` opens for substituter URLs."""

from __future__ import annotations

import base64
from pathlib import Path

import asyncssh
import pytest
from structlog.testing import capture_logs

from pynixd.config import (
    ExternalUnixStoreSpec,
    LocalBinaryCacheSpec,
    LocalStoreSubstituterSpec,
    PynixdSettings,
    SSHSubprocessStoreSpec,
)
from pynixd.constants import OPENSSH_CONFIG_FILES
from pynixd.store.daemon import DaemonStore
from pynixd.store.ssh import OpenSSHConfig, SSHSubprocessStore, read_openssh_config
from pynixd.store_url import spec_for
from pynixd.substituters import ClientSubstituters

HOST_KEY = asyncssh.generate_private_key("ssh-ed25519").export_public_key().decode().strip()


def _ssh(url: str) -> SSHSubprocessStoreSpec:
    spec = spec_for(url)
    assert isinstance(spec, SSHSubprocessStoreSpec)
    return spec


@pytest.mark.parametrize("url", ["unix:///run/peer/socket", "ssh-ng://builder"])
def test_a_substituter_gets_no_probe_builds(url: str) -> None:
    """A substituter builds nothing; a probe build there costs its slots and can fail its start."""
    spec = spec_for(url)
    assert spec is not None
    store = spec.to_store("t")
    assert isinstance(store, DaemonStore)
    assert store._probe is False


def test_nixkubes_substituter_is_a_store_that_only_substitutes() -> None:
    """`nixkube/kubenix/config.nix` names `ssh-ng://nix@pynixd?trusted=1`."""
    spec = _ssh("ssh-ng://nix@pynixd?trusted=1")
    assert (spec.username, spec.host, spec.port) == ("nix", "pynixd", None)
    assert spec.trusted
    assert spec.priority == 0
    assert (spec.no_schedule, spec.gc_enabled, spec.monitor) == (True, False, False)
    assert spec.remote_command == ["nix-daemon", "--stdio"]
    # nixkube's `/etc/ssh/ssh_config` names the key and the host key file (`nixkube/kubenix/secret.nix`).
    assert spec.ssh_config == list(OPENSSH_CONFIG_FILES)
    assert spec.known_hosts == []
    assert spec.host_public_key is None
    assert spec.client_keys == []


def test_the_parameters_nix_reads_reach_the_spec() -> None:
    """`ssh-store.cc:227` runs `remote-program --stdio --store remote-store`."""
    encoded = base64.b64encode(HOST_KEY.encode()).decode()
    spec = _ssh(
        "ssh-ng://root@[::1]:2222?ssh-key=/etc/nix/key&compress=1&remote-program=nix%20daemon"
        f"&remote-store=/srv/store&priority=5&base64-ssh-public-host-key={encoded}"
    )
    assert (spec.username, spec.host, spec.port) == ("root", "::1", 2222)
    assert spec.client_keys == ["/etc/nix/key"]
    assert spec.compress
    assert spec.remote_command == ["nix", "daemon", "--stdio", "--store", "/srv/store"]
    assert spec.priority == 5
    assert spec.host_public_key == HOST_KEY


@pytest.mark.parametrize(
    "url",
    [
        "ssh-ng://builder?max-connections=4",
        "ssh-ng://builder?ssh-key=relative/key",
        "ssh-ng://builder?base64-ssh-public-host-key=bm90IGEga2V5",
        "ssh-ng://builder?compress=maybe",
        "file:///srv/cache?want-mass-query=maybe",
        "ssh-ng://builder/some/path",
        "ssh-ng://",
    ],
)
def test_what_the_registry_cannot_read_is_left_to_the_daemon(url: str) -> None:
    assert spec_for(url) is None


def test_a_path_in_a_url_is_percent_decoded() -> None:
    """Nix decodes each path segment (`url.cc:194`), so a quoted `[` names the directory with a `[` in it."""
    cache, socket = spec_for("file:///tmp/a%5Bb/cache"), spec_for("unix:///tmp/a%5Bb/sock")
    assert isinstance(cache, LocalBinaryCacheSpec)
    assert isinstance(socket, ExternalUnixStoreSpec)
    assert (cache.path, socket.socket_path) == (Path("/tmp/a[b/cache"), Path("/tmp/a[b/sock"))


@pytest.mark.parametrize(
    "url", ["/srv/backup", "local:///srv/backup", "local?root=/srv/backup", "local://x?root=/srv/backup"]
)
def test_a_local_store_url_opens_the_store_at_its_root(url: str) -> None:
    """`store-reference.cc:103` and `:111`: a path and `local` with `?root=` name one local store; `?root=` wins."""
    spec = spec_for(url)
    assert isinstance(spec, LocalStoreSubstituterSpec)
    assert spec.store_path == Path("/srv/backup")
    assert (spec.priority, spec.trusted, spec.no_schedule, spec.gc_enabled) == (0, False, True, False)
    same = spec_for("/srv/backup/")
    assert same is not None
    assert spec.reference_key() == same.reference_key()


@pytest.mark.parametrize("url", ["local", "auto", "daemon", "local?trusted=1", "relative/backup", "/srv/b?state=/s"])
def test_the_machines_store_and_unread_local_settings_stay_with_the_daemon(url: str) -> None:
    """`local` alone is the machine's store (`store-reference.cc:103`); pynixd reads no `state`, so the daemon answers."""
    assert spec_for(url) is None


def test_another_trust_or_key_is_another_store() -> None:
    plain, trusted, keyed = (_ssh(url) for url in ("ssh-ng://b", "ssh-ng://b?trusted=1", "ssh-ng://b?ssh-key=/k"))
    assert len({plain.store_id, trusted.store_id, keyed.store_id}) == 3
    assert len({plain.reference_key(), trusted.reference_key(), keyed.reference_key()}) == 3
    assert _ssh("ssh-ng://b?priority=3").reference_key() == plain.reference_key()


def test_want_mass_query_reaches_the_spec_and_the_key() -> None:
    """`store-api.hh:227`: `?want-mass-query=` is a store setting, so it sets whether `QuerySubstitutablePaths` asks."""
    plain, wanted = spec_for("file:///srv/cache"), spec_for("file:///srv/cache?want-mass-query=true")
    assert plain is not None
    assert wanted is not None
    assert (plain.want_mass_query, wanted.want_mass_query) == (None, True)
    assert plain.reference_key() != wanted.reference_key()


def test_a_configured_builder_is_not_a_substituters_store() -> None:
    url = _ssh("ssh-ng://b?trusted=1")
    builder = url.model_copy(update={"no_schedule": False})
    assert builder.reference_key() != url.reference_key()


async def test_the_default_known_hosts_files_are_read_when_present(tmp_path: Path) -> None:
    present = tmp_path / "known_hosts"
    present.write_text(f"builder {HOST_KEY}\n")
    spec = _ssh("ssh-ng://builder").model_copy(update={"known_hosts": [str(tmp_path / "absent"), str(present)]})
    assert await SSHSubprocessStore(spec)._known_hosts() == [str(present)]


async def test_no_known_hosts_file_accepts_no_key(tmp_path: Path) -> None:
    """`None` would accept any key, and `()` only `~/.ssh/known_hosts` (nixidae/nixidae#1172)."""
    spec = _ssh("ssh-ng://builder").model_copy(update={"known_hosts": [str(tmp_path / "absent")]})
    known = await SSHSubprocessStore(spec)._known_hosts()
    assert isinstance(known, asyncssh.SSHKnownHosts)
    assert not any(known.match("builder", "", 22)[:3])


async def test_a_named_host_key_is_the_only_key() -> None:
    encoded = base64.b64encode(HOST_KEY.encode()).decode()
    store = SSHSubprocessStore(_ssh(f"ssh-ng://builder?base64-ssh-public-host-key={encoded}"))
    known = await store._known_hosts()
    assert isinstance(known, tuple)
    keys, *rest = known
    assert [key.export_public_key().decode().strip() for key in keys] == [HOST_KEY]  # type: ignore[union-attr]  # the first item holds SSHKeys
    assert not any(rest)


def test_the_config_files_name_the_host_key_files_and_the_keys(tmp_path: Path) -> None:
    """`ssh_config(5)`: the first value wins, a `Host` block applies to its host, and OpenSSH's defaults fill the rest."""
    key, known = tmp_path / "id", tmp_path / "global_known_hosts"
    key.write_text("")
    known.write_text("")
    user = tmp_path / "user_config"
    user.write_text(
        f"Host pynixd\n  IdentityFile {key}\n  IdentityFile {tmp_path / 'absent_key'}\n  GlobalKnownHostsFile {known}\n"
    )
    system = tmp_path / "system_config"
    system.write_text(f"Host *\n  GlobalKnownHostsFile {tmp_path / 'not_read'}\n  UserKnownHostsFile none\n")
    files = [str(user), str(tmp_path / "absent_config"), str(system)]

    config = read_openssh_config(files, "pynixd", None, "nix")
    other = read_openssh_config(files, "elsewhere", None, None)

    assert config == OpenSSHConfig(
        files=(str(user), str(system)), known_hosts=(str(known),), identity_files=(str(key),)
    )
    assert (other.identity_files, other.known_hosts) == (None, ())


async def test_the_config_names_no_host_key_file_and_no_key_is_accepted(tmp_path: Path) -> None:
    """`known_hosts: null` is `asyncssh`'s "accept any key"; beside a config it adds no file, and a key is still checked."""
    spec = _ssh("ssh-ng://builder").model_copy(update={"known_hosts": None})
    openssh = OpenSSHConfig(files=(), known_hosts=(), identity_files=None)
    known = await SSHSubprocessStore(spec)._known_hosts(openssh)
    assert isinstance(known, asyncssh.SSHKnownHosts)
    assert not any(known.match("builder", "", 22)[:3])


async def test_a_dead_host_leaves_the_client_op_to_the_daemon_where_one_runs() -> None:
    """Nothing listens on port 1: the start fails, and the client op does not. Nix keeps the store (`remote-store.cc:35`)."""
    substituters = ClientSubstituters(PynixdSettings())
    async with substituters.running():
        assert await substituters.resolve(["ssh-ng://127.0.0.1:1"], []) is None
        assert substituters.stores() == ()


async def test_a_dead_host_is_skipped_and_cools_down_with_no_daemon() -> None:
    """The NIX-DEVIATION of `ClientSubstituters._backend`: no daemon answers, so the op answers without the host."""
    clock = [0.0]
    substituters = ClientSubstituters(PynixdSettings(), now=lambda: clock[0], runs_daemon=lambda: False)
    url = "ssh-ng://127.0.0.1:1"
    async with substituters.running():
        with capture_logs() as logs:
            first = await substituters.resolve([url], [])
            cooling = await substituters.resolve([url], [])
            starts_while_cooling = sum(entry["event"] == "client_substituter_start_failed" for entry in logs)
            clock[0] = 60
            after = await substituters.resolve([url], [])
            starts = sum(entry["event"] == "client_substituter_start_failed" for entry in logs)
        assert substituters.stores() == ()

    assert (first, cooling, after) == ((), (), ())
    assert starts_while_cooling == 1, "a resolve during the cooldown starts nothing"
    assert starts == 2, "the cooldown ends, and the backend starts again"


@pytest.mark.parametrize(
    ("url", "stable"),
    [
        ("https://cache.nixos.org", True),
        ("https://cache.nixos.org/", True),
        ("https://nix-community.cachix.org", False),
        ("https://cache.nixos.org.example.com", False),
    ],
)
def test_only_cache_nixos_org_is_stable_unless_configured(url: str, stable: bool) -> None:
    """A path cache.nixos.org holds needs no cold-tier copy: it removes nothing. nixidae/nixidae#1417."""
    spec = spec_for(url)
    assert spec is not None
    assert spec.to_store("t").gc_stable is stable
    assert spec.model_copy(update={"gc_stable": not stable}).to_store("t").gc_stable is not stable
