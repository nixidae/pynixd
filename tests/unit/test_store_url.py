"""The stores `pynixd.store_url` opens for substituter URLs."""

from __future__ import annotations

import base64
from pathlib import Path

import asyncssh
import pytest

from pynixd.config import ExternalUnixStoreSpec, LocalBinaryCacheSpec, PynixdSettings, SSHSubprocessStoreSpec
from pynixd.store.daemon import DaemonStore
from pynixd.store.ssh import SSHSubprocessStore
from pynixd.store_url import OPENSSH_KNOWN_HOSTS, spec_for
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
    assert (spec.username, spec.host, spec.port) == ("nix", "pynixd", 22)
    assert spec.trusted
    assert spec.priority == 0
    assert (spec.no_schedule, spec.scheduleable, spec.gc_enabled, spec.monitor) == (True, False, False, False)
    assert spec.remote_command == ["nix-daemon", "--stdio"]
    assert spec.known_hosts == list(OPENSSH_KNOWN_HOSTS)
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
    builder = url.model_copy(update={"no_schedule": False, "scheduleable": True})
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


async def test_a_dead_host_leaves_the_client_op_to_the_daemon() -> None:
    """Nothing listens on port 1: the start fails, and the client op does not."""
    substituters = ClientSubstituters(PynixdSettings())
    async with substituters.running():
        assert await substituters.resolve(["ssh-ng://127.0.0.1:1"], []) is None
        assert substituters.stores() == ()
