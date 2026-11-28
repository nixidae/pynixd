"""An `ssh-ng://` substituter gets a backend of pynixd's, over a real SSH connection.

The far side is a second pynixd, serving a store that holds the paths over
its SSH listener: it answers `nix-daemon --stdio` as `sshd` and a remote
`nix-daemon` would. Its host key is in the URL
(`base64-ssh-public-host-key`), or in a known hosts file an OpenSSH config
file names, so the check is real.
"""

from __future__ import annotations

import base64
import contextlib
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import asyncssh
import pytest

from nix_daemon_protocol.ensure_path import EnsurePathRequest
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_path import StorePath
from pynixd import Server, store_url
from pynixd.config import PynixdSettings
from pynixd.connection import Connection
from pynixd.exceptions import BackendError
from pynixd.serde import SetOptionsRequest
from pynixd.store import LocalDBStore, SSHSubprocessStore, Store
from pynixd.substituters import ClientSubstituters
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.client_options import set_options
from tests._conftest.nix_cli_store import nix_cli_store
from tests._conftest.nix_config import for_test_store
from tests._conftest.ssh_keys import SESSION_SSH_KEYS
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

_PARENT = (
    'with import <nixpkgs> {}; runCommand "pynixd-ssh-ng-parent" {} '
    '"echo ${builtins.toFile "pynixd-ssh-ng-child" "the child of the ssh-ng test"} > $out"'
)


async def _valid(store_path: Path) -> set[str]:
    async with nix_cli_store(store_path) as store:
        _rc, stdout, _stderr, _both = await run_subproc(
            [str(NIX_BIN), "path-info", "--store", store, "--all"], verbose=False
        )
    return {line.strip() for line in stdout.splitlines() if line.strip()}


def _options(**overrides: str) -> SetOptionsRequest:
    """A `SetOptions` request with every field, because no field has a default."""
    return set_options(overrides=overrides)


def _url(port: int, host_key: asyncssh.SSHKey) -> str:
    encoded = base64.b64encode(host_key.export_public_key()).decode()
    return (
        f"ssh-ng://admin-user@127.0.0.1:{port}?ssh-key={SESSION_SSH_KEYS / 'id_admin'}"
        f"&base64-ssh-public-host-key={encoded}&trusted=1"
    )


@asynccontextmanager
async def _source(tmp_path: Path) -> AsyncIterator[tuple[Server, asyncssh.SSHKey, str]]:
    """A pynixd whose store holds `parent` and its `child`, behind its SSH listener."""
    store_dir = tmp_path / "source"
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "build", "--impure", "--no-link", "--print-out-paths", "--expr", _PARENT], verbose=False
    )
    parent = stdout.strip()
    await run_subproc([str(NIX_BIN), "copy", "--no-check-sigs", "--to", str(store_dir), parent])
    host_key = asyncssh.generate_private_key("ssh-ed25519")
    host_key_path = tmp_path / "host_key"
    host_key.write_private_key(str(host_key_path))
    # On SQLite: Nix's CLI fills the source store.
    spec = make_test_spec(store_id="local", store_path=store_dir, no_probe=True, postgres=None)
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    async with Server(
        stores=stores, unix_path=tmp_path / "source.sock", ssh_port=0, ssh_host_key=host_key_path, http_port=None
    ) as server:
        yield server, host_key, parent


_CONFIGURED_URL = "ssh-ng://pynixd-source?trusted=1"
"""nixkube's shape: the URL names no key and no host key, and `/etc/ssh/ssh_config` names both."""


def _ssh_config(tmp_path: Path, port: int, host_key: asyncssh.SSHKey) -> Path:
    """An OpenSSH config that leads `pynixd-source` to *port*, with *host_key* in its global known hosts file."""
    known = tmp_path / "global_known_hosts"
    known.write_text(f"[127.0.0.1]:{port} {host_key.export_public_key().decode().strip()}\n")
    config = tmp_path / "ssh_config"
    config.write_text(
        "Host pynixd-source\n"
        "  HostName 127.0.0.1\n"
        f"  Port {port}\n"
        "  User admin-user\n"
        f"  IdentityFile {SESSION_SSH_KEYS / 'id_admin'}\n"
        f"  UserKnownHostsFile {tmp_path / 'absent_known_hosts'}\n"
        f"  GlobalKnownHostsFile {tmp_path / 'absent_global_known_hosts'} {known}\n"
    )
    return config


async def _ensure(tmp_path: Path, target: Path, url: str, parent: str, *, daemon: bool) -> int:
    """EnsurePath of *parent* in a fresh store at *target* whose client names *url*, through one ssh-ng backend.

    A store with no daemon writes the substituted paths itself: `pynixd_writes`.
    """
    spec = (
        make_test_spec(store_id="local", store_path=target, no_probe=True)
        if daemon
        else make_test_spec(store_id="local", store_path=target, no_probe=True, daemon=False, pynixd_writes=True)
    )
    stores: dict[StoreId, Store] = {StoreId("local"): LocalDBStore(spec)}
    socket = tmp_path / "target.sock"
    async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None) as server:
        link = await open_unix_connection(socket)
        conn = Connection(UnixNixReader(link, identifier="t"), UnixNixWriter(link, identifier="t"), "t")
        await conn.connect()
        try:
            await conn.call(_options(substituters=url), raise_on_error=True)
            response = await conn.call(EnsurePathRequest(path=StorePath(path=parent)), raise_on_error=True)
        finally:
            await conn.close()
        assert server.ctx.scheduler is not None
        backends = server.ctx.scheduler.substitution_queue.client_substituters.stores()
    assert [type(store) for store in backends] == [SSHSubprocessStore]
    value: int = response.value
    return value


@pytest.mark.parametrize("daemon", [True, False], ids=["daemon", "daemonless"])
async def test_a_client_named_ssh_ng_substituter_fetches_the_closure(tmp_path: Path, daemon: bool) -> None:
    target = tmp_path / "target"
    async with _source(tmp_path) as (source, host_key, parent):
        answer = await _ensure(tmp_path, target, _url(source.port, host_key), parent, daemon=daemon)
    assert answer == 1
    assert await _valid(target) == await _valid(tmp_path / "source")


def _public_key(secret: str) -> str:
    """`nix key convert-secret-to-public`: an ed25519 secret key holds the public key in its last 32 bytes."""
    name, _, encoded = secret.strip().partition(":")
    return f"{name}:{base64.b64encode(base64.b64decode(encoded)[32:]).decode()}"


async def test_a_signature_added_after_a_refusal_is_seen(tmp_path: Path) -> None:
    """nixidae/nixidae#1674: sg1's nodes asked pynixd before ekn signed the path, and refused it until a restart."""
    target = tmp_path / "target"
    _rc, secret, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "key", "generate-secret", "--key-name", "cluster-1"], verbose=False
    )
    key_file = tmp_path / "cluster.key"
    key_file.write_text(secret)
    socket = tmp_path / "target.sock"
    async with contextlib.AsyncExitStack() as stack:
        source, host_key, parent = await stack.enter_async_context(_source(tmp_path))
        url = _url(source.port, host_key).removesuffix("&trusted=1")
        # The node's shape: the substituter and the cluster key in the server's own configuration.
        config = for_test_store(substituters=(url,), trusted_public_keys=(_public_key(secret),), require_sigs=True)
        spec = make_test_spec(
            store_id="local", store_path=target, no_probe=True, daemon=False, pynixd_writes=True, nix_config=config
        )
        await stack.enter_async_context(
            Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None)
        )
        options = _options()

        async def ensure() -> bool:
            link = await open_unix_connection(socket)
            conn = Connection(UnixNixReader(link, identifier="t"), UnixNixWriter(link, identifier="t"), "t")
            await conn.connect()
            try:
                await conn.call(options, raise_on_error=True)
                await conn.call(EnsurePathRequest(path=StorePath(path=parent)), raise_on_error=True)
            except BackendError:
                return False
            finally:
                await conn.close()
            return True

        unsigned = await ensure()
        await run_subproc(
            [str(NIX_BIN), "store", "sign", "--store", url, "--key-file", key_file, "--recursive", parent]
        )
        signed = await ensure()
    assert (unsigned, signed) == (False, True)


async def test_the_openssh_config_names_the_key_the_host_and_its_host_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nix's `ssh` reads `ssh_config`, and so does pynixd: the key, the user, the address and the known hosts file."""
    target = tmp_path / "target"
    async with _source(tmp_path) as (source, host_key, parent):
        config = _ssh_config(tmp_path, source.port, host_key)
        monkeypatch.setattr(store_url, "SSH_NG_CONFIG_FILES", (str(tmp_path / "absent_config"), str(config)))
        answer = await _ensure(tmp_path, target, _CONFIGURED_URL, parent, daemon=False)
    assert answer == 1
    assert await _valid(target) == await _valid(tmp_path / "source")


async def test_a_host_key_the_openssh_config_does_not_list_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A known hosts file that lists another key refuses the host; with no daemon the URL is skipped, not sent on."""
    other = asyncssh.generate_private_key("ssh-ed25519")
    async with _source(tmp_path) as (source, host_key, _parent):
        wrong, right = tmp_path / "wrong", tmp_path / "right"
        wrong.mkdir()
        right.mkdir()
        monkeypatch.setattr(store_url, "SSH_NG_CONFIG_FILES", (str(_ssh_config(wrong, source.port, other)),))
        substituters = ClientSubstituters(PynixdSettings(), runs_daemon=lambda: False)
        async with substituters.running():
            refused = await substituters.resolve([_CONFIGURED_URL], [])
            made_after_refusal = substituters.stores()
            monkeypatch.setattr(store_url, "SSH_NG_CONFIG_FILES", (str(_ssh_config(right, source.port, host_key)),))
            accepted = await substituters.resolve([_CONFIGURED_URL], [])
    assert (refused, made_after_refusal) == ((), ())
    assert accepted is not None
    assert [type(store) for store in accepted] == [SSHSubprocessStore]


async def test_a_host_key_that_does_not_match_gets_no_backend(tmp_path: Path) -> None:
    """The host refuses nothing; pynixd refuses the host. The daemon then answers for the URL."""
    other = asyncssh.generate_private_key("ssh-ed25519")
    async with _source(tmp_path) as (source, host_key, _parent):
        substituters = ClientSubstituters(PynixdSettings())
        async with substituters.running():
            assert await substituters.resolve([_url(source.port, other)], []) is None
            assert substituters.stores() == ()
            right = await substituters.resolve([_url(source.port, host_key)], [])
            assert right is not None
            assert [type(store) for store in right] == [SSHSubprocessStore]
