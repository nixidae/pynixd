"""An `ssh-ng://` substituter gets a backend of pynixd's, over a real SSH connection.

The far side is a second pynixd, serving a store that holds the paths over
its SSH listener: it answers `nix-daemon --stdio` as `sshd` and a remote
`nix-daemon` would. Its host key is in the URL
(`base64-ssh-public-host-key`), so the check is real.
"""

from __future__ import annotations

import base64
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import asyncssh

from nix_daemon_protocol.ensure_path import EnsurePathRequest
from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.store_path import StorePath
from pynixd import Server
from pynixd.config import PynixdSettings
from pynixd.connection import Connection
from pynixd.serde import SetOptionsRequest, TimeSpan, Verbosity
from pynixd.store import LocalDBStore, SSHSubprocessStore, Store
from pynixd.substituters import ClientSubstituters
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
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
    _rc, stdout, _stderr, _both = await run_subproc(
        [str(NIX_BIN), "path-info", "--store", str(store_path), "--all"], verbose=False
    )
    return {line.strip() for line in stdout.splitlines() if line.strip()}


def _options(**overrides: str) -> SetOptionsRequest:
    """A `SetOptions` request with every field, because no field has a default."""
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=False,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=1,
        max_silent_time=TimeSpan(0),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=1,
        use_substitutes=True,
        overrides=overrides,
    )


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
    stores: dict[StoreId, Store] = {
        StoreId("local"): LocalDBStore(make_test_spec(store_id="local", store_path=store_dir, no_probe=True)),
    }
    async with Server(
        stores=stores, unix_path=tmp_path / "source.sock", ssh_port=0, ssh_host_key=host_key_path, http_port=None
    ) as server:
        yield server, host_key, parent


async def test_a_client_named_ssh_ng_substituter_fetches_the_closure(tmp_path: Path) -> None:
    target = tmp_path / "target"
    async with _source(tmp_path) as (source, host_key, parent):
        stores: dict[StoreId, Store] = {
            StoreId("local"): LocalDBStore(make_test_spec(store_id="local", store_path=target, no_probe=True)),
        }
        socket = tmp_path / "target.sock"
        async with Server(stores=stores, unix_path=socket, ssh_port=None, http_port=None) as server:
            link = await open_unix_connection(socket)
            conn = Connection(UnixNixReader(link, identifier="t"), UnixNixWriter(link, identifier="t"), "t")
            await conn.connect()
            try:
                await conn.call(_options(substituters=_url(source.port, host_key)), raise_on_error=True)
                response = await conn.call(EnsurePathRequest(path=StorePath(path=parent)), raise_on_error=True)
            finally:
                await conn.close()
            assert server.ctx.scheduler is not None
            backends = server.ctx.scheduler.substitution_queue.client_substituters.stores()
    assert response.value == 1
    assert [type(store) for store in backends] == [SSHSubprocessStore]
    assert await _valid(target) == await _valid(tmp_path / "source")


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
