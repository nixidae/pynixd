"""Pynixd server configuration, CLI options, and the session-scoped server fixture."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import structlog
from environs import env

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import LocalSocketStoreSpec
from pynixd.op_routes import OpRoute
from pynixd.store.local_db import LocalDBStore
from tests._conftest.constants import (
    _NO_PROBE_FEATURE_MATRIX,
    SESSION_NIX_CONFIG,
    SESSION_STORE_PREFIX,
    STORE_PREFIX,
)
from tests._conftest.helpers import rmtree_robust
from tests._conftest.nix_config import for_test_store
from tests._conftest.ssh_keys import SESSION_SSH_KEYS, install_session_keys

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from pathlib import Path

    from pynixd.nix_config import NixConfig


log = structlog.get_logger(__name__)

TEST_OP_ROUTES: dict[str, OpRoute] = {
    "NarFromPath": OpRoute.shadow,
    "QuerySubstitutablePathInfo": OpRoute.shadow,
    "QuerySubstitutablePathInfos": OpRoute.shadow,
    "AddToStore": OpRoute.pynixd,
    "AddToStoreNar": OpRoute.pynixd,
    "AddMultipleToStore": OpRoute.pynixd,
    "AddIndirectRoot": OpRoute.pynixd,
    "AddPermRoot": OpRoute.pynixd,
    "AddSignatures": OpRoute.pynixd,
    "RegisterDrvOutput": OpRoute.pynixd,
    "AddBuildLog": OpRoute.pynixd,
    "OptimiseStore": OpRoute.pynixd,
    "VerifyStore": OpRoute.pynixd,
    "CollectGarbage": OpRoute.pynixd,
    "EnsurePath": OpRoute.pynixd,
}
"""Answers on trial: in every test store, and on the daemon in a deployment.

An answer that writes has no shadow, so it takes the `pynixd` route here, in a
store with `pynixd_writes` on. `nix/functional-tests/make-shim.sh` and
`tests/parity/test_wire_parity.py` write the same routes into their
configurations."""

_WRITES = frozenset(
    {
        "AddToStore",
        "AddToStoreNar",
        "AddMultipleToStore",
        "AddIndirectRoot",
        "AddPermRoot",
        "AddSignatures",
        "RegisterDrvOutput",
        "AddBuildLog",
        "OptimiseStore",
        "VerifyStore",
        "CollectGarbage",
        "EnsurePath",
    }
)
"""The trial routes that a store with `pynixd_writes` off cannot take."""

# ── Nix binary paths ─────────────────────────────────────────────

# `NIX_BIN` was mandatory, and the development shell of the repository that
# this project came from always set it. That shell is not the shell here, so
# the whole suite failed at collection with `EnvNotSetError`. The `nix` on
# PATH is the one that a person in this shell means, so it is the default.
#
# huggorm loads the Nix libraries in process, so few tests need a Nix
# binary, and a stricter answer than "the one on PATH" buys nothing.
NIX_BIN: Path = env.path("NIX_BIN", None) or Path(shutil.which("nix") or "nix")

# Nix's master branch, for what no release has yet: `builder-rpc-v0`. `None` where nothing pins it.
NIX_GIT_BIN: Path | None = env.path("NIX_GIT_BIN", None)

# The oldest Nix pynixd supports, `supportedNixFloor`. `None` where nothing pins it.
NIX_FLOOR_BIN: Path | None = env.path("NIX_FLOOR_BIN", None)

# One binary, for the client, for the local store and for the builder. No
# test runs Lix, though pynixd supports its protocol, 1.35.
CLIENT_BIN: Path = NIX_BIN


# ── CLI options ───────────────────────────────────────────────────


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--async-test-timeout",
        type=float,
        default=120.0,
        help="Timeout in seconds for async tests wrapped with asyncio.timeout",
    )


def pytest_configure(config: pytest.Config) -> None:
    # Unregister pytest-asyncio — we use anyio for async test execution.
    asyncio_plugin = config.pluginmanager.get_plugin("asyncio")
    if asyncio_plugin is not None:
        config.pluginmanager.unregister(asyncio_plugin)
    install_session_keys()
    # Off by default, so on here: every build of every suite then runs the
    # trace's path through the scheduler. nixidae/nixidae#1070.
    os.environ.setdefault("PYNIXD_PLACEMENT_TRACE", '{"enabled": true}')


# ── URI helpers ───────────────────────────────────────────────────


def server_uri(server: Server) -> str:
    """Return the URI of the server, with the untrusted session key."""
    return f"{server.uri()}?ssh-key={SESSION_SSH_KEYS / 'id_user'}"


def ssh_admin_uri(server: Server) -> str:
    """Return an SSH URI for admin-user on the given server, with the trusted key."""
    return f"ssh-ng://admin-user@127.0.0.1:{server.port}?ssh-key={SESSION_SSH_KEYS / 'id_admin'}"


def ssh_user_uri(server: Server) -> str:
    """Return an SSH URI for regular-user on the given server, with an untrusted key."""
    return f"ssh-ng://regular-user@127.0.0.1:{server.port}?ssh-key={SESSION_SSH_KEYS / 'id_user'}"


def unix_session_uri(server: Server) -> str:
    """Return a Unix socket URI pointing to the session server."""
    socket_path = SESSION_STORE_PREFIX / "pynixd.sock"
    local_path = server.local_store.daemon_client.store_path
    return f"unix://{socket_path}?root={local_path}"


# ── Store spec factory ────────────────────────────────────────────


def make_test_spec(
    store_id: str = "local",
    store_path: Path | None = None,
    nix_config: NixConfig | None = None,
    no_probe: bool = False,
    monitor: bool = False,
    extra_args: list[str] | None = None,
    extra_env: dict[str, str] | None = None,
    **kwargs: Any,
) -> LocalSocketStoreSpec:
    """Create a LocalSocketStoreSpec with test defaults.

    An explicit `daemon`, an op the caller routes to the daemon or its
    shadow, or `managed=False` keeps `PYNIXD_NO_DAEMON` off the spec. A bare
    daemon client (`LocalStore`) passes `daemon=True`: it has no answer of its
    own. An unmanaged store is a daemon that someone else runs.
    """
    asks_the_daemon = (
        "daemon" in kwargs
        or kwargs.get("managed") is False
        or any(OpRoute(route) is not OpRoute.pynixd for route in kwargs.get("op_routes", {}).values())
    )
    if nix_config is None:
        nix_config = for_test_store()
    daemon_args = nix_config.to_daemon_args()
    if extra_args is not None:
        daemon_args.extend(extra_args)

    if extra_env is None:
        extra_env = {}
    if "NIX_SSHOPTS" not in extra_env:
        from tests._conftest.constants import DEFAULT_SSH_OPTS

        extra_env["NIX_SSHOPTS"] = DEFAULT_SSH_OPTS
    if "NIX_CONFIG" not in extra_env:
        extra_env["NIX_CONFIG"] = nix_config.to_nix_config_env()

    if no_probe:
        kwargs.setdefault("probe", False)
        kwargs.setdefault("feature_matrix", _NO_PROBE_FEATURE_MATRIX)

    if store_path is None:
        store_path = STORE_PREFIX / store_id

    nix_bin = str(kwargs.pop("nix_bin", NIX_BIN))
    # A test store is the test's own, so pynixd may write it, as a deployment may not by default.
    kwargs.setdefault("pynixd_writes", True)
    # The answers still on trial run in shadow here, and on the daemon in a deployment.
    # A store without its database has no answer of pynixd's to shadow.
    if kwargs.get("use_db", True):
        trial = {op: route for op, route in TEST_OP_ROUTES.items() if kwargs["pynixd_writes"] or op not in _WRITES}
        if os.environ.get("PYNIXD_NO_DAEMON") == "1" and kwargs["pynixd_writes"] and not asks_the_daemon:
            # `PYNIXD_NO_DAEMON` in `tests/derivations/guest`: every store with no daemon, so each test that fails names a gap.
            kwargs["daemon"] = False
        if kwargs.get("daemon") is False:
            # The caller's own routes stay, so one the store cannot take still refuses.
            trial = dict.fromkeys(trial, OpRoute.pynixd)
        kwargs["op_routes"] = {**trial, **kwargs.get("op_routes", {})}

    return LocalSocketStoreSpec(
        store_id=StoreId(store_id),
        store_path=store_path,
        nix_bin=nix_bin,
        nix_config=nix_config,
        extra_args=daemon_args,
        extra_env=extra_env,
        monitor=monitor,
        **kwargs,
    )


# ── Session fixtures ──────────────────────────────────────────────


@pytest.fixture(scope="session")
def nix_env() -> dict[str, str]:
    """Environment variables for nix subprocess calls."""
    return {}


@pytest.fixture(scope="session", autouse=True)
async def pynixd_server(
    anyio_backend: tuple[str, dict[str, bool]],
    # **Named to order the two, not to use it.** Both are session-scoped and
    # autouse, and pytest then orders them by collection. This store spawns
    # daemons and runs the capability probe, and with the wrong order none of
    # that reaches a log file at all. Issue #47.
    session_logging: Path,  # noqa: ARG001
    request: pytest.FixtureRequest,
    tmp_path_factory: pytest.TempPathFactory,
) -> AsyncGenerator[Server]:
    """Session-scoped shared pynixd server (autouse)."""
    local_path = SESSION_STORE_PREFIX / "local"
    builder_path = SESSION_STORE_PREFIX / "builder"
    socket_path = SESSION_STORE_PREFIX / "pynixd.sock"

    rmtree_robust(local_path)
    rmtree_robust(builder_path)
    rmtree_robust(socket_path)

    local_store = LocalDBStore(
        make_test_spec(
            store_id="local",
            store_path=local_path,
            nix_config=SESSION_NIX_CONFIG,
            nix_bin=str(NIX_BIN),
        ),
    )
    builder_store = LocalDBStore(
        make_test_spec(
            store_id="builder",
            store_path=builder_path,
            nix_config=SESSION_NIX_CONFIG,
            nix_bin=str(NIX_BIN),
        ),
    )

    upload_dir = tmp_path_factory.mktemp("http-uploads")

    async with Server(
        stores={StoreId("local"): local_store, StoreId("builder"): builder_store},
        ssh_port=0,
        http_port=0,
        unix_path=socket_path,
        http_upload_dir=upload_dir,
        http_user="testuser",
        http_pass="testpass",
    ) as server:
        # The server listens before `builder` has probed. Its 12 probe builds then
        # land in the op counters of whichever test runs first.
        if server.ctx.scheduler is not None and server.ctx.scheduler.stores_opened is not None:
            await server.ctx.scheduler.stores_opened.wait()
        yield server
