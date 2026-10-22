"""Functional tests for reverse store (builder-initiated connections)."""

from __future__ import annotations

import math
import time
from typing import TYPE_CHECKING

import anyio
import asyncssh
import pytest
import structlog

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import (
    PynixdSettings,
    ReverseAcceptorSettings,
    ReverseInitiatorSettings,
    StoreRankingSettings,
)
from pynixd.store import DaemonStore, LocalSocketStore
from tests.conftest import CLIENT_BIN, STORE_PREFIX, make_test_spec, rmtree_robust, run_subproc

if TYPE_CHECKING:
    from pathlib import Path

log = structlog.get_logger(__name__)


def _keypair(tmp_path: Path, name: str) -> tuple[Path, Path]:
    """A fresh ed25519 pair: `(private_path, public_path)`."""
    key = asyncssh.generate_private_key("ssh-ed25519")
    priv = tmp_path / name
    pub = tmp_path / f"{name}.pub"
    key.write_private_key(priv)
    key.write_public_key(pub)
    return priv, pub


def _controller_settings(tmp_path: Path, builder_pub: Path) -> PynixdSettings:
    return PynixdSettings(
        ssh_port=None,
        unix_path=tmp_path / "controller.sock",
        reverse_acceptor=ReverseAcceptorSettings(
            enabled=True,
            host="127.0.0.1",
            port=0,
            host_key_path=_keypair(tmp_path, "controller")[0],
            authorized_builder_keys=[builder_pub],
        ),
        # The builder reports the guest's PSI, which is the host's: a busy host
        # put its score at -569 and the build waited for ever. These tests
        # prove routing, not the threshold. nixidae/nixidae#1013.
        ranking=StoreRankingSettings(min_schedule_score=-math.inf),
    )


def _builder_settings(
    tmp_path: Path,
    acceptor_port: int,
    builder_priv: Path,
    store_id: str,
    nix_bin: str = "nix",
) -> tuple[PynixdSettings, LocalSocketStore]:
    builder_path = STORE_PREFIX / store_id
    rmtree_robust(builder_path)
    builder_local = LocalSocketStore(
        make_test_spec(store_id="local", store_path=builder_path, no_probe=True),
    )
    settings = PynixdSettings(
        ssh_port=None,
        unix_path=tmp_path / "builder.sock",
        reverse_acceptor=ReverseAcceptorSettings(enabled=False, authorized_builder_keys=None),
        reverse_initiator=ReverseInitiatorSettings(
            enabled=True,
            acceptor_host="127.0.0.1",
            acceptor_port=acceptor_port,
            store_id=store_id,
            systems=["x86_64-linux"],
            nix_bin=nix_bin,
            server_host_key_paths=[builder_priv],
            reconnect_min_delay=0.1,
            reconnect_max_delay=1.0,
        ),
    )
    return settings, builder_local


async def test_reverse_store_registration(tmp_path: Path) -> None:
    """Builder connects to controller via reverse initiator, registers as a store.

    The acceptor pins the builder host key; the builder names no
    controller key. Registration with the expected properties proves
    the pinned handshake.
    """
    builder_store_id = "test-builder"
    builder_priv, builder_pub = _keypair(tmp_path, "builder")

    # Each server's socket in this test's own directory. The default is
    # /run/pynixd, which exists only where the NixOS module runs pynixd,
    # and there it is the live service's.
    ctrl_settings = _controller_settings(tmp_path, builder_pub)

    async with Server(settings=ctrl_settings) as controller:
        if controller.reverse_acceptor is None:
            pytest.fail("Reverse acceptor did not start")
        acceptor_port = controller.reverse_acceptor.get_port()
        log.info("controller_acceptor_listening", port=acceptor_port)

        builder_settings, builder_local = _builder_settings(tmp_path, acceptor_port, builder_priv, builder_store_id)

        builder = Server(
            stores={StoreId("local"): builder_local},
            settings=builder_settings,
        )
        await builder.start()

        try:
            store_id = StoreId(builder_store_id)

            for _ in range(50):
                if store_id in controller.stores:
                    break
                await anyio.sleep(0.1)
            else:
                pytest.fail("Builder did not register within 5 seconds")

            store = controller.stores[store_id]
            assert store.store_id == store_id
            assert isinstance(store, DaemonStore)
            assert store.systems == {"x86_64-linux"}

        finally:
            await builder.close()
            rmtree_robust(STORE_PREFIX / builder_store_id)


async def test_reverse_wrong_builder_key_registers_nothing(tmp_path: Path) -> None:
    """A builder key the acceptor did not pin never registers. Issue #75."""
    builder_store_id = "test-rogue-builder"
    _builder_priv, builder_pub = _keypair(tmp_path, "builder")
    rogue_priv, _rogue_pub = _keypair(tmp_path, "rogue")

    ctrl_settings = _controller_settings(tmp_path, builder_pub)

    async with Server(settings=ctrl_settings) as controller:
        if controller.reverse_acceptor is None:
            pytest.fail("Reverse acceptor did not start")
        acceptor_port = controller.reverse_acceptor.get_port()

        builder_settings, builder_local = _builder_settings(tmp_path, acceptor_port, rogue_priv, builder_store_id)

        builder = Server(
            stores={StoreId("local"): builder_local},
            settings=builder_settings,
        )
        await builder.start()

        try:
            for _ in range(20):
                if StoreId(builder_store_id) in controller.stores:
                    pytest.fail("Rogue builder registered with an unpinned key")
                await anyio.sleep(0.1)
        finally:
            await builder.close()
            rmtree_robust(STORE_PREFIX / builder_store_id)


_DELEGATED_EXPR = """with import <nixpkgs> {}; runCommand "pynixd-delegation-probe" {} "echo delegated > $out"
"""
"""A trivial build the test routes to the reverse builder."""


def _controller_without_local_builds(tmp_path: Path, builder_pub: Path) -> tuple[PynixdSettings, LocalSocketStore]:
    """Controller settings plus a local store that builds nothing.

    An empty feature matrix fails every `supports_derivation`, so the
    scheduler can never assign the local store: with a compatible reverse
    builder registered the build must delegate, and without one it must fail
    instead of hanging. That exclusivity is what proves delegation below.

    The store lives under `STORE_PREFIX`, not `tmp_path`: the managed daemon
    socket nests deep under the store, and the test name in `tmp_path` already
    spends most of the 107-byte Unix socket budget.
    """
    store_path = STORE_PREFIX / "delegation-controller"
    rmtree_robust(store_path)
    local = LocalSocketStore(
        make_test_spec(store_id="local", store_path=store_path, no_probe=True, feature_matrix={}),
    )
    return _controller_settings(tmp_path, builder_pub), local


async def _wait_for_builder(controller: Server, builder_store_id: str) -> None:
    store_id = StoreId(builder_store_id)
    for _ in range(50):
        if store_id in controller.stores:
            return
        await anyio.sleep(0.1)
    pytest.fail("Builder did not register within 5 seconds")


async def test_delegated_build_runs_on_the_builder(tmp_path: Path) -> None:
    """A build the controller cannot take runs on the reverse builder. Issue #79."""
    builder_store_id = "delegation-builder"
    builder_priv, builder_pub = _keypair(tmp_path, "builder")
    ctrl_settings, ctrl_local = _controller_without_local_builds(tmp_path, builder_pub)

    async with Server(
        stores={StoreId("local"): ctrl_local},
        settings=ctrl_settings,
    ) as controller:
        if controller.reverse_acceptor is None:
            pytest.fail("Reverse acceptor did not start")
        acceptor_port = controller.reverse_acceptor.get_port()

        builder_settings, builder_local = _builder_settings(tmp_path, acceptor_port, builder_priv, builder_store_id)
        builder = Server(
            stores={StoreId("local"): builder_local},
            settings=builder_settings,
        )
        await builder.start()

        try:
            await _wait_for_builder(controller, builder_store_id)

            expr_path = tmp_path / "delegated.nix"
            expr_path.write_text(_DELEGATED_EXPR)  # noqa: ASYNC240 -- test setup
            uri = f"unix://{ctrl_settings.unix_path}?root={ctrl_local.store_path}"

            rc, stdout, stderr, _both = await run_subproc(
                [
                    str(CLIENT_BIN),
                    "build",
                    "--file",
                    str(expr_path),
                    "--store",
                    uri,
                    "--no-link",
                    "--print-out-paths",
                    "--print-build-logs",
                    "--impure",
                ],
            )
            assert rc == 0, stderr
            assert "/nix/store/" in stdout
            # The controller-local store supports no system, so no local
            # build was possible: the builder is the only store that could
            # have answered, and it says so on the client log.
            assert f"building on {builder_store_id}" in stderr

            # The builder's own pynixd streams its host's samples, disk
            # included, with nothing else deployed. nixidae/nixidae#1016.
            reverse = controller.stores[StoreId(builder_store_id)]
            assert isinstance(reverse, DaemonStore)
            with anyio.fail_after(10):
                while reverse.monitor is None or reverse.monitor.health.disk is None:
                    await anyio.sleep(0.1)
            assert reverse.monitor.health.meminfo is not None
        finally:
            await builder.close()
            rmtree_robust(STORE_PREFIX / builder_store_id)
            rmtree_robust(STORE_PREFIX / "delegation-controller")


async def test_unbuildable_without_a_builder_fails_fast(tmp_path: Path) -> None:
    """No compatible store is a fast error, never a silent wait. Issue #79."""
    _builder_priv, builder_pub = _keypair(tmp_path, "builder")
    ctrl_settings, ctrl_local = _controller_without_local_builds(tmp_path, builder_pub)

    async with Server(
        stores={StoreId("local"): ctrl_local},
        settings=ctrl_settings,
    ):
        expr_path = tmp_path / "undelegated.nix"
        expr_path.write_text(_DELEGATED_EXPR)  # noqa: ASYNC240 -- test setup
        uri = f"unix://{ctrl_settings.unix_path}?root={ctrl_local.store_path}"

        try:
            started = time.monotonic()
            rc, _stdout, _stderr, _both = await run_subproc(
                [
                    str(CLIENT_BIN),
                    "build",
                    "--file",
                    str(expr_path),
                    "--store",
                    uri,
                    "--no-link",
                    "--print-out-paths",
                    "--impure",
                ],
                expected_retcode=None,
            )
            assert rc != 0
            assert time.monotonic() - started < 90.0
        finally:
            rmtree_robust(STORE_PREFIX / "delegation-controller")


async def test_builder_with_no_serve_path_registers_nothing(tmp_path: Path) -> None:
    """A builder that cannot serve never joins the scheduler. Issue #80."""
    builder_store_id = "unserving-builder"
    builder_priv, builder_pub = _keypair(tmp_path, "builder")
    ctrl_settings, ctrl_local = _controller_without_local_builds(tmp_path, builder_pub)

    async with Server(
        stores={StoreId("local"): ctrl_local},
        settings=ctrl_settings,
    ) as controller:
        if controller.reverse_acceptor is None:
            pytest.fail("Reverse acceptor did not start")
        acceptor_port = controller.reverse_acceptor.get_port()

        builder_settings, builder_local = _builder_settings(
            tmp_path,
            acceptor_port,
            builder_priv,
            builder_store_id,
            nix_bin="/nonexistent/pynixd-probe",
        )
        builder = Server(
            stores={StoreId("local"): builder_local},
            settings=builder_settings,
        )
        await builder.start()

        try:
            # The builder redials about every second; every attempt must
            # refuse at the probe, so three seconds of dials prove refusal.
            await anyio.sleep(3.0)
            if StoreId(builder_store_id) in controller.stores:
                pytest.fail("Builder with no serve path registered")
        finally:
            await builder.close()
            rmtree_robust(STORE_PREFIX / builder_store_id)
            rmtree_robust(STORE_PREFIX / "delegation-controller")
