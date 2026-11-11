"""Determinate Nix's `QueryActiveBuilds` (op 48) shows a build of pynixd's own builder while it runs.

`nix ps` of Determinate v3.24.0 sends it (`ps.cc:186`) once the daemon names `queryActiveBuilds`
(`remote-store.cc:909`). Upstream Nix has neither. nixidae/nixidae#1250.
"""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import anyio

from nix_daemon_protocol.ids import StoreId
from nix_daemon_protocol.query_active_builds import QueryActiveBuildsRequest
from pynixd import Server
from pynixd.connection import Connection
from pynixd.op_routes import OpRoute
from pynixd.store import LocalDBStore
from pynixd.unix_socket import open_unix_connection
from pynixd.wire import UnixNixReader, UnixNixWriter
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SYNC = "/active-sync"
SCRIPT = "active-builds-sleeper.sh"
SLEEPER = (
    'derivation { name = "active-builds-sleeper"; system = builtins.currentSystem; builder = "/bin/sh"; '
    f"args = [ (builtins.toFile \"{SCRIPT}\" ''\necho > {SYNC}/started\nread go < {SYNC}/go\necho done > $out\n'') ]; }}"
)


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, route: OpRoute) -> AsyncIterator[tuple[str, Path]]:
    """A fresh store whose `BuildDerivation` takes *route*: its URL and its socket."""
    root = tmp_path / route.name / "store"
    socket = tmp_path / f"{route.name}.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=root,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        op_routes={"BuildDerivation": route},
    )
    async with Server(stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None):
        yield f"unix://{socket}?root={root}", socket


@contextlib.asynccontextmanager
async def _connect(socket: Path) -> AsyncIterator[Connection]:
    link = await open_unix_connection(socket)
    conn = Connection(UnixNixReader(link, identifier="test"), UnixNixWriter(link, identifier="test"), "test-ps")
    await conn.connect()
    try:
        yield conn
    finally:
        await conn.close()


async def _builds(conn: Connection) -> list[dict[str, object]]:
    response = await conn.call(QueryActiveBuildsRequest(), raise_on_error=True)
    builds: list[dict[str, object]] = json.loads(response.builds)
    return builds


async def test_a_build_of_pynixds_builder_shows_until_it_ends(tmp_path: Path) -> None:
    sync = tmp_path / "sync"
    sync.mkdir()
    for name in ("started", "go"):
        os.mkfifo(sync / name)
    async with _serve(tmp_path, OpRoute.pynixd) as (url, socket), _connect(socket) as conn:
        assert "queryActiveBuilds" in conn.features
        _rc, drv, _err, _both = await run_subproc(
            [NIX_BIN, "eval", "--store", url, "--impure", "--raw", "--expr", f"({SLEEPER}).drvPath"]
        )
        drv = drv.strip()
        realise = [
            NIX_BIN.with_name("nix-store"),
            "--store",
            url,
            "--realise",
            drv,
            "--option",
            "extra-sandbox-paths",
            f"{SYNC}={sync}",
        ]

        async def realise_it() -> None:
            await run_subproc(realise)

        started = os.open(sync / "started", os.O_RDONLY | os.O_NONBLOCK)
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(realise_it)
                try:
                    with anyio.fail_after(60):
                        await anyio.wait_readable(started)
                    builds = await _builds(conn)
                finally:
                    # Release the builder whatever the query answered, or the build holds the session open.
                    go = os.open(sync / "go", os.O_RDWR)
                    os.write(go, b"\n")
                    os.close(go)
        finally:
            os.close(started)
        assert len(builds) == 1, builds
        (build,) = builds
        assert (build["type"], build["derivation"], build["nixPid"]) == ("build", Path(drv).name, os.getpid())
        processes = build["processes"]
        assert isinstance(processes, list)
        assert any(arg.endswith(SCRIPT) for process in processes for arg in process["argv"]), processes
        assert await _builds(conn) == []


async def test_a_store_whose_daemon_builds_does_not_name_query_active_builds(tmp_path: Path) -> None:
    """The daemon of upstream Nix names no `queryActiveBuilds`, and pynixd cannot see its builds."""
    async with _serve(tmp_path, OpRoute.daemon) as (_url, socket), _connect(socket) as conn:
        assert "queryActiveBuilds" not in conn.features
