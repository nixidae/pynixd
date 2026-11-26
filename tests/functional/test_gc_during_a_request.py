"""A collection during a build request leaves what the request made for its client (nixidae/nixidae#1636).

dynhetz, 2026-11-26: an `ekn kubeapply` read `...-dynatron.drv`, a `.drv` that `dynatron.drv.drv` produces. Its
request had made that `.drv` again, and a collection under disk pressure deleted it before the client resolved
through it: "store path '...-dynatron.drv' does not exist". Nix's worker roots each path as its goal makes it
valid, for the client's whole session.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.daemon_extensions import PynixdGCAction
from pynixd.gc import Collector
from pynixd.goals.requests import BuildPathsWithResultsGoal
from pynixd.store import LocalDBStore
from tests._conftest.constants import SESSION_NIX_CONFIG
from tests._conftest.nix_config import for_dynamic_derivations, host_daemon_substituters
from tests.conftest import CLIENT_BIN, TEST_NIX, make_test_spec, run_subproc
from tests.functional.test_gc_max_age import MAX_AGE, _freeze_tracker

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from nix_daemon_protocol.build_paths_with_results import BuildPathsWithResultsResponse

DYN_NIX_CONFIG = for_dynamic_derivations(substituters=host_daemon_substituters())


@asynccontextmanager
async def _pressed(tmp_path: Path) -> AsyncIterator[tuple[Server, str]]:
    """A server whose store no collection can bring under its target, so a pass takes every dead path."""
    store = tmp_path / "store"
    socket = tmp_path / "pynixd.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=store,
        nix_config=SESSION_NIX_CONFIG,
        no_probe=True,
        postgres=None,
        gc_max_age=MAX_AGE,
        gc_target_usage=1e-9,
        gc_allow_execute=True,
    )
    async with Server(
        stores={StoreId("local"): LocalDBStore(spec)}, unix_path=socket, ssh_port=None, http_port=None
    ) as server:
        _freeze_tracker(server)
        yield server, f"unix://{socket}?root={store}"


@pytest.mark.ca_derivations
async def test_a_collection_during_the_request_leaves_the_drv_it_made_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with _pressed(tmp_path) as (server, uri):
        store = ["--option", "builders", "", "--store", uri]
        by_attr = ["--impure", "--file", str(TEST_NIX), "dyn.nestedCaUser"]
        build = [str(CLIENT_BIN), "build", *store, "--no-link", "--print-out-paths"]
        _, producer, _, _ = await run_subproc(
            [
                str(CLIENT_BIN),
                "eval",
                *store,
                "--impure",
                "--file",
                str(TEST_NIX),
                "dyn.producingCaWrapper.drvPath",
                "--raw",
            ],
            nix_config=DYN_NIX_CONFIG,
            expected_retcode=0,
        )
        await run_subproc([*build, *by_attr], nix_config=DYN_NIX_CONFIG, expected_retcode=0)
        _, produced, _, _ = await run_subproc(
            [*build, f"{producer.strip()}^out"], nix_config=DYN_NIX_CONFIG, expected_retcode=0
        )
        produced = produced.strip()
        rc, _, _, deleted = await run_subproc(
            [str(CLIENT_BIN), "store", "delete", *store, produced], nix_config=DYN_NIX_CONFIG
        )
        assert rc == 0, f"could not delete {produced}:\n{deleted}"
        decoy = tmp_path / "decoy"
        decoy.write_text("dead")
        _, dead, _, _ = await run_subproc(
            [str(CLIENT_BIN), "store", "add", *store, "--mode", "flat", str(decoy)],
            nix_config=DYN_NIX_CONFIG,
            expected_retcode=0,
        )

        # The pass lands where dynhetz's background pass did: after the goals, before the answer.
        result = BuildPathsWithResultsGoal.result
        swept: list[set[str]] = []

        async def then_a_pass(self: BuildPathsWithResultsGoal) -> BuildPathsWithResultsResponse:
            response = await result(self)
            done = await Collector(server.ctx).run(PynixdGCAction.EXECUTE)
            swept.append({str(path) for path in done.store_paths})
            return response

        monkeypatch.setattr(BuildPathsWithResultsGoal, "result", then_a_pass)
        read = [str(CLIENT_BIN), "eval", *store, *by_attr, "--apply", "builtins.readFile", "--raw"]
        rc, stdout, _, stdboth = await run_subproc(read, nix_config=DYN_NIX_CONFIG)
    assert swept, "no pass ran"
    assert dead.strip() in swept[0], "the pass left a dead path, so it proves nothing"
    assert rc == 0, f"the read after a pass during its request failed:\n{stdboth}"
    assert stdout.strip() == "hello", stdout
