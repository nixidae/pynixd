"""pynixd runs the local post-build hook after a build on a builder, as Nix's build hook does.

`buildWithHook` runs it once the outputs are valid (`derivation-building-goal.cc:765`).
`build-remote-input-addressed.sh:35` of Nix's functional suite counts a run on the
builder and one on the host for each derivation.
"""

from __future__ import annotations

from nix_daemon_protocol.build_result import BuildResult, BuildResultStatus
from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from nix_daemon_protocol.store_path import StorePath
from pynixd.build_queue import BuildQueue
from pynixd.serde import BuildDerivationResponse
from tests._conftest.execution import FakeSandbox, fake_executor, queued
from tests._conftest.placement import FakeStore

OUT = "/nix/store/n3dciw9sbc2hx6727pzi31f6wvhfmrxh-build-remote-input-1"


async def _run(sandbox: FakeSandbox, *, on_the_builder: bool, status: BuildResultStatus) -> BuildDerivationResponse:
    stores = {LOCAL_STORE_ID: FakeStore(LOCAL_STORE_ID), StoreId("sg1-n1"): FakeStore(StoreId("sg1-n1"))}
    queue = BuildQueue()
    build = await queued(queue, outputs={"out": OUT})
    store = stores[StoreId("sg1-n1")] if on_the_builder else stores[LOCAL_STORE_ID]
    resp = BuildDerivationResponse(result=BuildResult(status=int(status)))
    return await fake_executor(queue, stores, here=sandbox).post_build_hook_here(build, store, resp)


async def test_a_build_on_a_builder_runs_the_local_hook() -> None:
    sandbox = FakeSandbox()
    resp = await _run(sandbox, on_the_builder=True, status=BuildResultStatus.BUILT)
    assert sandbox.hooks == [{StorePath(OUT)}]
    assert resp.result.status == BuildResultStatus.BUILT


async def test_a_failing_hook_fails_the_build() -> None:
    sandbox = FakeSandbox(hook_failure="program 'hook' failed")
    resp = await _run(sandbox, on_the_builder=True, status=BuildResultStatus.BUILT)
    assert resp.result.status == BuildResultStatus.MISC_FAILURE
    assert resp.result.error_msg == "program 'hook' failed"


async def test_a_local_build_leaves_the_hook_to_its_own_builder() -> None:
    sandbox = FakeSandbox()
    await _run(sandbox, on_the_builder=False, status=BuildResultStatus.BUILT)
    assert sandbox.hooks == []


async def test_an_output_the_builder_already_had_runs_the_hook() -> None:
    """`ALREADY_VALID`: the local goal sees only that the hook succeeded, and runs the hook."""
    sandbox = FakeSandbox()
    await _run(sandbox, on_the_builder=True, status=BuildResultStatus.ALREADY_VALID)
    assert sandbox.hooks == [{StorePath(OUT)}]


async def test_a_failed_build_runs_no_hook() -> None:
    sandbox = FakeSandbox()
    await _run(sandbox, on_the_builder=True, status=BuildResultStatus.PERMANENT_FAILURE)
    assert sandbox.hooks == []
