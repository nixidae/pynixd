"""pynixd runs the local post-build hook after a build on a builder, as Nix's build hook does.

`buildWithHook` runs it once the outputs are valid (`derivation-building-goal.cc:765`).
`build-remote-input-addressed.sh:35` of Nix's functional suite counts a run on the
builder and one on the host for each derivation.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from nix_daemon_protocol.build_result import BuildResult, BuildResultStatus
from nix_daemon_protocol.store_path import StorePath
from pynixd.scheduler import Scheduler
from pynixd.serde import BuildDerivationResponse
from pynixd.store import LocalDBStore

DRV = "/nix/store/xfb3ya060dvzsa0d4i6wi4xnmf82m4qh-build-remote-input-1.drv"
OUT = "/nix/store/n3dciw9sbc2hx6727pzi31f6wvhfmrxh-build-remote-input-1"


class HookStore(LocalDBStore):
    """A local store whose post-build hook records each call; `__init__` is skipped on purpose."""

    def __init__(self, failure: str | None) -> None:  # noqa: D107 -- no store to open
        self.calls: list[tuple[str, list[str]]] = []
        self.failure = failure

    async def post_build_hook_after(self, request: Any, options: Any, out_paths: Any, on_log: Any) -> str | None:
        del options, on_log
        self.calls.append((str(request.drv_path), sorted(str(p) for p in out_paths)))
        return self.failure


def _build() -> Any:
    derivation = SimpleNamespace(output_paths=lambda: {"out": StorePath(OUT)})
    return SimpleNamespace(
        request=SimpleNamespace(drv_path=StorePath(DRV), derivation=derivation),
        options=None,
        post_log_and_fanout=None,
    )


def _resp(status: BuildResultStatus) -> BuildDerivationResponse:
    return BuildDerivationResponse(result=BuildResult(status=int(status)))


async def _run(store: HookStore, *, on_the_builder: bool, status: BuildResultStatus) -> BuildDerivationResponse:
    local_builder = object()
    fake = SimpleNamespace(local_builder=local_builder, local_store=store)
    builder = object() if on_the_builder else local_builder
    return await Scheduler._post_build_hook_here(cast("Any", fake), _build(), cast("Any", builder), _resp(status))  # noqa: SLF001 -- the hook is the unit


async def test_a_build_on_a_builder_runs_the_local_hook() -> None:
    store = HookStore(None)
    resp = await _run(store, on_the_builder=True, status=BuildResultStatus.BUILT)
    assert store.calls == [(DRV, [OUT])]
    assert resp.result.status == BuildResultStatus.BUILT


async def test_a_failing_hook_fails_the_build() -> None:
    store = HookStore("program 'hook' failed")
    resp = await _run(store, on_the_builder=True, status=BuildResultStatus.BUILT)
    assert resp.result.status == BuildResultStatus.MISC_FAILURE
    assert resp.result.error_msg == "program 'hook' failed"


async def test_a_local_build_leaves_the_hook_to_its_own_builder() -> None:
    store = HookStore(None)
    await _run(store, on_the_builder=False, status=BuildResultStatus.BUILT)
    assert store.calls == []


async def test_an_output_the_builder_already_had_runs_the_hook() -> None:
    """`ALREADY_VALID`: the local goal sees only that the hook succeeded, and runs the hook."""
    store = HookStore(None)
    await _run(store, on_the_builder=True, status=BuildResultStatus.ALREADY_VALID)
    assert store.calls == [(DRV, [OUT])]


async def test_a_failed_build_runs_no_hook() -> None:
    store = HookStore(None)
    await _run(store, on_the_builder=True, status=BuildResultStatus.PERMANENT_FAILURE)
    assert store.calls == []
