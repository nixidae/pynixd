"""A builder that already holds the outputs still hands them over.

`execute_build` and `_collect_outputs` pulled the outputs for `BUILT` alone.
A builder that held them from an earlier build answered `ALREADY_VALID`, so
the local store never got them. A dynamic derivation's produced `.drv` then
failed with "pynixd: derivation not found" on every run, until the builder
collected it. Measured on dynhetz with croshome's kubecove: the `.drv.drv`
build on sg1-n3 took 0.2 s and logged no `pulling_paths`.
nixidae/nixidae#998.
"""

from __future__ import annotations

import contextlib
from typing import Any, cast

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.scheduler import Scheduler
from pynixd.serde import BuildResultStatus
from pynixd.store_path import StorePath

PRODUCED_DRV = "/nix/store/d72wk730icpqmcjzfrxfpn22g2riadjd-kubecove.drv"
SUCCESSES_THAT_BUILT_NOTHING = [BuildResultStatus.ALREADY_VALID, BuildResultStatus.SUBSTITUTED]


class FakeRealisation:
    out_path = PRODUCED_DRV


class FakeResult:
    error_msg = ""

    def __init__(self, status: BuildResultStatus) -> None:
        self.status = status

    def realised_outputs(self) -> dict[str, FakeRealisation]:
        return {"out": FakeRealisation()}


class FakeResponse:
    def __init__(self, status: BuildResultStatus) -> None:
        self.result = FakeResult(status)


class FakeDerivation:
    @staticmethod
    def output_paths() -> dict[str, StorePath]:
        """Floating and text-hashed: the derivation names no output path."""
        return {"out": StorePath("")}


class FakeRequest:
    derivation = FakeDerivation()


class FakeBuild:
    build_id = "b1"
    assigned_store_id: StoreId | None = None
    options = None
    request = FakeRequest()
    ca_realisations: list[Any] = []

    async def post_log_and_fanout(self, _msg: Any) -> None:
        return


class FakeStore:
    store_id = StoreId("sg1-n3")

    def build_conn(self, _options: Any = None) -> Any:
        return contextlib.nullcontext(object())


class FakeQueue:
    def __init__(self, steps: list[str]) -> None:
        self.steps = steps

    async def complete(self, _build_id: str, _response: Any) -> None:
        self.steps.append("complete")

    async def fail(self, _build_id: str, _error_msg: str) -> None:
        self.steps.append("fail")


class FakeContext:
    def __init__(self) -> None:
        self.output_locations: dict[str, StoreId] = {}


class FakeScheduler:
    """Enough of `Scheduler` for `execute_build` and `_collect_outputs`,
    called unbound, as `test_outputs_before_completion.py` does."""

    def __init__(self, status: BuildResultStatus) -> None:
        self.status = status
        self.steps: list[str] = []
        self.queue = FakeQueue(self.steps)
        self.ctx = FakeContext()
        self.local_store = None
        self.pulled: set[StorePath] = set()

    def trigger(self) -> None:
        return

    async def _prepare_build(self, *_args: Any) -> None:
        return

    async def _say_where_it_builds(self, *_args: Any) -> None:
        return

    async def _execute(self, *_args: Any) -> FakeResponse:
        return FakeResponse(self.status)

    async def _collect_outputs(self, *args: Any) -> None:
        self.steps.append("collect")
        await Scheduler._collect_outputs(cast("Any", self), *args)  # noqa: SLF001 -- the method under test

    async def _record_build_stats(self, *_args: Any) -> None:
        self.steps.append("stats")

    async def _pull_outputs(self, _store: Any, paths: set[StorePath]) -> None:
        self.pulled |= paths


async def _run(status: BuildResultStatus) -> FakeScheduler:
    scheduler = FakeScheduler(status)
    await Scheduler.execute_build(cast("Any", scheduler), cast("Any", FakeBuild()), cast("Any", FakeStore()))
    return scheduler


class TestABuilderThatBuiltNothing:
    @pytest.mark.anyio
    @pytest.mark.parametrize("status", SUCCESSES_THAT_BUILT_NOTHING)
    async def test_pulls_the_realised_output(self, status: BuildResultStatus):
        scheduler = await _run(status)

        assert scheduler.pulled == {StorePath(PRODUCED_DRV)}

    @pytest.mark.anyio
    @pytest.mark.parametrize("status", SUCCESSES_THAT_BUILT_NOTHING)
    async def test_records_where_the_output_is(self, status: BuildResultStatus):
        scheduler = await _run(status)

        assert scheduler.ctx.output_locations == {PRODUCED_DRV: StoreId("sg1-n3")}

    @pytest.mark.anyio
    @pytest.mark.parametrize("status", SUCCESSES_THAT_BUILT_NOTHING)
    async def test_records_no_build_time(self, status: BuildResultStatus):
        """A 0.2 s answer is not what the build costs."""
        scheduler = await _run(status)

        assert scheduler.steps == ["collect", "complete"]


class TestABuilderThatBuilt:
    @pytest.mark.anyio
    async def test_pulls_and_records_the_build_time(self):
        scheduler = await _run(BuildResultStatus.BUILT)

        assert scheduler.pulled == {StorePath(PRODUCED_DRV)}
        assert scheduler.steps == ["collect", "stats", "complete"]


class TestAFailedBuild:
    @pytest.mark.anyio
    async def test_pulls_nothing(self):
        scheduler = await _run(BuildResultStatus.PERMANENT_FAILURE)

        assert scheduler.pulled == set()
        assert scheduler.steps == ["complete"]
