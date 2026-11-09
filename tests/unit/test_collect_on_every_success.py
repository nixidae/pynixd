"""A builder that already holds the outputs still hands them over.

pynixd pulled the outputs for `BUILT` alone.
A builder that held them from an earlier build answered `ALREADY_VALID`, so
the local store never got them. A dynamic derivation's produced `.drv` then
failed with "pynixd: derivation not found" on every run, until the builder
collected it. Measured on dynhetz with croshome's kubecove: the `.drv.drv`
build on sg1-n3 took 0.2 s and logged no `pulling_paths`.
nixidae/nixidae#998.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue, QueuedBuild
from pynixd.serde import (
    BasicDerivation,
    BuildDerivationRequest,
    BuildDerivationResponse,
    BuildMode,
    BuildResult,
    BuildResultStatus,
)
from pynixd_store.store_path import StorePath
from tests._conftest.execution import FakeLocal, FakeSandbox, Streamed, Triggers, answering, fake_executor, queued
from tests._conftest.placement import FakeStore

PRODUCED_DRV = "/nix/store/d72wk730icpqmcjzfrxfpn22g2riadjd-kubecove.drv"
SUCCESSES_THAT_BUILT_NOTHING = [BuildResultStatus.ALREADY_VALID, BuildResultStatus.SUBSTITUTED]
BUILDER = StoreId("sg1-n3")


@dataclass
class Run:
    build: QueuedBuild
    steps: list[str]
    pulled: set[StorePath]
    located: dict[str, StoreId]
    observed: list[int]


async def _run(status: BuildResultStatus) -> Run:
    """A floating, text-hashed build on a builder: the derivation names no output path, and the answer realises it."""
    steps: list[str] = []
    store = FakeStore(BUILDER)
    store.conn.answer = answering(status, realised=PRODUCED_DRV)
    local = FakeLocal()
    sandbox = FakeSandbox()
    stream = Streamed(steps)
    located: dict[str, StoreId] = {}
    queue = BuildQueue()
    build = await queued(queue, outputs={"out": ""})
    executor = fake_executor(
        queue,
        {BUILDER: store},
        local=local,
        here=sandbox,
        stream=stream,
        trigger=Triggers(steps),
        output_locations=located,
    )
    await executor.run(build, store)
    pulled = {path for transfer in stream.calls if transfer.dst is local for path in transfer.paths}
    return Run(build, steps, pulled, located, [observation.status for observation in sandbox.db.observations])


class TestABuilderThatBuiltNothing:
    @pytest.mark.anyio
    @pytest.mark.parametrize("status", SUCCESSES_THAT_BUILT_NOTHING)
    async def test_pulls_the_realised_output(self, status: BuildResultStatus) -> None:
        run = await _run(status)

        assert run.pulled == {StorePath(PRODUCED_DRV)}

    @pytest.mark.anyio
    @pytest.mark.parametrize("status", SUCCESSES_THAT_BUILT_NOTHING)
    async def test_records_where_the_output_is(self, status: BuildResultStatus) -> None:
        run = await _run(status)

        assert run.located == {PRODUCED_DRV: BUILDER}

    @pytest.mark.anyio
    @pytest.mark.parametrize("status", SUCCESSES_THAT_BUILT_NOTHING)
    async def test_records_the_answer_under_its_own_status(self, status: BuildResultStatus) -> None:
        """A 0.2 s answer measures delegation, not the build. The estimator
        reads `BUILT` rows alone, so the row keeps its status."""
        run = await _run(status)

        assert run.observed == [int(status)]
        assert run.steps == ["copy", "trigger"]
        assert run.build.answer.result().result.status == status


class TestABuilderThatBuilt:
    @pytest.mark.anyio
    async def test_pulls_and_records_the_build(self) -> None:
        run = await _run(BuildResultStatus.BUILT)

        assert run.pulled == {StorePath(PRODUCED_DRV)}
        assert run.observed == [int(BuildResultStatus.BUILT)]
        assert run.steps == ["copy", "trigger"]


class TestAFailedBuild:
    @pytest.mark.anyio
    async def test_pulls_nothing(self) -> None:
        run = await _run(BuildResultStatus.PERMANENT_FAILURE)

        assert run.pulled == set()
        assert run.steps == ["trigger"]
        assert run.build.answer.result().result.status == BuildResultStatus.PERMANENT_FAILURE


class TestTheCompletionMetric:
    @pytest.mark.anyio
    @pytest.mark.parametrize("status", SUCCESSES_THAT_BUILT_NOTHING)
    async def test_counts_every_success_as_one(self, status: BuildResultStatus) -> None:
        queue = BuildQueue()
        build_id, _ = await queue.enqueue(
            BuildDerivationRequest(
                drv_path=StorePath(path="/nix/store/00000000000000000000000000000001-trivial.drv"),
                derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh"),
                build_mode=BuildMode.NORMAL,
            )
        )
        before = REGISTRY.get_sample_value("pynixd_builds_completed_total", {"status": "success"}) or 0.0

        await queue.complete(build_id, BuildDerivationResponse(result=BuildResult(status=status)))

        assert REGISTRY.get_sample_value("pynixd_builds_completed_total", {"status": "success"}) == before + 1
