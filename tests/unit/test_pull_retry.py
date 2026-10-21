"""A built output that fails to arrive is pulled again, and named only once it arrives.

A 1.6 GB output failed with "0 bytes read", and the same pull worked minutes
later, so the build failed for a stream that dropped once. The failed pull
also left `ctx.output_locations` naming the builder, and `_ensure_opaque`
then answered success for a path that no local store held.
nixidae/nixidae#942, nixidae/nixidae#1003.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.scheduler import Scheduler
from pynixd.serde import BasicDerivation, BuildDerivationResponse, BuildResult, BuildResultStatus, DerivationOutput

OUTPUT = "/nix/store/00000000000000000000000000000001-big"
BUILDER = StoreId("sg1-n1")


class PullScheduler:
    """Enough of `Scheduler` for `_collect_outputs`, with a pull that fails *failures* times."""

    _pull_with_retries = Scheduler._pull_with_retries  # noqa: SLF001 -- the retry is the unit
    _record_observation = Scheduler._record_observation  # noqa: SLF001 -- records nothing without a local store

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.pulls = 0
        self.local_store = None
        self.ctx = SimpleNamespace(output_locations={})

    async def _pull_outputs(self, _store: Any, _paths: Any) -> int:
        self.pulls += 1
        if self.pulls <= self.failures:
            raise EOFError("0 bytes read on a total of 1668838416 expected bytes")
        return 1668838416


async def _collect(scheduler: PullScheduler) -> None:
    build = SimpleNamespace(
        build_id=1,
        observation=None,
        request=SimpleNamespace(
            derivation=BasicDerivation(
                platform="x86_64-linux", builder="/bin/sh", outputs={"out": DerivationOutput(path=OUTPUT)}
            )
        ),
    )
    response = BuildDerivationResponse(result=BuildResult(status=BuildResultStatus.BUILT))
    await Scheduler._collect_outputs(  # noqa: SLF001 -- the collection is the unit
        cast("Any", scheduler),
        cast("Any", build),
        cast("Any", SimpleNamespace(store_id=BUILDER)),
        cast("Any", None),
        response,
    )


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    async def instantly(_seconds: float) -> None:
        return

    monkeypatch.setattr("pynixd.scheduler.anyio.sleep", instantly)


class TestAPullThatFails:
    @pytest.mark.anyio
    async def test_is_tried_again(self):
        scheduler = PullScheduler(failures=2)

        await _collect(scheduler)

        assert scheduler.pulls == 3
        assert scheduler.ctx.output_locations == {OUTPUT: BUILDER}

    @pytest.mark.anyio
    async def test_names_no_builder_when_it_never_arrives(self):
        scheduler = PullScheduler(failures=3)

        with pytest.raises(EOFError):
            await _collect(scheduler)

        assert scheduler.pulls == 3
        assert scheduler.ctx.output_locations == {}
