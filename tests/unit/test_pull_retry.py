"""A built output that fails to arrive is pulled again, and named only once it arrives.

A 1.6 GB output failed with "0 bytes read", and the same pull worked minutes
later, so the build failed for a stream that dropped once. The failed pull
also left `ctx.output_locations` naming the builder, and `_ensure_opaque`
then answered success for a path that no local store held.
nixidae/nixidae#942, nixidae/nixidae#1003.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.ids import StoreId
from pynixd.build_queue import BuildQueue
from pynixd.serde import BuildDerivationResponse, BuildResult, BuildResultStatus
from tests._conftest.execution import Streamed, fake_executor, queued
from tests._conftest.placement import FakeStore

OUTPUT = "/nix/store/00000000000000000000000000000001-big"
BUILDER = StoreId("sg1-n1")


async def _collect(
    stream: Streamed, stores: dict[StoreId, FakeStore], store: FakeStore, located: dict[str, StoreId] | None = None
) -> None:
    """Collect a built output from *store*; *located* gets where pynixd then says the output is."""
    queue = BuildQueue()
    build = await queued(queue, outputs={"out": OUTPUT})
    executor = fake_executor(queue, stores, stream=stream, output_locations=located)
    response = BuildDerivationResponse(result=BuildResult(status=BuildResultStatus.BUILT))
    await executor.collect_outputs(build, store, response)


class TestAPullThatFails:
    @pytest.mark.anyio
    async def test_is_tried_again(self) -> None:
        stream = Streamed(fail_first=2)
        store = FakeStore(BUILDER)
        located: dict[str, StoreId] = {}

        await _collect(stream, {BUILDER: store}, store, located)

        assert len(stream.calls) == 3
        assert located == {OUTPUT: BUILDER}

    @pytest.mark.anyio
    async def test_names_no_builder_when_it_never_arrives(self) -> None:
        stream = Streamed(fail_first=3)
        store = FakeStore(BUILDER)
        located: dict[str, StoreId] = {}

        with pytest.raises(EOFError):
            await _collect(stream, {BUILDER: store}, store, located)

        assert len(stream.calls) == 3
        assert located == {}


class TestABuilderThatReconnects:
    @pytest.mark.anyio
    async def test_is_pulled_from_through_its_new_connection(self) -> None:
        """A reverse builder that restarts registers again as a new store
        under the same id, and the old one has no connection left."""
        returned = FakeStore(BUILDER)
        gone = FakeStore(BUILDER)
        stream = Streamed(fail_first=1)

        await _collect(stream, {BUILDER: returned}, gone)

        assert [transfer.src for transfer in stream.calls] == [gone, returned]
