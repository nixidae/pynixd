"""An exception from pynixd's own sandbox fails the build once; the same store gets no second try.

The scheduler retries an `OSError` on the same store, because from a remote
it means a lost connection. From pynixd's sandbox it is a bug or this
machine. A wrong build log path retried the parity test's build until the
limit, each time with `Permission denied: '/n'` and no traceback.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.build_queue import BuildQueue
from pynixd.scheduler import Scheduler
from pynixd.serde import BuildResultStatus
from tests._conftest.execution import fake_executor, queued
from tests._conftest.placement import FakeStore

if TYPE_CHECKING:
    from pynixd.build_queue import QueuedBuild


class Local:
    async def build_here(self, *_args: Any) -> Any:
        raise PermissionError(13, "Permission denied", "/n")


async def _crash(store_ids: tuple[str, ...]) -> QueuedBuild:
    """A local build whose sandbox raised, with *store_ids* as the stores."""
    stores = {StoreId(n): FakeStore(StoreId(n)) for n in store_ids}
    queue = BuildQueue()
    build = await queued(queue)
    scheduler = SimpleNamespace(
        local_store=stores[LOCAL_STORE_ID],
        queue=queue,
        trigger=lambda: None,
        executor=fake_executor(queue, stores),
        _say_where_it_builds=Scheduler._say_where_it_builds,  # noqa: SLF001 -- the unit's own step
    )
    store = stores[LOCAL_STORE_ID]
    await Scheduler._build_here(
        cast("Any", scheduler), build, cast("Any", store), cast("Any", Local()), cast("Any", None)
    )  # noqa: SLF001
    return build


async def test_a_crash_with_no_other_store_fails_the_build_once() -> None:
    build = await _crash(("local",))
    assert build.store_failures == {}
    resp = build.answer.result()
    assert resp.result.status == BuildResultStatus.MISC_FAILURE
    assert "Permission denied: '/n'" in str(resp.result.error_msg)


async def test_a_crash_moves_to_a_store_that_has_not_tried() -> None:
    build = await _crash(("local", "sg1-n1"))
    assert not build.answer.done()
    assert build.store_failures == {LOCAL_STORE_ID: 1}
