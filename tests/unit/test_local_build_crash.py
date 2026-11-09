"""An exception from pynixd's own sandbox fails the build once; the same store gets no second try.

The scheduler retries an `OSError` on the same store, because from a remote
it means a lost connection. From pynixd's sandbox it is a bug or this
machine. A wrong build log path retried the parity test's build until the
limit, each time with `Permission denied: '/n'` and no traceback.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any, cast

from nix_daemon_protocol.ids import LOCAL_STORE_ID, StoreId
from pynixd.scheduler import Scheduler
from pynixd.serde import BasicDerivation, BuildResultStatus


class Store:
    def __init__(self, store_id: str) -> None:
        self.store_id = StoreId(store_id)
        self.session_only = False

    def supports_derivation(self, *_args: Any) -> bool:
        return True


class Local:
    async def build_here(self, *_args: Any) -> Any:
        raise PermissionError(13, "Permission denied", "/n")


def _scheduler(store_ids: tuple[str, ...]) -> tuple[Any, list[Any], list[str]]:
    stores = {StoreId(n): Store(n) for n in store_ids}
    completed: list[Any] = []
    retried: list[str] = []

    async def complete(_build_id: Any, resp: Any) -> None:
        completed.append(resp)

    async def retry(_build: Any, _store: Any, _resp: Any, fault: str) -> None:
        retried.append(fault)

    scheduler = SimpleNamespace(
        local_store=stores[LOCAL_STORE_ID],
        stores=stores,
        queue=SimpleNamespace(complete=complete),
        trigger=lambda: None,
        _client_forbids_local=lambda _build: False,
        _say_where_it_builds=Scheduler._say_where_it_builds,  # noqa: SLF001 -- the unit's own step
        _retry_elsewhere=retry,
    )
    scheduler._retryable_elsewhere = lambda *a: Scheduler._retryable_elsewhere(cast("Any", scheduler), *a)  # noqa: SLF001
    return scheduler, completed, retried


def _build() -> Any:
    async def post(_message: Any) -> None:
        pass

    return SimpleNamespace(
        build_id=1,
        enqueued_at=time.monotonic(),
        wait_time=None,
        request=SimpleNamespace(derivation=BasicDerivation(platform="x86_64-linux", builder="/bin/sh")),
        store_failures={},
        options=None,
        builders=None,
        post_log_and_fanout=post,
    )


async def test_a_crash_with_no_other_store_fails_the_build_once() -> None:
    scheduler, completed, retried = _scheduler(("local",))
    store = scheduler.stores[LOCAL_STORE_ID]
    await Scheduler._build_here(cast("Any", scheduler), _build(), store, cast("Any", Local()), cast("Any", None))  # noqa: SLF001
    assert retried == []
    [resp] = completed
    assert resp.result.status == BuildResultStatus.MISC_FAILURE
    assert "Permission denied: '/n'" in str(resp.result.error_msg)


async def test_a_crash_moves_to_a_store_that_has_not_tried() -> None:
    scheduler, completed, retried = _scheduler(("local", "sg1-n1"))
    store = scheduler.stores[LOCAL_STORE_ID]
    await Scheduler._build_here(cast("Any", scheduler), _build(), store, cast("Any", Local()), cast("Any", None))  # noqa: SLF001
    assert (completed, retried) == ([], ["misc_failure"])
