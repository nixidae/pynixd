"""The background task group of a long-lived owner: the server, or a store.

nixidae/nixidae#1143.
"""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import anyio
import structlog

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine
    from typing import Any

    from anyio.abc import TaskGroup, TaskStatus

log = structlog.get_logger(__name__)


async def _logged(name: str, scope: anyio.CancelScope, fn: Callable[..., Awaitable[object]], *args: object) -> None:
    """Run *fn* in *scope*, and log it the moment it raises: the owner may take a while to report it."""
    with scope:
        try:
            await fn(*args)
        except Exception:
            log.exception("background_task_failed", task=name)
            raise


class Background:
    """Loops that run as long as their owner, started from sync code too.

    A loop that raises cancels the rest, and the error leaves the owner's
    `async with`. The owner decides what that means.
    """

    def __init__(self, owner: str) -> None:
        self.owner = owner
        self._tg: TaskGroup | None = None
        self._entered = False
        self._done = anyio.Event()

    @contextlib.asynccontextmanager
    async def running(self) -> AsyncIterator[None]:
        """Run the loops for the body of this block, and cancel them at its end.

        Two groups, not one: `aclose` cancels the inner one, and cancelling
        the outer one would cancel the owner's body too.

        An error of the body leaves as itself, not in an `ExceptionGroup`:
        `pynixd state` names the error of each socket it tried. A loop's
        error leaves in a group.
        """
        body_error: BaseException | None = None
        try:
            async with anyio.create_task_group() as tg:
                await tg.start(self._host)
                try:
                    yield
                except BaseException as e:
                    body_error = e
                    raise
                finally:
                    await self.aclose()
        except BaseExceptionGroup as group:
            if body_error is not None and group.exceptions == (body_error,):
                raise body_error from body_error.__cause__
            raise

    async def _host(self, *, task_status: TaskStatus[None]) -> None:
        self._entered = True
        try:
            async with anyio.create_task_group() as tg:
                self._tg = tg
                task_status.started()
                await anyio.sleep_forever()
        finally:
            self._tg = None
            self._done.set()

    @property
    def open(self) -> bool:
        """Whether `spawn` and `start` accept a loop now."""
        return self._tg is not None

    def spawn(self, fn: Callable[..., Awaitable[object]], *args: object) -> anyio.CancelScope:
        """Run *fn* until the owner closes, or until the answered scope is cancelled."""
        if self._tg is None:
            raise RuntimeError(f"{self.owner} runs its background loops only inside its `async with`")
        name = repr(fn)
        scope = anyio.CancelScope()
        self._tg.start_soon(_logged, name, scope, fn, *args, name=name)
        return scope

    async def start(self, fn: Callable[..., Coroutine[Any, Any, object]], *args: object) -> Any:  # noqa: ANN401 -- what *fn* hands `task_status.started`
        """Run *fn* until the owner closes, once it calls `task_status.started`.

        Raises what *fn* raises before that, and logs nothing: *fn* handles its own errors.
        """
        if self._tg is None:
            raise RuntimeError(f"{self.owner} runs its background loops only inside its `async with`")
        return await self._tg.start(fn, *args, name=repr(fn))

    async def aclose(self) -> None:
        """Cancel every loop and wait for them to end. Safe to call twice."""
        if self._tg is not None:
            self._tg.cancel_scope.cancel()
        if self._entered:
            await self._done.wait()
