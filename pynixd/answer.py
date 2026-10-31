"""The answer of shared work, set once and awaited by any number of waiters."""

from __future__ import annotations

import anyio

_UNSET = object()


class Answer[T]:
    """One setter, any number of waiters, and no task behind it.

    A cancelled waiter cancels only its own wait. `asyncio.Future` does the
    reverse: cancelling an `await` of it cancels it for every holder, so one
    client that hung up failed a shared build for all of them.
    nixidae/nixidae#15, nixidae/nixidae#1143.
    """

    def __init__(self) -> None:
        self._event = anyio.Event()
        self._value: T | object = _UNSET
        self._error: Exception | None = None

    def done(self) -> bool:
        return self._event.is_set()

    def set(self, value: T) -> None:
        if self.done():
            raise RuntimeError("answer already set")
        self._value = value
        self._event.set()

    def set_exception(self, error: Exception) -> None:
        if self.done():
            raise RuntimeError("answer already set")
        self._error = error
        self._event.set()

    def result(self) -> T:
        """The value, or the error raised again; `RuntimeError` before either is set."""
        if not self.done():
            raise RuntimeError("answer not set yet")
        if self._error is not None:
            raise self._error
        return self._value  # type: ignore[return-value]  # set() stored a T: `_UNSET` never survives `done()`

    async def wait(self) -> T:
        await self._event.wait()
        return self.result()
