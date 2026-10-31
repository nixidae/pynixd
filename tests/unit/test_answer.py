"""A cancelled waiter of an `Answer` leaves it to the others. nixidae/nixidae#15."""

from __future__ import annotations

import anyio
import pytest

from pynixd.answer import Answer


@pytest.mark.anyio
async def test_a_cancelled_waiter_leaves_the_answer_to_the_others() -> None:
    answer: Answer[str] = Answer()
    seen: list[str] = []

    async def keep_waiting() -> None:
        seen.append(await answer.wait())

    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(keep_waiting)
            with anyio.move_on_after(0.01):
                await answer.wait()
            assert not answer.done()
            answer.set("built")

    assert seen == ["built"]


@pytest.mark.anyio
async def test_an_error_reaches_every_waiter() -> None:
    answer: Answer[str] = Answer()
    answer.set_exception(ValueError("failed"))

    for _ in range(2):
        with pytest.raises(ValueError, match="failed"):
            await answer.wait()
    with pytest.raises(RuntimeError):
        answer.set("late")
