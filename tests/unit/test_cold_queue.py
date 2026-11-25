"""The cold tier's upload queue as a state machine, with no bucket. nixidae/nixidae#1600."""

from __future__ import annotations

from pynixd.cold_queue import Lane, Stage, UploadQueue, Work


def _drain_nars(queue: UploadQueue) -> list[str]:
    taken: list[str] = []
    while (work := queue.take()) is not None:
        assert work.stage is Stage.NAR
        taken.append(work.path)
    return taken


def test_background_goes_in_insertion_order() -> None:
    queue = UploadQueue()
    assert queue.add(["c", "a", "b"]) == 3
    assert _drain_nars(queue) == ["c", "a", "b"]


def test_urgent_overtakes_background() -> None:
    queue = UploadQueue()
    queue.add(["a", "b", "c"])
    queue.raise_urgent(["c"])
    assert _drain_nars(queue) == ["c", "a", "b"]


def test_adding_a_held_path_urgent_raises_it_and_counts_nothing_new() -> None:
    queue = UploadQueue()
    queue.add(["a", "b"])
    assert queue.add(["b"], Lane.URGENT) == 0
    assert _drain_nars(queue) == ["b", "a"]


def test_a_path_without_uncovered_references_publishes_after_its_nar() -> None:
    queue = UploadQueue()
    queue.add(["a"])
    assert queue.take() == Work("a", Stage.NAR)
    assert queue.staged("a", set()) == []
    assert queue.take() == Work("a", Stage.PUBLISH)
    queue.covered("a")
    assert queue.take() is None
    assert "a" not in queue


def test_a_self_reference_does_not_gate() -> None:
    queue = UploadQueue()
    queue.add(["a"])
    queue.take()
    queue.staged("a", {"a"})
    assert queue.take() == Work("a", Stage.PUBLISH)


def test_a_referrer_waits_for_its_reference_and_the_cover_releases_it() -> None:
    """The narinfo gate: `parent` stays unpublished while `child` is uncovered."""
    queue = UploadQueue()
    queue.add(["parent", "child"])
    assert queue.take() == Work("parent", Stage.NAR)
    queue.staged("parent", {"child"})
    assert queue.take() == Work("child", Stage.NAR)
    assert queue.take() is None
    queue.staged("child", set())
    assert queue.take() == Work("child", Stage.PUBLISH)
    assert queue.take() is None
    queue.covered("child")
    assert queue.take() == Work("parent", Stage.PUBLISH)


def test_a_reference_the_queue_does_not_hold_joins_the_referrers_lane() -> None:
    """A path registered after the last fill: the gate queues it rather than waiting for ever."""
    queue = UploadQueue()
    queue.add(["parent"], Lane.URGENT)
    queue.add(["other"])
    queue.take()
    queue.staged("parent", {"late"})
    assert queue.take() == Work("late", Stage.NAR)


def test_raising_a_gated_path_raises_what_it_waits_on() -> None:
    queue = UploadQueue()
    queue.add(["parent", "other", "child"])
    queue.take()
    queue.staged("parent", {"child"})
    queue.raise_urgent(["parent"])
    assert queue.take() == Work("child", Stage.NAR)


def test_a_cover_seen_before_the_gate_does_not_gate() -> None:
    """The record was read before `child`'s cover landed: the queue remembers the cover."""
    queue = UploadQueue()
    queue.add(["parent", "child"])
    queue.take()
    child = queue.take()
    assert child == Work("child", Stage.NAR)
    queue.staged("child", set())
    queue.take()
    queue.covered("child")
    queue.staged("parent", {"child"})
    assert queue.take() == Work("parent", Stage.PUBLISH)
    assert queue.add(["child"]) == 0


def test_a_parked_reference_parks_its_waiters() -> None:
    """Every waiting state has an exit: a reference that cannot go up takes its referrers out too."""
    queue = UploadQueue()
    queue.add(["top", "parent", "child"])
    queue.take()
    queue.staged("top", {"parent"})
    queue.take()
    queue.staged("parent", {"child"})
    assert queue.take() == Work("child", Stage.NAR)
    assert sorted(queue.park("child")) == ["child", "parent", "top"]
    assert queue.take() is None
    assert queue.depth()["gated"] == 0
    assert queue.depth()["parked"] == 3
    assert queue.add(["top"]) == 0


def test_a_dropped_reference_parks_its_waiters_and_may_come_back() -> None:
    queue = UploadQueue()
    queue.add(["parent", "child"])
    queue.take()
    queue.staged("parent", {"child"})
    queue.take()
    assert queue.drop("child") == ["parent"]
    assert queue.take() is None
    assert queue.add(["child"]) == 1
    assert queue.add(["parent"]) == 0


def test_gating_on_a_parked_reference_parks_at_once() -> None:
    queue = UploadQueue()
    queue.add(["parent", "child"])
    queue.take()
    queue.take()
    queue.park("child")
    assert queue.staged("parent", {"child"}) == ["parent"]


def test_a_retry_goes_to_the_head_of_its_lane() -> None:
    queue = UploadQueue()
    queue.add(["a", "b"])
    work = queue.take()
    assert work == Work("a", Stage.NAR)
    queue.retry(work)
    assert _drain_nars(queue) == ["a", "b"]


def test_a_publish_retry_stays_a_publish() -> None:
    queue = UploadQueue()
    queue.add(["a"])
    queue.take()
    queue.staged("a", set())
    work = queue.take()
    assert work == Work("a", Stage.PUBLISH)
    queue.retry(work)
    assert queue.take() == Work("a", Stage.PUBLISH)


def test_pending_leaves_out_covered_and_parked() -> None:
    queue = UploadQueue()
    queue.add(["a", "b", "c"])
    queue.take()
    queue.staged("a", set())
    queue.take()
    queue.covered("a")
    queue.park("b")
    assert queue.pending(["a", "b", "c", "d"]) == {"c"}
