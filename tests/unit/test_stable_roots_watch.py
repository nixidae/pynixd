"""The watcher wakes the check when the stable roots move, and only then.

`gc.cc:309` scans `gcroots` and `profiles`, and no profile path arrives
through `gcroots/auto`, so both trees are watched: the profiles case
below is the regression test for watching one tree and going blind on
the other. Events run through a real watchdog observer on a lab state
dir. There is no overflow test: watchdog coalesces instead of
overflowing, and a missed event degrades to the periodic poll either
way, which is the check's property, not the watcher's.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import anyio
import pytest

from pynixd.liveness_watch import DirtyFlag, StableRootsWatch

TARGET = "/nix/store/00000000000000000000000000000000-a"


def _lab(state: Path) -> tuple[Path, Path]:
    """A state dir with both trees and one link each, like a Nix system."""
    auto = state / "gcroots" / "auto"
    auto.mkdir(parents=True)
    profiles = state / "profiles"
    profiles.mkdir(parents=True)
    (auto / "direct").symlink_to(TARGET)
    (profiles / "profile").symlink_to(TARGET)
    return auto, profiles


async def _wakes(state: Path, action: Callable[[], None]) -> bool:
    """Whether *action* sets the dirty event within ten seconds."""
    watch = StableRootsWatch(state)
    dirty = DirtyFlag()
    async with anyio.create_task_group() as tg:
        tg.start_soon(watch.run, dirty)
        await anyio.sleep(0.2)
        action()
        with anyio.move_on_after(10):
            await dirty.wait()
        tg.cancel_scope.cancel()
    return dirty.is_set()


async def _stays_quiet(state: Path, action: Callable[[], None]) -> bool:
    """Whether *action* leaves the dirty event unset for half a second."""
    watch = StableRootsWatch(state)
    dirty = DirtyFlag()
    async with anyio.create_task_group() as tg:
        tg.start_soon(watch.run, dirty)
        await anyio.sleep(0.2)
        action()
        with anyio.move_on_after(0.5):
            await dirty.wait()
        tg.cancel_scope.cancel()
    return not dirty.is_set()


@pytest.mark.anyio
async def test_a_created_link_wakes(tmp_path: Path) -> None:
    auto, _profiles = _lab(tmp_path)

    assert await _wakes(tmp_path, lambda: (auto / "new").symlink_to(TARGET))


@pytest.mark.anyio
async def test_a_deleted_link_wakes(tmp_path: Path) -> None:
    auto, _profiles = _lab(tmp_path)

    assert await _wakes(tmp_path, lambda: (auto / "direct").unlink())


@pytest.mark.anyio
async def test_a_retargeted_link_wakes(tmp_path: Path) -> None:
    """A replace is a delete plus a create; either half wakes."""
    auto, _profiles = _lab(tmp_path)

    def retarget() -> None:
        (auto / "direct").unlink()
        (auto / "direct").symlink_to(TARGET)

    assert await _wakes(tmp_path, retarget)


@pytest.mark.anyio
async def test_a_new_subdirectory_is_watched(tmp_path: Path) -> None:
    """The watch follows the tree down: a link under a new dir wakes."""
    auto, _profiles = _lab(tmp_path)

    def nest() -> None:
        sub = auto / "sub"
        sub.mkdir()
        (sub / "nested").symlink_to(TARGET)

    assert await _wakes(tmp_path, nest)


@pytest.mark.anyio
async def test_the_profiles_tree_is_watched(tmp_path: Path) -> None:
    """Profiles are their own root source (`gc.cc:309`), watched like `gcroots`."""
    _auto, profiles = _lab(tmp_path)

    assert await _wakes(tmp_path, lambda: (profiles / "profile").unlink())


async def _wakes_after(state: Path, setup: Callable[[], None], action: Callable[[], None]) -> bool:
    """Whether *action* wakes the check once *setup*'s own wake is spent."""
    watch = StableRootsWatch(state)
    dirty = DirtyFlag()
    async with anyio.create_task_group() as tg:
        tg.start_soon(watch.run, dirty)
        await anyio.sleep(0.2)
        setup()
        with anyio.move_on_after(10):
            await dirty.wait()
        await anyio.sleep(0.2)
        dirty.clear()
        with anyio.move_on_after(0.5):
            await dirty.wait()
        assert not dirty.is_set(), "setup still waking: the action proves nothing"
        action()
        with anyio.move_on_after(10):
            await dirty.wait()
        tg.cancel_scope.cancel()
    return dirty.is_set()


def _inotify_watches() -> int:
    """Inotify watches this process holds, from `/proc/self/fdinfo`."""
    return sum(
        line.startswith("inotify wd:")
        for info in Path("/proc/self/fdinfo").iterdir()
        for line in _fdinfo(info).splitlines()
    )


def _fdinfo(info: Path) -> str:
    try:
        return info.read_text()
    except OSError:  # an fd closed between listing and reading
        return ""


@pytest.mark.anyio
async def test_a_tree_that_appears_late_is_watched(tmp_path: Path) -> None:
    """A link under a `gcroots/` made after start wakes, not only its birth."""
    per_user = tmp_path / "gcroots" / "per-user"

    assert await _wakes_after(
        tmp_path,
        lambda: per_user.mkdir(parents=True),
        lambda: (per_user / "x").symlink_to(TARGET),
    )


@pytest.mark.anyio
async def test_a_tree_that_comes_back_is_watched(tmp_path: Path) -> None:
    """The old watch died with the old directory; the new one gets its own."""
    auto, _profiles = _lab(tmp_path)

    def replace() -> None:
        (auto / "direct").unlink()
        auto.rmdir()
        auto.parent.rmdir()
        auto.mkdir(parents=True)

    assert await _wakes_after(tmp_path, replace, lambda: (auto / "new").symlink_to(TARGET))


@pytest.mark.anyio
@pytest.mark.skipif(not Path("/proc/self/fdinfo").is_dir(), reason="inotify fdinfo is Linux only")
async def test_no_watch_outside_the_trees(tmp_path: Path) -> None:
    """`builds/` and the rest get no watch (nixidae/nixidae#1142).

    One watch for the state dir, one per directory inside the trees:
    `gcroots`, `gcroots/auto` and `profiles`.
    """
    _lab(tmp_path)
    for n in range(50):
        (tmp_path / "builds" / str(n) / "deep").mkdir(parents=True)
    before = _inotify_watches()
    watch = StableRootsWatch(tmp_path)
    async with anyio.create_task_group() as tg:
        tg.start_soon(watch.run, DirtyFlag())
        await anyio.sleep(0.2)
        held = _inotify_watches() - before
        tg.cancel_scope.cancel()

    assert held == 4


@pytest.mark.anyio
async def test_churn_outside_the_trees_stays_quiet(tmp_path: Path) -> None:
    """`temproots/` churn must never wake the check."""
    _lab(tmp_path)
    temproots = tmp_path / "temproots"
    temproots.mkdir()

    def churn() -> None:
        (temproots / "123").write_text(f"{TARGET}\n")  # noqa: ASYNC240 -- the event under test

    assert await _stays_quiet(tmp_path, churn)
