"""The background watch refreshes Python liveness without daemon traces."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from pathlib import Path

import anyio
import pytest

from pynixd.instance import Server
from pynixd.liveness_watch import DirtyFlag
from pynixd_store.gc import LivenessWatch
from pynixd_store.local_store_db import LocalStoreDB
from pynixd_store.store_layout import StoreLayout

A = "/nix/store/00000000000000000000000000000000-a"
B = "/nix/store/11111111111111111111111111111111-b"


@dataclass
class StubTracker:
    """The `RootsTracker` shape, with a preset live set and no filesystem."""

    live: set[str] = field(default_factory=set)
    refreshed: int = 0

    def refresh(self) -> set[str]:
        self.refreshed += 1
        return set(self.live)


async def test_refresh_never_asks_nix() -> None:
    """Refresh walks the mirror and asks Nix nothing: no lock, no trace."""
    tracker = StubTracker(live={A, B})
    watch = LivenessWatch(tracker)  # type: ignore[arg-type]  # stub tracker

    assert await watch.refresh() == {A, B}

    assert tracker.refreshed == 1


def test_a_watch_needs_a_layout_and_a_database(tmp_path: Path) -> None:
    active = LocalStoreDB(db_path=tmp_path / "db.sqlite", store_path=None, read_only=True, reference_flush_interval=60)
    inactive = LocalStoreDB(db_path=None, store_path=None, read_only=True, reference_flush_interval=60)
    layout = StoreLayout.chroot(tmp_path)
    assert LivenessWatch.of(None, active) is None
    assert LivenessWatch.of(layout, inactive) is None


def test_a_watch_mirrors_the_served_store(tmp_path: Path) -> None:
    """The tracker reads the layout's state and names its path prefix."""
    layout = StoreLayout(
        store_dir=Path("/nix/store"),
        real_store_dir=Path("/nix/store"),
        state_dir=tmp_path / "state",
        log_dir=tmp_path / "log",
        relocated=False,
    )

    db = LocalStoreDB(db_path=layout.db_path, store_path=None, read_only=True, reference_flush_interval=60)

    watch = LivenessWatch.of(layout, db)

    assert watch is not None
    assert watch.tracker.state_dir == layout.state_dir
    assert watch.tracker.store_dir == "/nix/store"
    assert watch.tracker.db is db


class PassesOnly:
    """The one piece of server state a tick reads: the marker a drain waits on."""

    def _singleton_pass(self) -> contextlib.nullcontext[None]:
        return contextlib.nullcontext()


@pytest.mark.parametrize("root_change", [False, True])
async def test_background_tick_uses_python_only(root_change: bool) -> None:
    tracker = StubTracker(live={A, B})
    watch = LivenessWatch(tracker)  # type: ignore[arg-type]  # stub tracker
    dirty = DirtyFlag()
    if root_change:
        dirty.set()
    with anyio.move_on_after(0.1):
        await Server._liveness_tick(PassesOnly(), watch, 0.01, dirty)  # type: ignore[arg-type]  # no server state
    assert tracker.refreshed > 0
