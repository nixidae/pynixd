"""The background watch refreshes Python liveness without daemon traces."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio
import pytest

from pynixd.gc import LivenessWatch
from pynixd.instance import Server
from pynixd.liveness_watch import DirtyFlag
from pynixd.store_layout import StoreLayout

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


def test_from_local_needs_a_layout() -> None:
    assert LivenessWatch.from_local(object()) is None  # type: ignore[arg-type]  # no layout


def test_from_local_mirrors_the_served_store(tmp_path: Path) -> None:
    """The tracker reads the layout's state and names its path prefix."""
    layout = StoreLayout(
        store_dir=Path("/nix/store"),
        real_store_dir=Path("/nix/store"),
        state_dir=tmp_path / "state",
        relocated=False,
    )

    @dataclass
    class LaidOut:
        layout: StoreLayout

        async def call(self, request: Any, **_kwargs: Any) -> Any:
            raise AssertionError("no call on construction")

    watch = LivenessWatch.from_local(LaidOut(layout=layout))  # type: ignore[arg-type]  # layout only

    assert watch is not None
    assert watch.tracker.state_dir == layout.state_dir
    assert watch.tracker.store_dir == "/nix/store"
    assert watch.tracker.db_path == layout.state_dir / "db" / "db.sqlite"


@pytest.mark.parametrize("root_change", [False, True])
async def test_background_tick_uses_python_only(root_change: bool) -> None:
    tracker = StubTracker(live={A, B})
    watch = LivenessWatch(tracker)  # type: ignore[arg-type]  # stub tracker
    dirty = DirtyFlag()
    if root_change:
        dirty.set()
    with anyio.move_on_after(0.1):
        await Server._liveness_tick(None, watch, 0.01, dirty)  # type: ignore[arg-type]  # no server state
    assert tracker.refreshed > 0
