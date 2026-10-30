"""The `gc` command lists what it plans when asked, and stays quiet otherwise.

A dry-run that names 22,000 paths is evidence only when the paths can be
saved and compared: the count and the bytes say how much, `--show-paths`
says what. The daemon answers the set; the flag only prints it.
"""

from __future__ import annotations

import argparse
import contextlib
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from pynixd.cli import gc as gc_cli
from pynixd.daemon_extensions.pynixd_collect_garbage import PynixdCollectGarbageResponse
from pynixd.serde import StorePath

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

PATH_B = "/nix/store/11111111111111111111111111111111-b"
PATH_C = "/nix/store/22222222222222222222222222222222-c"


class _FakeStore:
    """A daemon that planned two paths, and nothing else."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.request: Any = None

    async def start(self, **kwargs: Any) -> None:
        return None

    async def execute(self, request: Any) -> PynixdCollectGarbageResponse:
        self.request = request
        return PynixdCollectGarbageResponse(
            store_paths={StorePath(PATH_C), StorePath(PATH_B)},
            bytes=30,
        )

    async def close(self) -> None:
        return None


def _args(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> argparse.Namespace:
    """`_gc_main` with the daemon and the settings faked out."""

    @contextlib.asynccontextmanager
    async def open_pynixd(*args: Any) -> AsyncIterator[_FakeStore]:
        yield _FakeStore()

    monkeypatch.setattr(gc_cli, "open_pynixd", open_pynixd)
    monkeypatch.setattr(gc_cli, "load_settings", lambda: SimpleNamespace(unix_path="/sock"))
    monkeypatch.setattr(gc_cli, "setup_logging", lambda settings: None)
    fields = {"store": None, "execute": False, "limit": None, "show_paths": False, "socket": None}
    fields.update(kwargs)
    return argparse.Namespace(**fields)


@pytest.mark.anyio
async def test_show_paths_lists_every_planned_path_sorted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The set, sorted: a cohort that can be saved and diffed later."""
    await gc_cli._gc_main(_args(monkeypatch, show_paths=True))

    out = capsys.readouterr().out.splitlines()
    assert out[0] == PATH_B
    assert out[1] == PATH_C
    assert out[2] == "dry-run: 2 paths, up to 30 bytes"


@pytest.mark.anyio
async def test_paths_stay_quiet_by_default(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """Count and bytes only: the flag is what names names."""
    await gc_cli._gc_main(_args(monkeypatch))

    assert capsys.readouterr().out.splitlines() == ["dry-run: 2 paths, up to 30 bytes"]
