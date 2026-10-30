"""`pynixd roots` prints the hog table: exclusive bytes first, sliced by `--top`.

The daemon attributes; the command sorts and slices. Three rows out of
order go in, the biggest exclusive hog prints first, and `--top 1`
leaves the other two off.
"""

from __future__ import annotations

import argparse
import contextlib
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from pynixd.cli import roots as roots_cli
from pynixd.daemon_extensions.pynixd_roots_report import (
    PynixdRootsReportRequest,
    PynixdRootsReportResponse,
    RootsReportRow,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

ROWS = [
    RootsReportRow(label="small", full_paths=2, full_bytes=1500, exclusive_paths=1, exclusive_bytes=1000),
    RootsReportRow(
        label="big", full_paths=5, full_bytes=3_221_225_472, exclusive_paths=4, exclusive_bytes=2_147_483_648
    ),
    RootsReportRow(
        label="mid", full_paths=3, full_bytes=2_147_483_648, exclusive_paths=2, exclusive_bytes=1_610_612_736
    ),
]


class _FakeStore:
    """A daemon with three attributed roots."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.request: Any = None

    async def start(self, **kwargs: Any) -> None:
        return None

    async def execute(self, request: Any) -> PynixdRootsReportResponse:
        self.request = request
        return PynixdRootsReportResponse(rows=list(ROWS))

    async def close(self) -> None:
        return None


def _fake_daemon(monkeypatch: pytest.MonkeyPatch, store: _FakeStore) -> None:
    @contextlib.asynccontextmanager
    async def open_pynixd(*args: Any) -> AsyncIterator[_FakeStore]:
        yield store

    monkeypatch.setattr(roots_cli, "open_pynixd", open_pynixd)
    monkeypatch.setattr(roots_cli, "load_settings", lambda: SimpleNamespace(unix_path="/sock"))
    monkeypatch.setattr(roots_cli, "setup_logging", lambda settings: None)


def _args(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> argparse.Namespace:
    """`_roots_main` with the daemon and the settings faked out."""
    _fake_daemon(monkeypatch, _FakeStore())
    fields: dict[str, Any] = {"top": 20, "socket": None}
    fields.update(kwargs)
    return argparse.Namespace(**fields)


@pytest.mark.anyio
async def test_roots_print_biggest_exclusive_first(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Bytes read human, rows sorted by exclusive, request unparameterized."""
    store = _FakeStore()
    _fake_daemon(monkeypatch, store)
    await roots_cli._roots_main(argparse.Namespace(top=20, socket=None))

    assert isinstance(store.request, PynixdRootsReportRequest)
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split() == ["LABEL", "PATHS", "FULL", "EXCLUSIVE", "EXCL_PATHS"]
    assert lines[1].split() == ["big", "5", "3G", "2G", "4"]
    assert lines[2].split() == ["mid", "3", "2G", "1.5G", "2"]
    assert lines[3].split() == ["small", "2", "1.5K", "1000B", "1"]
    assert len(lines) == 4


@pytest.mark.anyio
async def test_top_slices_after_sorting(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """The gate is display: the computation attributes every root either way."""
    await roots_cli._roots_main(_args(monkeypatch, top=1))

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 2
    assert lines[1].split()[0] == "big"


def test_top_refuses_zero() -> None:
    """A count that shows nothing is a mistake, not a report."""
    with pytest.raises(SystemExit):
        roots_cli.roots_main(argparse.Namespace(top=0))
