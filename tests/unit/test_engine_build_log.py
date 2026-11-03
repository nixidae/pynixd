"""The engine records a build log as Nix does (`LocalStore::addBuildLog`).

`addBuildLog` (`local-store.cc:1645`) writes the derivation's build log to
`<logDir>/drvs/<name[:2]>/<name[2:]>.bz2`, bzip2-compressed. A log already
present is kept. The write is a temporary file and a rename, so a reader
never sees a half-written log.
"""

from __future__ import annotations

import bz2
from typing import TYPE_CHECKING, Any

import pytest

from pynixd_store import build_log
from pynixd_store.engine import StoreEngine
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path

DRV = "zg01p02q4lq6g9p7v3s6xk0bq0zs0y8c-hello.drv"
LOG = b"building hello\nphase: build\ndone\n"


def test_the_path_is_the_two_char_split_under_drvs(tmp_path: Path) -> None:
    assert build_log.log_path(tmp_path, DRV) == tmp_path / "drvs" / DRV[:2] / f"{DRV[2:]}.bz2"


def test_a_log_is_written_bzip2_compressed(tmp_path: Path) -> None:
    build_log.add_build_log(tmp_path, DRV, LOG)
    written = build_log.log_path(tmp_path, DRV)
    assert bz2.decompress(written.read_bytes()) == LOG


def test_a_log_already_present_is_kept(tmp_path: Path) -> None:
    """Nix returns before it writes, so the first log stands."""
    build_log.add_build_log(tmp_path, DRV, LOG)
    build_log.add_build_log(tmp_path, DRV, b"a different log")
    assert bz2.decompress(build_log.log_path(tmp_path, DRV).read_bytes()) == LOG


def test_the_write_leaves_no_temporary_file(tmp_path: Path) -> None:
    build_log.add_build_log(tmp_path, DRV, LOG)
    temporaries = list((tmp_path / "drvs" / DRV[:2]).glob("*.tmp-*"))
    assert temporaries == []


def test_a_base_name_that_is_not_a_derivation_is_refused(tmp_path: Path) -> None:
    with pytest.raises(build_log.BuildLogError, match="not a derivation"):
        build_log.add_build_log(tmp_path, "zg01p02q4lq6g9p7v3s6xk0bq0zs0y8c-hello", LOG)


async def _no_config() -> dict[str, Any]:
    raise AssertionError("addBuildLog reads no Nix setting")


async def test_the_engine_records_a_build_log(tmp_path: Path) -> None:
    layout = StoreLayout.chroot(tmp_path)
    engine = StoreEngine(layout, writes=True, read_config=_no_config, on_change=lambda _: None, name="test")
    await engine.add_build_log(DRV, LOG)
    assert bz2.decompress(build_log.log_path(layout.log_dir, DRV).read_bytes()) == LOG
