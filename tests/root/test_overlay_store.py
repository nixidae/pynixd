"""A `local-overlay://` store, set up as Nix's `tests/functional/local-overlay-store/common.sh` sets one up.

A tmpfs holds a lower store, OverlayFS's upper and work directories, and the
merged store directory. Nix mounts nothing itself (`local-overlay-store.hh:52`),
so the fixture mounts, and the store spec's `overlay` names the parts. The
guest's `root` suite runs these as root; anywhere else they skip.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from prometheus_client import REGISTRY

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import OverlaySpec
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, make_test_spec, run_subproc

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

HERE = Path("/var/lib/pynixd-root-tests/overlay")
NIX_STORE_BIN = NIX_BIN.with_name("nix-store")

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="an OverlayFS mount needs root")


@dataclass(frozen=True)
class Stores:
    lower: Path
    upper: Path
    merged: Path


async def _nix(*args: object) -> str:
    _rc, out, _err, _both = await run_subproc([NIX_BIN, "--extra-experimental-features", "nix-command", *args])
    return out.strip()


@pytest.fixture
async def stores() -> AsyncIterator[Stores]:
    volume = HERE / "volume"
    volume.mkdir(parents=True, exist_ok=True)
    # One file system for lowerdir, upperdir and workdir, as `setupStoreDirs` makes one.
    await run_subproc(["mount", "-t", "tmpfs", "tmpfs", volume])
    try:
        found = Stores(lower=volume / "lower", upper=volume / "upper", merged=volume / "merged")
        for directory in (found.lower / "nix" / "store", found.upper, found.merged / "nix" / "store", volume / "work"):
            directory.mkdir(parents=True)
        await run_subproc(
            [
                "mount",
                "-t",
                "overlay",
                "overlay",
                "-o",
                f"lowerdir={found.lower / 'nix' / 'store'},upperdir={found.upper},workdir={volume / 'work'}",
                found.merged / "nix" / "store",
            ]
        )
        try:
            yield found
        finally:
            await run_subproc(["umount", found.merged / "nix" / "store"])
    finally:
        await run_subproc(["umount", volume])


@contextlib.asynccontextmanager
async def _serve(tmp_path: Path, stores: Stores, **spec: Any) -> AsyncIterator[str]:
    socket = tmp_path / "overlay.sock"
    store_spec = make_test_spec(
        store_id="local",
        store_path=stores.merged,
        no_probe=True,
        nix_config=for_test_store(substituters=()),
        overlay=OverlaySpec(lower_store=stores.lower, upper_layer=stores.upper),
        # Each deletes through the upper layer on an overlay store, which only the daemon does.
        op_routes=dict.fromkeys(("CollectGarbage", "OptimiseStore", "VerifyStore", "BuildDerivation"), "daemon"),
        **spec,
    )
    async with Server(
        stores={StoreId("local"): store_spec.to_store("local")}, unix_path=socket, ssh_port=None, http_port=None
    ):
        yield f"unix://{socket}?root={stores.merged}"


async def _lower_closure(stores: Stores) -> tuple[str, str]:
    """A path in the lower store and the path it refers to: `builtins.toFile` takes the context as references."""
    inner = 'builtins.toFile "inner" "lower\\n"'
    outer = await _nix("eval", "--raw", "--store", stores.lower, "--expr", f'builtins.toFile "outer" "${{{inner}}}"')
    inner_path = await _nix("eval", "--raw", "--store", stores.lower, "--expr", inner)
    return outer, inner_path


def _upper_rows(stores: Stores) -> set[tuple[str, str | None]]:
    """Each path of the upper database, with each reference it has."""
    db = stores.merged / "nix" / "var" / "nix" / "db" / "db.sqlite"
    with contextlib.closing(sqlite3.connect(f"file:{db}?mode=ro", uri=True)) as conn:
        rows = conn.execute(
            "SELECT p.path, r.path FROM ValidPaths p "
            "LEFT JOIN Refs ON Refs.referrer = p.id LEFT JOIN ValidPaths r ON r.id = Refs.reference"
        ).fetchall()
    return {(path, reference) for path, reference in rows}


def _fallthroughs() -> float:
    return REGISTRY.get_sample_value("pynixd_op_fallthrough_total", {"store_id": "local", "op": "IsValidPath"}) or 0.0


async def test_a_lower_path_is_valid_through_the_overlay(tmp_path: Path, stores: Stores) -> None:
    """pynixd reads the lower store's database: `queryPathInfoUncached` (`local-overlay-store.cc:93`)."""
    content = tmp_path / "in-lower"
    content.write_text("lower\n")
    path = await _nix("store", "add-file", "--store", stores.lower, content)

    async with _serve(tmp_path, stores) as url:
        assert await _nix("path-info", "--store", url, path) == path
        assert await _nix("store", "cat", "--store", url, f"{path}") == "lower"


@pytest.mark.parametrize(
    ("route", "spec"),
    [
        ("pynixd", {"pynixd_writes": True}),
        ("fallthrough", {"pynixd_writes": False}),
        ("daemon", {"use_db": False}),
    ],
)
async def test_is_valid_path_copies_the_lower_closure_up(
    tmp_path: Path, stores: Stores, route: str, spec: dict[str, Any]
) -> None:
    """`isValidPathUncached` (`local-overlay-store.cc:157`) registers the lower path and its closure in the upper database.

    Every route leaves the same rows. Without `pynixd_writes` pynixd leaves
    the sync, and so the answer, to the daemon.
    """
    outer, inner = await _lower_closure(stores)
    before = _fallthroughs()

    async with _serve(tmp_path, stores, **spec) as url:
        rc, out, err, _both = await run_subproc(
            [NIX_STORE_BIN, "--store", url, "--check-validity", "--print-invalid", outer]
        )
        assert (rc, out.strip()) == (0, ""), err
        referrers = await _nix("path-info", "--store", url, "--recursive", outer)

    assert sorted(referrers.split()) == sorted([outer, inner])
    assert _upper_rows(stores) == {(outer, inner), (inner, None)}
    assert (_fallthroughs() > before) == (route == "fallthrough")
