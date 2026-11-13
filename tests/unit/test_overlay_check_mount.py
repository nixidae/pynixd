"""`check-mount` (`local-overlay-store.cc:49`): pynixd reads `/proc/self/mounts` as Nix does, field by field.

`tests/root/test_nixft_local_overlay.py` holds the same check to a real mount (`bad-uris.sh:28`).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pynixd_store.overlay_engine import mounted_correctly
from pynixd_store.store_layout import Overlay, StoreLayout

OVERLAY = Overlay(lower=StoreLayout.chroot(Path("/s/store-a")), upper_layer=Path("/s/store-b"))
MERGED = Path("/s/merged-store/nix/store")
GOOD = "overlay /s/merged-store/nix/store overlay rw,relatime,lowerdir=/s/store-a/nix/store,upperdir=/s/store-b,workdir=/s/workdir 0 0"


@pytest.mark.parametrize(
    ("mounts", "expected"),
    [
        (GOOD, True),
        ("tmpfs /s tmpfs rw 0 0", False),
        (GOOD.replace("upperdir=/s/store-b", "upperdir=/s/other"), False),
        (GOOD.replace("lowerdir=/s/store-a/nix/store", "lowerdir=/s/store-a/nix/store:/s/more"), False),
        # The source is the caller's name for the mount; the type is the third field.
        (GOOD.replace("overlay /s", "elsewhere /s", 1), True),
        (GOOD.replace(" overlay rw", " tmpfs rw"), False),
        # Mounts stack: the last one at the directory is in effect.
        (f"{GOOD}\ntmpfs /s/merged-store/nix/store tmpfs rw 0 0", False),
        (f"{GOOD}\n{GOOD.replace('upperdir=/s/store-b', 'upperdir=/s/other')}", False),
    ],
    ids=[
        "good",
        "no-overlay",
        "other-upper",
        "two-lowers",
        "other-source",
        "other-type",
        "stacked-tmpfs",
        "stacked-overlay",
    ],
)
def test_the_mount_in_effect_names_both_layers(mounts: str, expected: bool) -> None:
    assert mounted_correctly(mounts, OVERLAY, MERGED) is expected


def test_the_kernel_escapes_a_space() -> None:
    overlay = Overlay(lower=StoreLayout.chroot(Path("/s/a b")), upper_layer=Path("/s/store-b"))
    line = r"overlay /s/m\040x/nix/store overlay rw,lowerdir=/s/a\040b/nix/store,upperdir=/s/store-b 0 0"
    assert mounted_correctly(line, overlay, Path("/s/m x/nix/store"))
