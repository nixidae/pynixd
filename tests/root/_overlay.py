"""A `local-overlay://` store as Nix's `tests/functional/local-overlay-store/common.sh` makes one, on three arms.

`setupStoreDirs` (`common.sh:47`) puts a lower store, OverlayFS's upper and work directories and the merged
store directory on one tmpfs. Nix mounts nothing itself (`local-overlay-store.hh:52`), so `mountOverlayfs`
(`common.sh:65`) mounts. Nix's scripts run in a user namespace as its root (`execUnshare`); the `root` suite
is root already, so it needs neither `build-users-group =` nor `require-drop-supplementary-groups = false`.

An overlay store takes a root, so its arms are chroot stores at the merged directory, not `arm_store`'s
relocated ones. The scripts `unset NIX_STORE_DIR` for the same reason (`add-lower-inner.sh:10`).
"""

from __future__ import annotations

import contextlib
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from nix_daemon_protocol.ids import StoreId
from pynixd import Server
from pynixd.config import OverlaySpec
from pynixd.op_routes import OpRoute
from tests.conftest import make_test_spec, run_subproc
from tests.functional._nixft import Arm, ArmStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pynixd.nix_config import NixConfig

HERE = Path("/var/lib/pynixd-root-tests/overlay")
FEATURE = "local-overlay-store"


@dataclass(frozen=True)
class Stores:
    """`setupStoreDirs`' names: `storeA`, `storeBTop`, `storeBRoot` (`common.sh:55`)."""

    volume: Path
    lower: Path
    upper: Path
    merged: Path

    @property
    def work(self) -> Path:
        return self.volume / "workdir"

    def url(self, *, root: Path | None = None, lower: Path | None = None, upper: Path | None = None) -> str:
        """`storeB` (`common.sh:59`), with any of its three paths replaced."""
        return (
            f"local-overlay://?root={root or self.merged}&lower-store={lower or self.lower}"
            f"&upper-layer={upper or self.upper}"
        )


@contextlib.asynccontextmanager
async def overlay_volume() -> AsyncIterator[Stores]:
    """`setupStoreDirs` (`common.sh:47`): the four directories on one tmpfs, nothing mounted over them yet."""
    volume = HERE / "stores"
    volume.mkdir(parents=True, exist_ok=True)
    # A tmpfs is 1777 by default, and a chroot build refuses a world-writable store parent.
    await run_subproc(["mount", "-t", "tmpfs", "-o", "mode=0755", "tmpfs", volume])
    try:
        stores = Stores(volume, volume / "store-a", volume / "store-b", volume / "merged-store")
        for directory in (stores.lower / "nix/store", stores.upper, stores.merged / "nix/store", stores.work):
            directory.mkdir(parents=True)
        yield stores
    finally:
        await run_subproc(["umount", volume])


@contextlib.asynccontextmanager
async def mounted(stores: Stores, source: str = "overlay") -> AsyncIterator[None]:
    """`mountOverlayfs` (`common.sh:65`). *source* is the name `mount` records for it; `common.sh` passes `overlay`."""
    options = f"lowerdir={stores.lower / 'nix/store'},upperdir={stores.upper},workdir={stores.work}"
    await run_subproc(["mount", "-t", "overlay", source, "-o", options, stores.merged / "nix/store"])
    try:
        yield
    finally:
        await run_subproc(["umount", stores.merged / "nix/store"])


async def remount(stores: Stores) -> None:
    """`remountOverlayfs` (`common.sh:83`), with `LIBMOUNT_FORCE_MOUNT2=always` as `common.sh:28` sets it.

    The new mount interface refuses the remount: "No changes allowed in reconfigure".
    """
    await run_subproc(["mount", "-o", "remount", stores.merged / "nix/store"], env={"LIBMOUNT_FORCE_MOUNT2": "always"})


@contextlib.asynccontextmanager
async def overlay_arm(
    stores: Stores,
    arm: Arm,
    nix_config: NixConfig,
    socket_dir: Path,
    *,
    root: Path | None = None,
    lower: Path | None = None,
    upper: Path | None = None,
) -> AsyncIterator[ArmStore]:
    """*arm*'s store over *stores*, with any of the three paths replaced as `Stores.url` replaces them.

    The local arm's client opens the `local-overlay://` URL itself. The two pynixd arms serve a chroot store
    at the merged directory whose spec names the overlay, and the client reaches it over `unix://`.
    """
    nix_config = nix_config.model_copy(
        update={"experimental_features": [*(nix_config.experimental_features or []), FEATURE]}
    )
    if arm is Arm.local:
        yield ArmStore(stores.url(root=root, lower=lower, upper=upper), {}, nix_config)
        return
    merged = root or stores.merged
    socket = socket_dir / f"{arm}.sock"
    spec = make_test_spec(
        store_id="local",
        store_path=merged,
        no_probe=True,
        nix_config=nix_config,
        overlay=OverlaySpec(lower_store=lower or stores.lower, upper_layer=upper or stores.upper),
        daemon=arm is Arm.daemon,
        op_routes={"BuildDerivation": OpRoute.daemon if arm is Arm.daemon else OpRoute.pynixd},
    )
    async with Server(
        stores={StoreId("local"): spec.to_store("local")}, unix_path=socket, ssh_port=None, http_port=None
    ):
        yield ArmStore(f"unix://{urllib.parse.quote(str(socket))}?root={merged}", {}, {"substituters": ""})
