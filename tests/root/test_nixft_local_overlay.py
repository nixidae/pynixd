"""Nix's `tests/functional/local-overlay-store/*.sh`, which skip on `needLocalStore`, held to three arms (`_overlay.py`).

`storeA` is the lower store, a plain chroot store that every arm's client opens itself. `storeB` is the
overlay store: the client's own on the local arm, pynixd's on the other two.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import anyio
import pytest

from pynixd_store.errors import PynixdError
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, ArmStore, copy_nix_tests
from tests.functional._unshared_groups import busybox
from tests.root._overlay import Stores, mounted, overlay_arm, overlay_volume, remount

if TYPE_CHECKING:
    from pynixd.nix_config import NixConfig

pytestmark = pytest.mark.skipif(os.geteuid() != 0, reason="an OverlayFS mount needs root")

MOUNTED_INCORRECTLY = re.compile(r"overlay filesystem .* mounted incorrectly")


def _config() -> NixConfig:
    return for_test_store(substituters=())


async def _run(
    url: str | Path, config: NixConfig | dict[str, str], program: str, *args: str | Path
) -> tuple[int, str, str]:
    with anyio.fail_after(120):
        rc, out, err, _both = await run_subproc(
            [NIX_BIN.with_name(program), "--store", url, *args], expected_retcode=None, nix_config=config
        )
    return rc, out.strip(), err


async def _ok(url: str | Path, config: NixConfig | dict[str, str], program: str, *args: str | Path, line: str) -> str:
    rc, out, err = await _run(url, config, program, *args)
    assert rc == 0, f"{line}: {err}"
    return out


async def _in_b(store: ArmStore, program: str, *args: str | Path, line: str) -> str:
    return await _ok(store.url, store.config, program, *args, line=line)


async def _in_a(stores: Stores, program: str, *args: str | Path, line: str) -> str:
    return await _ok(stores.lower, _config(), program, *args, line=line)


def _refusal(exc: BaseException) -> str | None:
    """The text of the `PynixdError` that ended *exc*, through any exception groups around it."""
    if isinstance(exc, BaseExceptionGroup):
        return next((text for inner in exc.exceptions if (text := _refusal(inner)) is not None), None)
    return str(exc) if isinstance(exc, PynixdError) else None


async def _doctor(stores: Stores, arm: Arm, tmp_path: Path, **paths: Path) -> tuple[int, str]:
    """`nix doctor --store storeB` (`bad-uris.sh:28`). pynixd opens the store when it starts, as Nix's client opens it."""
    try:
        async with overlay_arm(stores, arm, _config(), tmp_path, **paths) as store:
            rc, _out, err = await _run(
                store.url, store.config, "nix", "--extra-experimental-features", "nix-command", "doctor"
            )
            return rc, err
    except BaseException as exc:
        if (text := _refusal(exc)) is None:
            raise
        return 1, text


@pytest.mark.parametrize("bad", ["root", "lower", "upper"])
@pytest.mark.parametrize("arm", list(Arm))
async def test_an_overlay_mounted_elsewhere_is_refused(tmp_path: Path, arm: Arm, bad: str) -> None:
    """`bad-uris.sh:21` to `:30`: a store whose root, lower store or upper layer the mount does not name."""
    bad_root = tmp_path / "bad_test"
    bad_root.mkdir()
    async with overlay_volume() as stores, mounted(stores):
        rc, err = await _doctor(stores, arm, tmp_path, **{bad: bad_root})
    assert (rc, MOUNTED_INCORRECTLY.search(err) is not None) == (1, True), err


@pytest.mark.parametrize(
    ("arm", "refused"),
    [
        (Arm.local, True),
        pytest.param(Arm.daemon, True, marks=pytest.mark.skip(reason="nix-daemon checks the mount as Nix does")),
        (Arm.pynixd, False),
    ],
)
async def test_the_mount_check_reads_the_type_not_the_source(tmp_path: Path, arm: Arm, refused: bool) -> None:
    """The NIX-DEFECT in `mounted_correctly`: Nix refuses an overlay that `mount` names other than `overlay`."""
    async with overlay_volume() as stores, mounted(stores, source="elsewhere"):
        rc, err = await _doctor(stores, arm, tmp_path)
    assert (rc, MOUNTED_INCORRECTLY.search(err) is not None) == ((1, True) if refused else (0, False)), err


@dataclass(frozen=True)
class Lower:
    """What `initLowerStore` (`common.sh:94`) leaves: `drvPath` and `pathInLowerStore`."""

    drv: str
    out: str


async def _init_lower(stores: Stores, src: Path) -> Lower:
    """`initLowerStore` (`common.sh:94`): `dummy`, and a build of `hermetic.nix`, in the lower store."""
    await _in_a(stores, "nix-store", "--add", src / "dummy", line="common.sh:96")
    drv = await _in_a(
        stores, "nix-instantiate", src / "hermetic.nix", "--arg", "withFinalRefs", "true",
        "--arg", "busybox", busybox(), "--arg", "seed", "1", line="common.sh:99",
    )  # fmt: skip
    return Lower(drv, await _in_a(stores, "nix-store", "--realise", drv, line="common.sh:101"))


def _sources(tmp_path: Path) -> Path:
    src = tmp_path / "src"
    src.mkdir()
    copy_nix_tests(src, "hermetic.nix", "dummy")
    return src


def _real(directory: Path, path: str) -> Path:
    """`toRealPath` (`common.sh:87`): where *directory* holds the files of *path*."""
    return directory / Path(path).name


async def _add_text(
    url: str | Path, config: NixConfig | dict[str, str], home: Path, name: str, text: str, line: str
) -> str:
    """`addTextToStore` (`common.sh:104`)."""
    (home / name).write_text(f"{text}\n")
    return await _ok(url, config, "nix-store", "--add", home / name, line=line)


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_path_added_below_shows_through_after_a_remount(tmp_path: Path, arm: Arm) -> None:
    """`add-lower-inner.sh:20` to `:31`: a path added to each store is in the merged directory."""
    src = _sources(tmp_path)
    async with overlay_volume() as stores:
        await _init_lower(stores, src)
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path) as store:
            overlay_path = await _add_text(
                store.url, store.config, tmp_path, "overlay-file", "Add to overlay store", "add-lower-inner.sh:20"
            )
            assert _real(stores.merged / "nix/store", overlay_path).exists(), "add-lower-inner.sh:21"
            lower_path = await _add_text(
                stores.lower, _config(), tmp_path, "lower-file", "Add to lower store", "add-lower-inner.sh:24"
            )
            assert _real(stores.lower / "nix/store", lower_path).exists(), "add-lower-inner.sh:25"
            await remount(stores)
            assert _real(stores.merged / "nix/store", lower_path).exists(), "add-lower-inner.sh:31"


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_build_lands_in_the_upper_layer(tmp_path: Path, arm: Arm) -> None:
    """`build-inner.sh:21` to `:30`: a build in the overlay store writes the upper layer only."""
    src = _sources(tmp_path)
    async with overlay_volume() as stores:
        await _init_lower(stores, src)
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path) as store:
            path = await _in_b(
                store, "nix-build", src / "hermetic.nix", "--arg", "busybox", busybox(), "--arg", "seed", "2",
                "--no-out-link", line="build-inner.sh:21",
            )  # fmt: skip
            assert not _real(stores.lower / "nix/store", path).exists(), "build-inner.sh:24"
            assert _real(stores.upper, path).exists(), "build-inner.sh:27"
            await _in_b(store, "nix-store", "--verify-path", path, line="build-inner.sh:30")


async def _diff(a: Path, b: Path) -> tuple[int, str]:
    rc, out, _err, _both = await run_subproc(["diff", "-r", a, b], expected_retcode=None)
    return rc, out


@pytest.mark.parametrize("arm", list(Arm))
async def test_the_overlay_answers_as_its_lower_store(tmp_path: Path, arm: Arm) -> None:
    """`check-post-init-inner.sh:22` to `:71`: each query of a lower path answers as the lower store answers it."""
    src = _sources(tmp_path)
    async with overlay_volume() as stores:
        lower = await _init_lower(stores, src)
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path) as store:
            assert _real(stores.lower / "nix/store", lower.out).exists(), "check-post-init-inner.sh:22"
            assert not _real(stores.upper, lower.out).exists(), "check-post-init-inner.sh:25"
            merged = _real(stores.merged / "nix/store", lower.out)
            assert await _diff(_real(stores.lower / "nix/store", lower.out), merged) == (0, ""), (
                "check-post-init-inner.sh:28"
            )

            busybox_path = await _in_a(stores, "nix", "--extra-experimental-features", "nix-command", "store", "add-path",
                                       busybox(), line="check-post-init-inner.sh:38")  # fmt: skip
            for flag, path, line in (
                ("--requisites", lower.drv, 31),
                ("--referrers", busybox_path, 39),
                ("--deriver", lower.out, 46),
                ("--outputs", lower.drv, 53),
            ):
                where = f"check-post-init-inner.sh:{line}"
                expected = await _in_a(stores, "nix-store", "--query", flag, path, line=where)
                assert await _in_b(store, "nix-store", "--query", flag, path, line=where) == expected, where

            await _in_a(stores, "nix-store", "--verify-path", lower.out, line="check-post-init-inner.sh:60")
            await _in_b(store, "nix-store", "--verify-path", lower.out, line="check-post-init-inner.sh:63")
            hash_part = Path(lower.out).name.split("-", 1)[0]
            for line, found in (
                (68, await _in_a(stores, "nix", "--extra-experimental-features", "nix-command", "store",
                                 "path-from-hash-part", hash_part, line="check-post-init-inner.sh:68")),
                (71, await _in_b(store, "nix", "--extra-experimental-features", "nix-command", "store",
                                 "path-from-hash-part", hash_part, line="check-post-init-inner.sh:71")),
            ):  # fmt: skip
                assert found == lower.out, f"check-post-init-inner.sh:{line}"
