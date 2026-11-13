"""Nix's `tests/functional/local-overlay-store/*.sh`, which skip on `needLocalStore`, held to three arms (`_overlay.py`).

`storeA` is the lower store, a plain chroot store that every arm's client opens itself. `storeB` is the
overlay store: the client's own on the local arm, pynixd's on the other two.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

import anyio
import pytest

from pynixd.nix_config import NixConfig
from pynixd_store.errors import PynixdError
from tests._conftest.nix_config import for_test_store
from tests.conftest import NIX_BIN, run_subproc
from tests.functional._nixft import Arm, ArmStore, copy_nix_tests
from tests.functional._unshared_groups import busybox
from tests.root._overlay import Stores, mounted, overlay_arm, overlay_volume, remount

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


@pytest.mark.parametrize("arm", list(Arm))
async def test_an_add_of_a_lower_path_copies_nothing_up(tmp_path: Path, arm: Arm) -> None:
    """`redundant-add-inner.sh:22` to `:35`: the overlay store adds a path its lower store holds, and copies nothing.

    `:25` stats `$storeBTop/nix/store/...`, a directory the upper layer never has, so it passes whatever the store
    did. The port checks the upper layer itself, as `:35` does.
    """
    src = _sources(tmp_path)
    async with overlay_volume() as stores:
        await _init_lower(stores, src)
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path) as store:
            lower_path = await _in_a(stores, "nix-store", "--add", src / "dummy", line="redundant-add-inner.sh:22")
            assert not _real(stores.upper, lower_path).exists(), "redundant-add-inner.sh:25"
            from_b = await _in_b(store, "nix-store", "--add", src / "dummy", line="redundant-add-inner.sh:27")
            assert from_b == lower_path, "redundant-add-inner.sh:29"
            assert _real(stores.lower / "nix/store", lower_path).exists(), "redundant-add-inner.sh:32"
            assert not _real(stores.upper, lower_path).exists(), "redundant-add-inner.sh:35"


def _seed_2(src: Path, *attr: str) -> list[str | Path]:
    """`hermetic.nix` with seed 2 and its final references (`delete-refs-inner.sh:20`), or one of its inputs."""
    return [
        src / "hermetic.nix", "--no-out-link", "--arg", "busybox", busybox(), "--arg", "withFinalRefs", "true",
        "--arg", "seed", "2", *attr,
    ]  # fmt: skip


async def _refused_delete(url: str | Path, config: NixConfig | dict[str, str], path: str, line: int) -> None:
    rc, _out, err = await _run(url, config, "nix-store", "--delete", path)
    assert (rc, "Cannot delete path" in err) == (1, True), f"delete-refs-inner.sh:{line}: {err}"


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_path_that_a_path_refers_to_is_not_deleted(tmp_path: Path, arm: Arm) -> None:
    """`delete-refs-inner.sh:20` to `:40`: a referenced path stays, in either store, until its referrer goes."""
    src = _sources(tmp_path)
    async with overlay_volume() as stores:
        await _init_lower(stores, src)
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path) as store:
            hermetic = await _in_b(store, "nix-build", *_seed_2(src), line="delete-refs-inner.sh:20")
            inputs = [
                await _in_b(store, "nix-build", *_seed_2(src, "-A", f"passthru.input{n}", "-j0"),
                            line=f"delete-refs-inner.sh:{20 + n}")
                for n in (1, 2, 3)
            ]  # fmt: skip
            for line, path in zip((26, 27, 28), inputs, strict=True):
                await _refused_delete(store.url, store.config, path, line)
            for line, path in ((32, inputs[1]), (33, inputs[2])):
                await _refused_delete(stores.lower, _config(), path, line)
            await _in_b(store, "nix-store", "--delete", hermetic, line="delete-refs-inner.sh:36")
            await _in_b(store, "nix-store", "--delete", inputs[2], line="delete-refs-inner.sh:39")
            await _in_b(store, "nix-store", "--delete", inputs[1], line="delete-refs-inner.sh:40")


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_delete_of_a_duplicate_uncovers_the_lower_copy(
    tmp_path: Path, arm: Arm, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`delete-duplicate-inner.sh:20` to `:38`: a path in both layers goes from the upper one, and the hook remounts."""
    # `common.sh:28` exports it, so Nix's `remount.sh` runs with it, from the client or from pynixd.
    monkeypatch.setenv("LIBMOUNT_FORCE_MOUNT2", "always")
    src = _sources(tmp_path)
    (src / "local-overlay-store").mkdir()
    copy_nix_tests(src, "local-overlay-store/delete-duplicate.sh", "local-overlay-store/remount.sh")
    added = src / "local-overlay-store/delete-duplicate.sh"
    async with overlay_volume() as stores:
        await _init_lower(stores, src)
        hook = src / "local-overlay-store/remount.sh"
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path, remount_hook=hook) as store:
            upper_path = await _in_b(store, "nix-store", "--add", added, line="delete-duplicate-inner.sh:20")
            lower_path = await _in_a(stores, "nix-store", "--add", added, line="delete-duplicate-inner.sh:21")
            assert upper_path == lower_path, "delete-duplicate-inner.sh:22"
            merged, lower = (
                _real(stores.merged / "nix/store", upper_path),
                _real(stores.lower / "nix/store", lower_path),
            )
            assert merged.stat().st_ino != lower.stat().st_ino, "delete-duplicate-inner.sh:27"
            await _in_b(store, "nix-store", "--delete", upper_path, line="delete-duplicate-inner.sh:30")
            assert not _real(stores.upper, upper_path).exists(), "delete-duplicate-inner.sh:33"
            assert merged.stat().st_ino == lower.stat().st_ino, "delete-duplicate-inner.sh:38"


@pytest.mark.parametrize("arm", list(Arm))
async def test_the_collector_keeps_a_rooted_path_and_empties_the_upper_layer(tmp_path: Path, arm: Arm) -> None:
    """`gc-inner.sh:19` to `:58`: a rooted build survives two collections, and goes with its root.

    `:54` tests `$outPath`, the store path on the host, which the merged store never put there; the port tests
    the merged directory, as `:47` reads it.
    """
    src = _sources(tmp_path)
    async with overlay_volume() as stores:
        await _init_lower(stores, src)
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path) as store:
            out_path = await _in_b(
                store, "nix-build", src / "hermetic.nix", "--no-out-link", "--arg", "busybox", busybox(),
                "--arg", "seed", "2", line="gc-inner.sh:19",
            )  # fmt: skip
            roots = stores.merged / "nix/var/nix/gcroots"
            roots.mkdir(parents=True, exist_ok=True)
            (roots / "foo").symlink_to(out_path)
            found = await _in_b(store, "nix-store", "-q", "--roots", out_path, line="gc-inner.sh:26")
            assert found == f"{roots / 'foo'} -> {out_path}", "gc-inner.sh:26"
            for flag, line in (("--print-roots", 28), ("--print-live", 29)):
                assert out_path in await _in_b(store, "nix-store", "--gc", flag, line=f"gc-inner.sh:{line}"), line
            dead = await _in_b(store, "nix-store", "--gc", "--print-dead", line="gc-inner.sh:30")
            assert not any(entry.endswith(out_path) for entry in dead.splitlines()), "gc-inner.sh:30"
            rc, _out, err = await _run(store.url, store.config, "nix-store", "--delete", out_path)
            assert rc == 1, f"gc-inner.sh:34: {err}"
            merged = _real(stores.merged / "nix/store", out_path)
            assert merged.exists(), "gc-inner.sh:35"
            for entry in stores.merged.iterdir():
                (entry.parent / f"{entry.name}.lock").touch()
                (entry.parent / f"{entry.name}.chroot").touch()
            await _in_b(store, "nix-collect-garbage", line="gc-inner.sh:44")
            assert merged.exists(), "gc-inner.sh:47"
            (roots / "foo").unlink()
            await _in_b(store, "nix-collect-garbage", line="gc-inner.sh:51")
            assert not merged.exists(), "gc-inner.sh:54"
            assert list(stores.upper.iterdir()) == [], "gc-inner.sh:58"


@pytest.mark.parametrize("arm", list(Arm))
async def test_optimise_drops_the_upper_copy_of_a_lower_path(tmp_path: Path, arm: Arm) -> None:
    """`optimise-inner.sh:20` to `:51`: a path in both layers keeps only its lower copy."""
    src = _sources(tmp_path)
    duplicate = tmp_path / "dup-file"
    duplicate.write_text("Duplicate\n")
    async with overlay_volume() as stores:
        await _init_lower(stores, src)
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path) as store:
            path = await _in_b(store, "nix-store", "--add", duplicate, line="optimise-inner.sh:24")
            await _in_a(stores, "nix-store", "--add", duplicate, line="optimise-inner.sh:27")
            await remount(stores)
            lower, upper = _real(stores.lower / "nix/store", path), _real(stores.upper, path)
            merged = _real(stores.merged / "nix/store", path)
            assert upper.stat().st_ino == merged.stat().st_ino, "optimise-inner.sh:41"
            assert upper.stat().st_ino != lower.stat().st_ino, "optimise-inner.sh:42"
            await _in_b(store, "nix-store", "--optimise", line="optimise-inner.sh:45")
            await remount(stores)
            assert lower.exists(), "optimise-inner.sh:49"
            assert merged.exists(), "optimise-inner.sh:50"
            assert not upper.exists(), "optimise-inner.sh:51"


async def _verify(tmp_path: Path, arm: Arm, *, repair: bool) -> None:
    """`verify-inner.sh:23` to `:63`, and with *repair* to `:69`."""
    src = _sources(tmp_path)
    async with overlay_volume() as stores:
        lower = await _init_lower(stores, src)
        async with mounted(stores), overlay_arm(stores, arm, _config(), tmp_path) as store:
            await _in_b(store, "nix-store", "--realise", lower.drv, line="verify-inner.sh:23")
            dummy = await _in_b(store, "nix-store", "--add", src / "dummy", line="verify-inner.sh:26")
            lower_only = await _add_text(
                stores.lower, _config(), tmp_path, "lower-only", "Only in lower store", "verify-inner.sh:29"
            )
            await _in_b(store, "nix-store", "--verify", "--check-contents", line="verify-inner.sh:32")
            backup = stores.volume / "backup"
            backup.mkdir()
            rc, _out, err, _both = await run_subproc(
                ["cp", "-ar", stores.merged / "nix", backup], expected_retcode=None
            )
            assert rc == 0, f"verify-inner.sh:37: {err}"

            input_drv = next((stores.lower / "nix/store").glob("*-hermetic-input-1.drv"))
            input_drv.unlink()
            for corrupt in (
                next((stores.lower / "nix/store").glob("*-dummy")),
                _real(stores.lower / "nix/store", lower_only),
            ):
                corrupt.write_bytes(b"")
            await remount(stores)

            rc, _out, err = await _run(store.url, store.config, "nix-store", "--verify", "--check-contents")
            assert rc == 1, f"verify-inner.sh:60: {err}"
            missing = f"/nix/store/{input_drv.name}"
            assert f"path '{missing}' disappeared, but it still has valid referrers!" in err, (
                f"verify-inner.sh:61: {err}"
            )
            assert f"path '{dummy}' was modified! expected hash" in err, f"verify-inner.sh:62: {err}"
            assert lower_only not in err, f"verify-inner.sh:63: {err}"
            if not repair:
                return

            config = (
                store.config.model_copy(update={"substituters": [str(backup)]})
                if isinstance(store.config, NixConfig)
                else {**store.config, "substituters": str(backup)}
            )
            _rc, out, err = await _run(store.url, config, "nix-store", "--verify", "--check-contents", "--repair")
            assert f"copying path '{missing}'" in out + err, f"verify-inner.sh:68: {err}"
            assert f"copying path '{dummy}'" in out + err, f"verify-inner.sh:69: {err}"


@pytest.mark.parametrize("arm", list(Arm))
async def test_verify_finds_the_corrupted_paths_the_upper_database_holds(tmp_path: Path, arm: Arm) -> None:
    """`verify-inner.sh:23` to `:63`: verify reads the merged directory, and skips a path only the lower store holds."""
    await _verify(tmp_path, arm, repair=False)


NO_LOCAL_SUBSTITUTER = pytest.mark.xfail(
    strict=True, reason="pynixd opens no local store as a substituter: nixidae/nixidae#1312"
)


@pytest.mark.parametrize(
    "arm",
    [
        Arm.local,
        Arm.daemon,
        pytest.param(Arm.pynixd, marks=NO_LOCAL_SUBSTITUTER),
    ],
)
async def test_verify_repairs_from_a_local_store_substituter(tmp_path: Path, arm: Arm) -> None:
    """`verify-inner.sh:66` to `:69`: `--repair` copies the corrupted paths back from a backup store named by its path."""
    await _verify(tmp_path, arm, repair=True)


async def _trigger_stale_file_handle(stores: Stores, store: ArmStore, src: Path) -> tuple[int, str] | None:
    """`triggerStaleFileHandle` (`stale-file-handle-inner.sh:23`): the first step that fails, or `None`."""
    build: list[str | Path] = [
        "nix-build",
        src / "hermetic.nix",
        "--arg",
        "busybox",
        busybox(),
        "--arg",
        "seed",
        "1",
        "--no-out-link",
    ]
    steps: list[tuple[str | Path, NixConfig | dict[str, str], list[str | Path]]] = [
        (stores.lower, _config(), ["nix-store", "--gc"]),
        (store.url, store.config, build),
        (stores.lower, _config(), build),
        (store.url, store.config, ["nix-store", "--gc"]),
        (stores.lower, _config(), ["nix-store", "--gc"]),
        (store.url, store.config, build),
    ]
    for url, config, (program, *args) in steps:
        rc, _out, err = await _run(url, config, str(program), *args)
        if rc != 0:
            return rc, err
    return None


@pytest.mark.parametrize("arm", list(Arm))
async def test_a_remount_hook_keeps_the_store_usable_after_a_collection(
    tmp_path: Path, arm: Arm, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`stale-file-handle-inner.sh:43` to `:51`: a collection through the upper layer, then a build, with no hook and with one.

    Without the hook Nix expects `Stale file handle`, and skips the script on any other answer (`:43`). Measured
    on Linux 7.2.4: no stale handle, and the last build fails on every arm, the local one with `creating file
    ".../busybox": File exists`. The port asserts nothing of that round, and requires every step of the round
    with the hook to pass.
    """
    monkeypatch.setenv("LIBMOUNT_FORCE_MOUNT2", "always")
    src = _sources(tmp_path)
    (src / "local-overlay-store").mkdir()
    copy_nix_tests(src, "local-overlay-store/remount.sh")
    async with overlay_volume() as stores:
        await _init_lower(stores, src)
        async with mounted(stores):
            async with overlay_arm(stores, arm, _config(), tmp_path) as store:
                await _trigger_stale_file_handle(stores, store, src)
            await remount(stores)
            hook = src / "local-overlay-store/remount.sh"
            async with overlay_arm(stores, arm, _config(), tmp_path, remount_hook=hook) as store:
                failed = await _trigger_stale_file_handle(stores, store, src)
            assert failed is None, f"stale-file-handle-inner.sh:51: {failed}"
