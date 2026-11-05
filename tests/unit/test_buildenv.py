"""`builtin:buildenv` (`builtins/buildenv.cc`) on a host directory: the profile's links, priorities and errors."""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import pytest

from pynixd_store.builder.buildenv import BuildenvError, build_profile, packages, path_fmt
from pynixd_store.hintfmt import WARNING_PREFIX, magenta

if TYPE_CHECKING:
    from pathlib import Path


def test_derivations_read_as_nix_reads_them() -> None:
    assert packages("true 5 1 /a\nfalse -3 2 /b\t/c ") == [("/a", True, 5), ("/b", False, -3), ("/c", False, -3)]
    assert packages("") == []


@pytest.mark.parametrize("derivations", ["true x 1 /a", "true 5 2 /a", "true 5", "true 2147483648 1 /a", "1 5 -1"])
def test_derivations_nix_cannot_read_are_none(derivations: str) -> None:
    assert packages(derivations) is None


def _pkg(root: Path, name: str, files: dict[str, str]) -> str:
    pkg = root / name
    for rel, text in files.items():
        path = pkg / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if text.startswith("->"):
            path.symlink_to(text[2:])
        else:
            path.write_text(text)
    pkg.mkdir(exist_ok=True)
    return str(pkg)


def _links(out: Path) -> dict[str, str]:
    return {
        str(p.relative_to(out)): os.readlink(p) if p.is_symlink() else "dir"
        for p in sorted(out.rglob("*"))
        if p.is_symlink() or p.is_dir()
    }


def test_a_profile_merges_directories_and_keeps_the_higher_priority(
    tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    c = _pkg(tmp_path, "c", {"bin/c": ""})
    a = _pkg(
        tmp_path,
        "a",
        {
            "bin/a": "",
            "bin/same": "a",
            ".hidden": "",
            "manifest.json": "",
            "gone": "->/nowhere",
            "nix-support/propagated-user-env-packages": f"{c}\n",
        },
    )
    b = _pkg(tmp_path, "b", {"bin/b": "", "bin/same": "b"})
    out = tmp_path / "out"
    out.mkdir()

    build_profile(str(out), [(b, True, 5), (a, True, 3), (str(tmp_path / "off"), False, 1)])

    assert _links(out) == {
        "bin": "dir",
        "bin/a": f"{a}/bin/a",
        "bin/b": f"{b}/bin/b",
        "bin/c": f"{c}/bin/c",
        "bin/same": f"{a}/bin/same",
    }
    line = capfd.readouterr().err
    assert json.loads(line.removeprefix("@nix ")) == {
        "action": "msg",
        "level": 1,
        "msg": f"{WARNING_PREFIX}skipping dangling symlink '{magenta(str(out / 'gone'))}'",
    }


def test_two_files_at_one_priority_are_a_conflict(tmp_path: Path) -> None:
    a = _pkg(tmp_path, "a", {"bin/x": ""})
    b = _pkg(tmp_path, "b", {"bin/x": ""})
    out = tmp_path / "out"
    out.mkdir()

    with pytest.raises(BuildenvError) as e:
        build_profile(str(out), [(b, True, 5), (a, True, 5)])

    # *a* sorts first, so its link is the one in place when *b* comes.
    assert str(e.value) == (
        "Unable to build profile. There is a conflict for the following files:\n\n"
        f"  {path_fmt(f'{a}/bin/x')}\n  {path_fmt(f'{b}/bin/x')}"
    )
