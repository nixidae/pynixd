"""The engine adds the roots a client asks for as Nix does.

`LocalStore::addIndirectRoot` (`gc.cc:47`) and `IndirectRootStore::addPermRoot`
(`indirect-root-store.cc:20`) of Nix 2.35. The link name is Nix's own: `nix
hash convert --hash-algo sha1 --to nix32` of the SHA-1 of the path.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from pynixd_store.engine import StoreEngine
from pynixd_store.hintfmt import magenta
from pynixd_store.roots import RootError, canon_path
from pynixd_store.store_layout import StoreLayout

if TYPE_CHECKING:
    from pathlib import Path

BUILD_RESULT = "/tmp/nix-build-1-0/result"
BUILD_RESULT_LINK = "75vz4kmxwz09n898x3qy6vkrx9r9qa0x"
"""What Nix names the `gcroots/auto` link of `BUILD_RESULT`."""

STORE_PATH = "/nix/store/00000000000000000000000000000001-hello"


def _path_fmt(path: object) -> str:
    """`PathFmt` inside `HintFmt`: double quotes, then the colour."""
    return magenta(f'"{path}"')


async def _no_settings() -> dict[str, object]:
    raise AssertionError("a root reads no Nix setting")


def _engine(root: Path) -> StoreEngine:
    return StoreEngine(
        StoreLayout.chroot(root), writes=True, read_config=_no_settings, on_change=lambda _: None, name="test"
    )


@pytest.mark.parametrize(
    ("given", "canonical"),
    [("//a/./b/../c/", "/a/c"), ("/..", "/"), ("/", "/"), ("/a//b", "/a/b")],
)
def test_canon_path_is_lexical(given: str, canonical: str) -> None:
    assert canon_path(given) == canonical


def test_canon_path_refuses_a_relative_path() -> None:
    with pytest.raises(RootError) as refused:
        canon_path("a/b")
    assert str(refused.value) == f"not an absolute path: {_path_fmt('a/b')}"


async def test_an_indirect_root_takes_nix_s_link_name(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    await engine.add_indirect_root(BUILD_RESULT)
    link = engine.layout.state_dir / "gcroots" / "auto" / BUILD_RESULT_LINK
    assert os.readlink(link) == BUILD_RESULT


async def test_a_perm_root_links_holds_first_and_answers_the_canonical_root(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    held: list[tuple[str, bool]] = []
    root = tmp_path / "out" / "result"

    async def hold(path: str) -> None:
        held.append((path, os.path.lexists(root)))

    answer = await engine.add_perm_root(STORE_PATH, f"{tmp_path}//out/./result", hold=hold)

    assert answer == str(root)
    assert held == [(STORE_PATH, False)]
    assert os.readlink(root) == STORE_PATH
    indirect = list((engine.layout.state_dir / "gcroots" / "auto").iterdir())
    assert [os.readlink(link) for link in indirect] == [str(root)]


async def test_a_perm_root_replaces_a_link_into_the_store(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    root = tmp_path / "result"
    root.symlink_to("/nix/store/00000000000000000000000000000002-old")

    async def hold(path: str) -> None:
        pass

    await engine.add_perm_root(STORE_PATH, str(root), hold=hold)
    assert os.readlink(root) == STORE_PATH


async def test_a_perm_root_refuses_to_clobber_a_file(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    root = tmp_path / "result"
    root.write_text("mine")

    async def hold(path: str) -> None:
        pass

    with pytest.raises(RootError) as refused:
        await engine.add_perm_root(STORE_PATH, str(root), hold=hold)
    assert str(refused.value) == f"cannot create symlink {_path_fmt(root)}; already exists"
    assert root.read_text() == "mine"


async def test_a_perm_root_in_the_store_is_refused_before_the_temporary_root(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    held: list[str] = []

    async def hold(path: str) -> None:
        held.append(path)

    root = "/nix/store/result"
    with pytest.raises(RootError) as refused:
        await engine.add_perm_root(STORE_PATH, root, hold=hold)
    assert str(refused.value) == (
        f"creating a garbage collector root ({_path_fmt(root)}) in the Nix store is forbidden "
        "(are you running nix-build inside the store?)"
    )
    assert held == []
