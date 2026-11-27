"""pynixd's sandbox binds `/dev/kvm` when the store's features name `kvm`, as Nix does.

`linux-derivation-builder.cc:755` (Nix 2.35.6) binds it from `Store::Config::systemFeatures`, not the
derivation's own. Without it a QEMU or libkrun build finds no device (nixidae/nixidae#1648).
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, cast

from pynixd_store.builder import local
from pynixd_store.builder.settings import BuildSettings
from pynixd_store.builder.site import BuildPaths

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

    from pynixd_store.engine import StoreEngine


def _bound(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, features: set[str], *, device: bool) -> set[str]:
    kvm = tmp_path / "kvm"
    if device:
        kvm.touch()
    monkeypatch.setattr(local, "KVM_DEVICE", str(kvm))
    root = tmp_path / "root"
    (root / "etc").mkdir(parents=True)
    settings = dataclasses.replace(
        BuildSettings.from_nix_config({}, {"system": "x86_64-linux", "sandbox": "true"}),
        system_features=frozenset(features),
    )
    paths = BuildPaths(tmp_path, tmp_path / "build", tmp_path, root)
    # No input and no sandbox-paths closure: the engine is never asked for a path.
    binds = local._binds(cast("StoreEngine", None), paths, set(), set(), settings, sandboxed=True)
    return {target for target, _, _ in binds}


def test_a_store_with_kvm_binds_the_device(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert str(tmp_path / "kvm") in _bound(tmp_path, monkeypatch, {"kvm", "big-parallel"}, device=True)


def test_a_store_without_kvm_binds_no_device(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert str(tmp_path / "kvm") not in _bound(tmp_path, monkeypatch, {"big-parallel"}, device=True)


def test_a_host_without_the_device_binds_nothing_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert str(tmp_path / "kvm") not in _bound(tmp_path, monkeypatch, {"kvm"}, device=False)
