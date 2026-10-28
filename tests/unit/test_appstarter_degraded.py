"""Whether this pod runs what its deployment asks for, or the image's copy.

The failure this exists for is silent. `appstarter init` seeds the store from
the copy baked into the image when the fetch fails -- deliberately, because a
pod that starts behind beats a pod that does not start -- and then the pod
runs, every probe passes, and nothing says the version is old.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pynixd import metrics

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _root(tmp_path: Path, wanted: str, running: str) -> Path:
    """A store root holding what `appstarter init` would have written."""
    state = tmp_path / "var/appstarter"
    state.mkdir(parents=True)
    (state / "state.json").write_text(f'{{"wanted": "{wanted}", "running": "{running}"}}')
    (tmp_path / "store").mkdir()
    return tmp_path / "store"


def test_the_wanted_environment_reads_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _root(tmp_path, "/nix/store/aaa-cache", "/nix/store/aaa-cache")
    monkeypatch.setattr(metrics, "real_store_dir", lambda: str(store))

    [family] = list(metrics.AppstarterCollector().collect())

    assert family.name == "pynixd_appstarter_degraded"
    assert family.samples[0].value == 0


def test_the_image_fallback_reads_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _root(tmp_path, "/nix/store/aaa-cache", "/nix/store/bbb-older")
    monkeypatch.setattr(metrics, "real_store_dir", lambda: str(store))

    [family] = list(metrics.AppstarterCollector().collect())

    assert family.samples[0].value == 1


def test_no_state_serves_no_series(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Absent, never zero. A process that cannot tell must not answer "not
    degraded" -- the one answer that hides what this reports."""
    monkeypatch.setattr(metrics, "real_store_dir", lambda: str(tmp_path / "store"))

    assert list(metrics.AppstarterCollector().collect()) == []


def test_a_truncated_state_serves_no_series(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`init` writes through a staging file and renames, so this should not
    happen -- and a crash mid-write must still not read as healthy."""
    store = _root(tmp_path, "x", "y")
    (tmp_path / "var/appstarter/state.json").write_text('{"wanted": "/nix/store/aaa"')
    monkeypatch.setattr(metrics, "real_store_dir", lambda: str(store))

    assert list(metrics.AppstarterCollector().collect()) == []
