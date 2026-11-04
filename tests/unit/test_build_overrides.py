"""`ClientSettings::apply` (`daemon.cc:233`): what a session's SetOptions gives a sandbox build."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from pynixd.store.local_db import _build_overrides


def _options(**overrides: str) -> Any:
    return cast("Any", SimpleNamespace(max_silent_time=5, build_cores=2, overrides=overrides))


def test_the_daemon_keeps_its_own_experimental_features() -> None:
    """`daemon.cc:273`: a trusted client's experimental features do not reach the daemon either."""
    got = _build_overrides(
        _options(**{"experimental-features": "ca-derivations", "plugin-files": "/x", "sandbox": "relaxed"})
    )
    assert got == {"max-silent-time": "5", "cores": "2", "sandbox": "relaxed"}
