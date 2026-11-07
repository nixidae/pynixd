"""`ClientSettings::apply` (`daemon.cc:233`): what a session's SetOptions gives a sandbox build."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from pynixd.store.local_db import _build_overrides


def _options(overrides: dict[str, str], keep_failed: int = 0) -> Any:
    return cast(
        "Any",
        SimpleNamespace(
            keep_failed=keep_failed,
            keep_going=0,
            try_fallback=1,
            max_build_jobs=4,
            max_silent_time=5,
            build_cores=2,
            use_substitutes=1,
            overrides=overrides,
        ),
    )


def test_the_daemon_keeps_its_own_experimental_features() -> None:
    """`daemon.cc:273`: a trusted client's experimental features do not reach the daemon either."""
    got = _build_overrides(
        _options({"experimental-features": "ca-derivations", "plugin-files": "/x", "sandbox": "relaxed"})
    )
    assert got == {
        "keep-failed": "false",
        "keep-going": "false",
        "fallback": "true",
        "max-jobs": "4",
        "max-silent-time": "5",
        "cores": "2",
        "substitute": "true",
        "sandbox": "relaxed",
    }


def test_keep_failed_is_a_fixed_field_of_the_session() -> None:
    """`daemon.cc:223`: `settings.keepFailed = keepFailed`, whatever the daemon's own `keep-failed` says."""
    assert _build_overrides(_options({}, keep_failed=1))["keep-failed"] == "true"
