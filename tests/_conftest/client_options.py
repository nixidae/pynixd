"""A client's `SetOptions`, for a test that needs one."""

from __future__ import annotations

from pynixd.serde import SetOptionsRequest, TimeSpan, Verbosity


def set_options(
    *,
    max_build_jobs: int = 1,
    build_cores: int = 1,
    max_silent_time: int = 0,
    use_substitutes: bool = True,
    overrides: dict[str, str] | None = None,
) -> SetOptionsRequest:
    """A `SetOptions` request with every field, because no field has a default; a test names what it varies."""
    return SetOptionsRequest(
        keep_failed=False,
        keep_going=False,
        try_fallback=False,
        verbosity=Verbosity.ERROR,
        max_build_jobs=max_build_jobs,
        max_silent_time=TimeSpan(max_silent_time),
        obsolete_use_build_hook=True,
        build_verbosity=Verbosity.ERROR,
        obsolete_log_type=0,
        obsolete_print_build_trace=0,
        build_cores=build_cores,
        use_substitutes=use_substitutes,
        overrides={} if overrides is None else overrides,
    )
