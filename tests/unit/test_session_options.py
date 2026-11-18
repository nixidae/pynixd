"""What a session keeps of a client's `SetOptions` (`pynixd/session_options.py`). nixidae/nixidae#1198.

huggorm's Nix names `set-options-by-name`: an ssh-ng client then sends `SetOptions`, and every overridden setting
travels by name, header ones and nix.conf ones included (`nix-set-options-by-name.patch`).
"""

from __future__ import annotations

from pynixd.command_options import PynixdOption
from pynixd.serde.auth import Role
from pynixd.session_options import DaemonHeader, SessionOptions, session_options
from pynixd.trust import TrustPolicy
from tests._conftest.client_options import set_options

REMOTE = frozenset({"timeout", "max-silent-time", "keep-going"})
DAEMON = DaemonHeader(keep_going=False, max_build_jobs=8, max_silent_time=0, build_cores=4)


def _kept(
    overrides: dict[str, str], *, remote: bool, role: Role = Role.ADMIN, max_build_jobs: int = 1
) -> SessionOptions:
    request = set_options(overrides=overrides, max_build_jobs=max_build_jobs)
    return session_options(
        request, role=role, remote=remote, policy=TrustPolicy(), remote_settings=REMOTE, daemon=DAEMON
    )


def test_a_delegating_remote_client_does_not_stop_the_daemon() -> None:
    """Its nix.conf `max-jobs = 0` arrives by name and in the header; neither reaches the session."""
    kept = _kept({"max-jobs": "0", "cores": "1", "substituters": "https://example.org"}, remote=True, max_build_jobs=0)
    assert kept.options is None
    assert kept.dropped == ("cores", "max-jobs", "substituters")
    assert kept.warnings == ()


def test_a_remote_client_sets_a_listed_setting_over_the_daemon_s_header() -> None:
    kept = _kept({"max-jobs": "0", "max-silent-time": "30", "timeout": "60"}, remote=True, max_build_jobs=0)
    assert kept.options is not None
    assert kept.options.max_silent_time == 30
    assert kept.options.overrides == {"timeout": "60"}
    # The control: the header the client sent says max-jobs 0, and the daemon's own value stands.
    assert kept.options.max_build_jobs == DAEMON.max_build_jobs
    assert kept.options.build_cores == DAEMON.build_cores
    assert kept.dropped == ("max-jobs",)


def test_a_local_client_keeps_its_header_and_loses_the_names() -> None:
    """The header carries the same values; the names would only be refused for an untrusted user."""
    kept = _kept({"max-jobs": "0", "sandbox": "false"}, remote=False, role=Role.ADMIN, max_build_jobs=0)
    assert kept.options is not None
    assert kept.options.max_build_jobs == 0
    assert kept.options.overrides == {"sandbox": "false"}


def test_an_untrusted_local_client_gets_no_warning_for_the_header_names() -> None:
    kept = _kept({"max-jobs": "0", "sandbox": "false"}, remote=False, role=Role.USER)
    assert kept.warnings == (
        "ignoring the client-specified setting 'sandbox', because it is a restricted setting and you are not a"
        " trusted user",
    )


def test_a_pynixd_name_goes_to_the_session_and_never_upstream() -> None:
    for remote in (False, True):
        kept = _kept({"pynixd-pool": "big", "timeout": "5"}, remote=remote)
        assert kept.pynixd_options == {PynixdOption.POOL: "big"}
        assert kept.options is not None
        assert "pynixd-pool" not in kept.options.overrides


def test_an_untrusted_client_may_not_steer_by_setoptions() -> None:
    kept = _kept({"pynixd-pool": "big"}, remote=True, role=Role.USER)
    assert kept.pynixd_options == {}
    assert kept.warnings == (
        "ignoring the client-specified setting 'pynixd-pool', because it is a restricted setting and you are not a"
        " trusted user",
    )


def test_the_trust_filter_runs_before_the_list() -> None:
    """`timeout` is unrestricted in Nix; a listed restricted name stays refused for an untrusted user."""
    request = set_options(overrides={"timeout": "5", "sandbox": "false"})
    kept = session_options(
        request,
        role=Role.USER,
        remote=True,
        policy=TrustPolicy(),
        remote_settings=REMOTE | {"sandbox"},
        daemon=DAEMON,
    )
    assert kept.options is not None
    assert kept.options.overrides == {"timeout": "5"}


def test_a_bad_header_value_is_named() -> None:
    kept = _kept({"keep-going": "yes"}, remote=True)
    assert kept.options is None
    assert kept.warnings == ("ignoring the setting 'keep-going': 'yes' is not a Boolean",)


def test_the_daemon_header_reads_nix_config() -> None:
    config = {"max-jobs": {"value": 0}, "keep-going": {"value": True}, "cores": {"value": 2}}
    header = DaemonHeader.from_nix_config(config)
    assert (header.max_build_jobs, header.keep_going, header.build_cores, header.use_substitutes) == (0, True, 2, True)
