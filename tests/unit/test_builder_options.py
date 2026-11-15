"""A builder gets the client's options with `max-jobs = 1`, as Nix's hook sends them (`build-remote.cc:97`).

The client's `--max-jobs 0` means "build it elsewhere". Forwarded, it makes the builder refuse the build:
"local builds are disabled" (`derivation-building-goal.cc:302`).
"""

from __future__ import annotations

from typing import Any

from pynixd.execution import builder_options
from pynixd.serde import SetOptionsRequest
from tests._conftest.client_options import set_options

LOCAL: Any = object()
BUILDER: Any = object()


def _options(max_build_jobs: int) -> SetOptionsRequest:
    return set_options(max_build_jobs=max_build_jobs, overrides={"keep-outputs": "true"})


def test_a_builder_gets_one_job_whatever_the_client_set() -> None:
    for jobs in (0, 1, 16):
        sent = builder_options(_options(jobs), BUILDER, LOCAL)
        assert sent is not None
        assert sent.max_build_jobs == 1
        assert sent.overrides == {"keep-outputs": "true"}


def test_the_local_daemon_gets_the_clients_options_unchanged() -> None:
    sent = builder_options(_options(0), LOCAL, LOCAL)
    assert sent is not None
    assert sent.max_build_jobs == 0
    assert builder_options(None, BUILDER, LOCAL) is None
