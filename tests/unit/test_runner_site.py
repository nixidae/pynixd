"""The runner site: a build pod's runner container is the builder (`pynixd_store.builder.runner`, #1560).

nixkube's build pod test runs the runner on a real node; this file holds what the controller decides alone.
"""

from __future__ import annotations

import signal
from pathlib import Path

import pytest

from nix_daemon_protocol.basic_derivation import BasicDerivation
from nix_daemon_protocol.derivation_output import DerivationOutput
from nix_daemon_protocol.store_path import StorePath
from pynixd_store.builder.outputs import BuildFailure
from pynixd_store.builder.runner import (
    RunnerInvocation,
    RunnerSandbox,
    RunnerState,
    exit_status,
    from_container,
    invocation,
    mismatch,
    runner_state,
)
from pynixd_store.builder.settings import BuildSettings
from pynixd_store.builder.site import BuilderFailed, BuildPaths, Launch, RunnerSite, choose_site
from pynixd_store.builder.users import BuildUser

SANDBOX = RunnerSandbox(Path("/build/runner"), Path("/build/runner-ready"), "runner")
DRV = StorePath("/nix/store/cccccccccccccccccccccccccccccccc-x.drv")
PLAIN = DerivationOutput(path="/nix/store/00000000000000000000000000000000-x")


def _choose(
    *,
    builder: str = "/nix/store/aaaa-bash/bin/bash",
    features: str = "",
    user: BuildUser | None = None,
    sandbox_paths: str = "",
) -> object:
    env = {"requiredSystemFeatures": features} if features else {}
    drv = BasicDerivation(outputs={"out": PLAIN}, platform="x86_64-linux", builder=builder, env=env)
    settings = BuildSettings.from_nix_config({}, {"system": "x86_64-linux", "sandbox-paths": sandbox_paths})
    site, elsewhere = choose_site(
        drv,
        settings,
        SANDBOX,
        chroot=False,
        sandboxed=True,
        user=user,
        real_store_dir=Path("/nix/store"),
        visible=frozenset(),
    )
    assert elsewhere is None
    return site


def test_a_runner_runs_every_build() -> None:
    assert _choose() == RunnerSite(SANDBOX)


@pytest.mark.parametrize(
    ("site", "reason"),
    [
        (_choose(builder="builtin:fetchurl"), "builtin builder 'builtin:fetchurl'"),
        (_choose(features="recursive-nix"), "no socket for a recursive build"),
        (_choose(features="uid-range"), "not as a build user"),
        (_choose(user=BuildUser(uid=30001, gid=30000, fd=-1)), "not as a build user"),
        (_choose(sandbox_paths="/bin/sh=/nix/store/aaaa-busybox/bin/sh"), "binds no sandbox-paths (/bin/sh)"),
    ],
)
def test_a_runner_refuses_what_it_does_not_give(site: object, reason: str) -> None:
    assert isinstance(site, RunnerSite)
    assert reason in str(site.refusal())


def test_central_escapes_each_dollar_and_the_controller_reads_it_back() -> None:
    """Measured (nixkube's `nriprobe`, run nrip5): Kubernetes expands `$(out)` and gives `$` for `$$`."""
    expected = invocation("/bin/$(x)", ["-c", "echo $out"], {"b": "$$", "a": "plain"})
    assert expected.command == ("/bin/$$(x)",)
    assert expected.args == ("-c", "echo $$out")
    assert expected.env == (("a", "plain"), ("b", "$$$$"))
    assert expected.working_dir == "/build"
    assert from_container(expected.container_fields()) == expected
    assert mismatch(expected, from_container(expected.container_fields())) is None


def test_a_runner_spec_that_differs_names_what() -> None:
    expected = invocation("/bin/sh", ["-c", "true"], {"out": "/nix/store/a", "PATH": "/p"})
    fields = expected.container_fields()
    fields["env"] = [{"name": "out", "value": "/nix/store/b"}, {"name": "PATH", "value": "/p"}, {"name": "x"}]
    assert mismatch(expected, from_container(fields)) == "its environment differs in out, x"
    fields = expected.container_fields() | {"args": ["-c", "false"]}
    assert mismatch(expected, from_container(fields)) == "its arguments are ['-c', 'false'], not ['-c', 'true']"
    other = RunnerInvocation(command=("/bin/bash",), args=expected.args, env=expected.env, working_dir="/build")
    assert mismatch(expected, other) == "its command is ['/bin/bash'], not ['/bin/sh']"


def _pod(state: dict[str, object]) -> dict[str, object]:
    return {"status": {"containerStatuses": [{"name": "controller", "state": {}}, {"name": "runner", "state": state}]}}


def test_the_runner_state_reads_the_pod() -> None:
    assert runner_state({"status": {}}, "runner") == RunnerState(started=False, terminated=None, refused=None)
    waiting = runner_state(_pod({"waiting": {"reason": "PodInitializing"}}), "runner")
    assert waiting == RunnerState(started=False, terminated=None, refused=None)
    assert runner_state(_pod({"running": {}}), "runner").started
    ended = runner_state(_pod({"terminated": {"exitCode": 7}}), "runner")
    assert ended.terminated == {"exitCode": 7}
    refused = runner_state(_pod({"waiting": {"reason": "CreateContainerError", "message": "no /nix"}}), "runner")
    assert refused.refused == "CreateContainerError: no /nix"


def test_a_signal_the_runtime_names_reads_as_popen_does() -> None:
    assert exit_status({"exitCode": 7}) == 7
    assert exit_status({"exitCode": 137, "signal": 9}) == -signal.SIGKILL


def test_the_runner_ends_as_its_exit_file_says(tmp_path: Path) -> None:
    paths = BuildPaths(top=tmp_path, tmp=tmp_path / "build", chroot_parent=None, root=None)
    site = RunnerSite(SANDBOX)
    failure = site.ended(paths, DRV, 1)
    assert isinstance(failure, BuildFailure)
    assert "did not report how it ended (its helper exited with 1)" in failure.message
    (tmp_path / "runner-exit").write_text("0")
    assert site.ended(paths, DRV, 0) is None
    (tmp_path / "runner-exit").write_text("-9")
    assert site.ended(paths, DRV, 0) == BuilderFailed("failed due to signal 9 (Killed)")
    assert site.setup_error(paths, Launch([])) == ""
    (tmp_path / "runner-status").write_text("the runner container does not start")
    assert site.setup_error(paths, Launch([])) == "the runner container does not start"
