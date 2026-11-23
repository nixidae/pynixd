"""Central writes a build pod's runner container (nixidae/nixidae#1560, spike).

The controller's helper compares what central wrote with what `build()` computes (`mismatch`); a real pod is
nixkube's test. This file holds what central decides alone.
"""

from __future__ import annotations

import pytest

from nix_daemon_protocol.basic_derivation import BasicDerivation
from nix_daemon_protocol.build_derivation import BuildDerivationRequest
from nix_daemon_protocol.build_result import BuildMode
from nix_daemon_protocol.derivation_output import DerivationOutput
from nix_daemon_protocol.store_path import StorePath
from pynixd.config import KubernetesTemplateSettings
from pynixd.provisioning.kubernetes import (
    BuildPodEnd,
    BuildPodOwner,
    BuildPodWatch,
    RunnerFill,
    RunnerPod,
    build_pod_manifest,
    runner_pod,
)
from pynixd.store.build_pods import runner_refusal
from pynixd_store.builder.env import hash_placeholder
from pynixd_store.builder.outputs import fallback_path
from pynixd_store.builder.runner import (
    INVOCATION_BYTES,
    NETWORK_ANNOTATION,
    SANDBOX_ANNOTATION,
    drv_refusal,
    etc_files,
    planned_invocation,
)

DRV = StorePath("/nix/store/cccccccccccccccccccccccccccccccc-x.drv")
OUT = "/nix/store/00000000000000000000000000000000-x"
FIXED = DerivationOutput(path=OUT, method="flat:sha256", hash_digest="ab" * 32)
OWNER = BuildPodOwner(store="pods", run="r0")


def _drv(
    env: dict[str, str] | None = None,
    *,
    builder: str = "/nix/store/aaaa-bash/bin/bash",
    outputs: dict[str, DerivationOutput] | None = None,
) -> BasicDerivation:
    return BasicDerivation(
        outputs=outputs or {"out": DerivationOutput(path=OUT)},
        platform="x86_64-linux",
        builder=builder,
        args=["-c", "echo $out"],
        env={"out": OUT, **(env or {})},
    )


def _env(drv: BasicDerivation, *, impure_env: dict[str, str] | None = None) -> dict[str, str | None]:
    planned = planned_invocation(
        DRV, drv, store_dir="/nix/store", build_cores=3, impure_env=impure_env or {"TOKEN": "t$k"}
    )
    return dict(planned.env)


def test_central_writes_nix_s_environment_with_each_dollar_doubled() -> None:
    planned = planned_invocation(DRV, _drv(), store_dir="/nix/store", build_cores=3, impure_env={})

    assert planned.command == ("/nix/store/aaaa-bash/bin/bash",)
    assert planned.args == ("-c", "echo $$out")
    assert planned.working_dir == "/build"
    env = dict(planned.env)
    assert env["NIX_BUILD_CORES"] == "3"
    assert env["out"] == OUT
    assert env["NIX_BUILD_TOP"] == env["TMPDIR"] == "/build"


def test_a_floating_output_builds_at_its_fallback_path() -> None:
    floating = DerivationOutput(path="", method="nar:sha256")
    drv = _drv({"out": hash_placeholder("out")}, outputs={"out": floating})

    assert _env(drv)["out"] == str(fallback_path(DRV, "out", "x", "/nix/store"))


def test_only_a_fixed_output_build_takes_impure_env() -> None:
    impure = {"impureEnvVars": "TOKEN"}

    assert _env(_drv(impure, outputs={"out": FIXED}))["TOKEN"] == "t$$k"
    assert "TOKEN" not in _env(_drv(impure))


@pytest.mark.parametrize(
    ("drv", "says"),
    [
        (_drv(builder="builtin:fetchurl"), "builtin builder"),
        (_drv({"requiredSystemFeatures": "recursive-nix"}), "recursive build"),
        (_drv({"requiredSystemFeatures": "uid-range"}), "build user"),
        (_drv(outputs={"out": DerivationOutput(path="", method="nar:sha256", hash_digest="impure")}), "impure"),
        (_drv({"bad": "\udcff"}), "UTF-8"),
        (_drv({"a=b": ""}), "'a=b'"),
        (_drv({"big": "x" * INVOCATION_BYTES}), "exceed"),
    ],
)
def test_central_refuses_what_the_runner_cannot_run(drv: BasicDerivation, says: str) -> None:
    refused = drv_refusal(drv)

    assert refused is not None and says in refused


def test_a_runner_pod_builds_no_check_or_repair() -> None:
    for mode in (BuildMode.CHECK, BuildMode.REPAIR):
        request = BuildDerivationRequest(drv_path=DRV, derivation=_drv(), build_mode=mode)
        assert runner_refusal(request) == "the runner container builds no repair or check"
    assert runner_refusal(BuildDerivationRequest(drv_path=DRV, derivation=_drv(), build_mode=BuildMode.NORMAL)) is None


def _template(resources: dict[str, object], container: str = "runner") -> KubernetesTemplateSettings:
    return KubernetesTemplateSettings(
        pod={
            "metadata": {"annotations": {SANDBOX_ANNOTATION: container}},
            "spec": {
                "containers": [
                    {"name": "pynixd", "image": "scratch"},
                    {"name": "runner", "resources": resources, "env": [{"name": "KEPT"}], "envFrom": [{}]},
                ]
            },
        }
    )


def test_the_runner_s_cores_are_its_cpu_limit_else_its_request() -> None:
    assert runner_pod(_template({"limits": {"cpu": "1500m"}, "requests": {"cpu": 1}})) == RunnerPod("runner", 2)
    assert runner_pod(_template({"requests": {"cpu": "250m"}})) == RunnerPod("runner", 1)
    assert runner_pod(KubernetesTemplateSettings(pod={"spec": {"containers": []}})) is None
    with pytest.raises(ValueError, match="no CPU"):
        runner_pod(_template({}))
    with pytest.raises(ValueError, match="names no container 'other'"):
        runner_pod(_template({"limits": {"cpu": 1}}, container="other"))


def test_central_fills_the_runner_and_gives_a_fixed_output_build_the_network() -> None:
    planned = planned_invocation(DRV, _drv(), store_dir="/nix/store", build_cores=1, impure_env={})
    template = _template({"limits": {"cpu": 1}})

    pod = build_pod_manifest(template, "b", OWNER, runner=RunnerFill("runner", planned, network=True))
    runner = pod["spec"]["containers"][1]
    assert runner["command"] == list(planned.command)
    assert runner["env"] == planned.container_fields()["env"]
    assert "envFrom" not in runner
    assert pod["metadata"]["annotations"][NETWORK_ANNOTATION] == "true"
    closed = build_pod_manifest(template, "b", OWNER, runner=RunnerFill("runner", planned, network=False))
    assert NETWORK_ANNOTATION not in closed["metadata"]["annotations"]


def test_the_runner_s_etc_is_the_sandbox_s() -> None:
    closed = etc_files(uid=1000, gid=100, network=False, pod_hosts="10.0.0.5 pod\n")
    opened = etc_files(uid=1000, gid=100, network=True, pod_hosts="10.0.0.5 pod\n")

    assert closed.keys() == opened.keys()
    assert "nixbld:x:1000:100:Nix build user:/build:/noshell\n" in closed["passwd"]
    assert closed["group"] == "root:x:0:\nnixbld:!:100:\nnogroup:x:65534:\n"
    assert closed["hosts"] == "127.0.0.1 localhost\n::1 localhost\n"
    assert closed["nsswitch.conf"] == ""
    assert opened["hosts"] == "10.0.0.5 pod\n"
    assert opened["nsswitch.conf"] == "hosts: files dns\nservices: files\n"


def test_a_runner_that_failed_leaves_the_controller_s_result() -> None:
    """The pod fails when the runner exits non-zero; central reads the controller's own end."""
    watch = BuildPodWatch("pynixd")
    statuses = [
        {"name": "pynixd", "state": {"terminated": {"exitCode": 0, "message": "result"}}},
        {"name": "runner", "state": {"terminated": {"exitCode": 1}}},
    ]

    watch.saw({"status": {"phase": "Failed", "containerStatuses": statuses}})

    assert watch.end == BuildPodEnd(exit_code=0, message="result", cause="Failed")
    assert watch.end is not None and not watch.end.disrupted
